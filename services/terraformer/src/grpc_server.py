"""gRPC surface for Terraformer provisioning capabilities."""

from __future__ import annotations

import logging
import re
import time
from typing import Any

import grpc
from pneuma_proto.provisioning.platform.v1 import platform_provisioning_api_pb2_grpc

from services.terraformer.src.routes.provisioning import _TENANT_ID_PATTERN
from services.terraformer.src.settings import Settings
from services.terraformer.src.terraform_runner import (
    PlatformBusTopologyInputs,
    PlatformSecretsInputs,
    TenantInputs,
    TerraformError,
    TerraformRunner,
    get_runner,
    scrub_credentials,
)

_LOG = logging.getLogger("terraformer.grpc")

_TENANT_ID_RE = re.compile(_TENANT_ID_PATTERN)


def _string_map(values: dict[str, Any]) -> dict[str, str]:
    return {str(k): "" if v is None else str(v) for k, v in values.items()}


def _platform_secrets_resource_counts(outputs: dict[str, Any]) -> dict[str, str]:
    """Map `TerraformRunner.reconcile_platform_secrets`'s raw `terraform
    output -json` dict to `ApplyPlatformSecretsResponse.resources`'
    documented shape: target OpenBao path -> reference-entry count.

    The platform-secrets module (pneuma-deployments infrastructure/
    terraform/modules/platform-secrets/outputs.tf) exposes 4 named
    outputs — `reconciled_target_paths` (list), `canonical_source_paths`
    (list), `entry_count` (a single cluster-wide int), and
    `fanout_summary` (a map of target_path -> list of secret KEY NAMES,
    never values). Passing the raw outputs dict through `_string_map`
    would stringify Terraform's 4 OUTPUT-BLOCK NAMES as keys with
    Python `repr()`-style list/dict dumps as values — not the
    per-path count map the proto promises. `fanout_summary` is the one
    output with the right per-path granularity; this derives the count
    from the length of each path's key list.
    """
    fanout_summary = outputs.get("fanout_summary")
    if not isinstance(fanout_summary, dict):
        return {}
    return {
        str(path): str(len(keys)) if isinstance(keys, list) else "0"
        for path, keys in fanout_summary.items()
    }


async def _tenant_inputs(tenant_id: str, profile: str, workspace: str, settings: Settings) -> TenantInputs:
    if workspace and workspace != tenant_id:
        raise ValueError("workspace override is not supported by the current tenant module")

    # Import here so unit tests can import the module without a live proto wheel.
    from services.terraformer.src.capability_sync import CapabilityRegistry

    registry = CapabilityRegistry(
        settings.supabase_url,
        settings.supabase_service_key.get_secret_value(),
    )
    try:
        tenant = await registry.find_tenant(tenant_id)
    finally:
        await registry.aclose()

    if not tenant:
        raise ValueError(f"tenant {tenant_id!r} not found")
    return TenantInputs(
        tenant_id=tenant_id,
        tenant_slug=tenant["slug"],
        env=settings.env,
        compliance_profile=profile or tenant.get("compliance_profile") or "standard",
        pooled_namespace=settings.pneuma_namespace,
    )


def _terraform_error_code(exc: TerraformError) -> grpc.StatusCode:
    # exit_code 124 is _spawn's own timeout sentinel (see terraform_runner.py
    # _spawn's asyncio.TimeoutError branch) — map it to DEADLINE_EXCEEDED so
    # callers can distinguish "ran out of time" from "terraform errored".
    if exc.result.exit_code == 124:
        return grpc.StatusCode.DEADLINE_EXCEEDED
    return grpc.StatusCode.INTERNAL


class ProvisioningService:
    def __init__(self, settings: Settings, runner: TerraformRunner | None = None):
        self._settings = settings
        self._runner = runner or get_runner()

    async def RunTenantReconcile(self, request, context):
        pb2 = _pb2()
        if not _TENANT_ID_RE.fullmatch(request.tenant_id):
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"invalid tenant_id: {request.tenant_id!r}",
            )
        started = time.monotonic()
        try:
            inputs = await _tenant_inputs(
                request.tenant_id,
                request.profile,
                request.workspace,
                self._settings,
            )
            result = await self._runner.reconcile(
                inputs, timeout=request.timeout_seconds or None
            )
            return pb2.RunTenantReconcileResponse(
                resources=_string_map(result.outputs),
                duration_ms=int((time.monotonic() - started) * 1000),
                runner_summary=_summary(result.stdout),
                exit_code=result.exit_code,
            )
        except TerraformError as exc:
            await context.abort(
                _terraform_error_code(exc), scrub_credentials(str(exc), self._settings)
            )
        except ValueError as exc:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))

    async def RunTenantDestroy(self, request, context):
        pb2 = _pb2()
        if not _TENANT_ID_RE.fullmatch(request.tenant_id):
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"invalid tenant_id: {request.tenant_id!r}",
            )
        started = time.monotonic()
        # Audit line — RunTenantDestroy is irreversible and operator-gated
        # upstream (offboarding confirmation flow); record who authorised it
        # and why regardless of outcome. request.authorized_by/.reason are
        # proto3 strings — "<unset>" makes a missing field visually distinct
        # from an empty-but-provided one in the log.
        _LOG.info(
            "RunTenantDestroy audit: tenant_id=%s authorized_by=%s reason=%s",
            request.tenant_id,
            request.authorized_by or "<unset>",
            request.reason or "<unset>",
        )
        try:
            inputs = await _tenant_inputs(
                request.tenant_id,
                "",
                "",
                self._settings,
            )
            result = await self._runner.destroy(
                inputs, timeout=request.timeout_seconds or None
            )
            return pb2.RunTenantDestroyResponse(
                destroyed_resources=_string_map(result.outputs),
                duration_ms=int((time.monotonic() - started) * 1000),
                runner_summary=_summary(result.stdout),
                exit_code=result.exit_code,
            )
        except TerraformError as exc:
            await context.abort(
                _terraform_error_code(exc), scrub_credentials(str(exc), self._settings)
            )
        except ValueError as exc:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))

    async def GetTenantState(self, request, context):
        pb2 = _pb2()
        if not _TENANT_ID_RE.fullmatch(request.tenant_id):
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"invalid tenant_id: {request.tenant_id!r}",
            )
        try:
            state = await self._runner.state(request.tenant_id)
            return pb2.GetTenantStateResponse(
                resources=_string_map(state.get("outputs") or {}),
                # Sanctioned document-zero: reading the true last-apply
                # timestamp requires the .tfstate object's S3 metadata,
                # which the raw-httpx capability_sync stopgap doesn't carry
                # today. P4's typed-ORM refactor threads that through; until
                # then this is honestly zero rather than a fabricated value.
                last_applied_at=0,
                state_path=(
                    f"s3://{self._settings.tf_state_backend_bucket}"
                    f"/tenants/{request.tenant_id}.tfstate"
                ),
            )
        except ValueError as exc:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))


class PlatformProvisioningService(platform_provisioning_api_pb2_grpc.PlatformProvisioningServiceServicer):
    """gRPC servicer for `PlatformProvisioningService` (proto
    `provisioning/platform/v1/platform_provisioning_api.proto`) — the
    platform-tier sibling of `ProvisioningService` above. Both RPCs on
    this service are env-scoped, NEVER tenant-scoped (see the proto's own
    header comment: "Never mesh them" — D2 in plan §3.1). Registered as a
    SEPARATE gRPC service on the same server (see start_grpc_server) so a
    caller can never reach a platform-wide, ALL-tenants blast-radius
    operation through the tenant-scoped ProvisioningService surface.

    Unlike `ProvisioningService` above (a bare duck-typed class), this
    class INHERITS the generated `PlatformProvisioningServiceServicer`
    base. That's a deliberate, forced choice, not a style preference:
    grpc's generated `add_PlatformProvisioningServiceServicer_to_server`
    accesses `servicer.ApplyPlatformSecrets` directly when building its
    method-handler map (see platform_provisioning_api_pb2_grpc.py) — a
    bare class implementing only ApplyPlatformBusTopology would raise
    AttributeError at server-start, taking down the whole gRPC server
    (including the already-working tenant-tier RPCs) for a method this
    PR was never scoped to build.

    ApplyPlatformBusTopology (P5.2) AND ApplyPlatformSecrets (P7.3
    follow-up, unblocking core:platform_apply_secret_reconcile's
    draft->active flip per docs/plans/2026-07-11-terraformer-onboarding-
    provisioning.md human gate #6) are both implemented here — the fully
    tested runner-level method `TerraformRunner.reconcile_platform_secrets`
    already existed (previously dispatched only over HTTP via
    POST /provisioning/reconcile-platform-secrets, see
    routes/provisioning.py) and this handler is a thin wrapper around it,
    mirroring ApplyPlatformBusTopology's exact shape."""

    def __init__(self, settings: Settings, runner: TerraformRunner | None = None):
        self._settings = settings
        self._runner = runner or get_runner()

    async def ApplyPlatformSecrets(self, request, context):
        pb2 = _platform_pb2()
        started = time.monotonic()
        try:
            result = await self._runner.reconcile_platform_secrets(
                PlatformSecretsInputs(env=request.env)
            )
            return pb2.ApplyPlatformSecretsResponse(
                resources=_platform_secrets_resource_counts(result.outputs),
                duration_ms=int((time.monotonic() - started) * 1000),
                runner_summary=_summary(result.stdout),
                exit_code=result.exit_code,
            )
        except TerraformError as exc:
            await context.abort(
                _terraform_error_code(exc), scrub_credentials(str(exc), self._settings)
            )
        except ValueError as exc:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))

    async def ApplyPlatformBusTopology(self, request, context):
        pb2 = _platform_pb2()
        started = time.monotonic()
        try:
            result = await self._runner.reconcile_platform_bus_topology(
                PlatformBusTopologyInputs(env=request.env)
            )
            return pb2.ApplyPlatformBusTopologyResponse(
                resources=_string_map(result.outputs),
                duration_ms=int((time.monotonic() - started) * 1000),
                runner_summary=_summary(result.stdout),
                exit_code=result.exit_code,
            )
        except TerraformError as exc:
            await context.abort(
                _terraform_error_code(exc), scrub_credentials(str(exc), self._settings)
            )
        except ValueError as exc:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))


def _summary(stdout: str) -> str:
    for line in reversed((stdout or "").splitlines()):
        if line.strip():
            return line.strip()[:500]
    return ""


def _pb2():
    from pneuma_proto.provisioning.provisioning.v1 import provisioning_api_pb2

    return provisioning_api_pb2


def _platform_pb2():
    from pneuma_proto.provisioning.platform.v1 import platform_provisioning_api_pb2

    return platform_provisioning_api_pb2


async def start_grpc_server(settings: Settings, runner: TerraformRunner | None = None) -> grpc.aio.Server:
    from pneuma_proto.provisioning.provisioning.v1 import provisioning_api_pb2_grpc

    server = grpc.aio.server()
    provisioning_api_pb2_grpc.add_ProvisioningServiceServicer_to_server(
        ProvisioningService(settings, runner=runner),
        server,
    )
    platform_provisioning_api_pb2_grpc.add_PlatformProvisioningServiceServicer_to_server(
        PlatformProvisioningService(settings, runner=runner),
        server,
    )
    listen_addr = f"[::]:{settings.grpc_port}"
    server.add_insecure_port(listen_addr)
    await server.start()
    _LOG.info("terraformer gRPC server listening on %s", listen_addr)
    return server
