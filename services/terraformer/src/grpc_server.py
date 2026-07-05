"""gRPC surface for Terraformer provisioning capabilities."""

from __future__ import annotations

import logging
import time
from typing import Any

import grpc

from services.terraformer.src.settings import Settings
from services.terraformer.src.terraform_runner import (
    TenantInputs,
    TerraformError,
    get_runner,
)

_LOG = logging.getLogger("terraformer.grpc")


def _string_map(values: dict[str, Any]) -> dict[str, str]:
    return {str(k): "" if v is None else str(v) for k, v in values.items()}


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


class ProvisioningService:
    def __init__(self, settings: Settings):
        self._settings = settings

    async def RunTenantReconcile(self, request, context):
        pb2 = _pb2()
        started = time.monotonic()
        try:
            inputs = await _tenant_inputs(
                request.tenant_id,
                request.profile,
                request.workspace,
                self._settings,
            )
            result = await get_runner().reconcile(inputs)
            return pb2.RunTenantReconcileResponse(
                resources=_string_map(result.outputs),
                duration_ms=int((time.monotonic() - started) * 1000),
                runner_summary=_summary(result.stdout),
                exit_code=result.exit_code,
            )
        except (TerraformError, ValueError) as exc:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(exc))

    async def RunTenantDestroy(self, request, context):
        pb2 = _pb2()
        started = time.monotonic()
        try:
            inputs = await _tenant_inputs(
                request.tenant_id,
                "",
                "",
                self._settings,
            )
            result = await get_runner().destroy(inputs)
            return pb2.RunTenantDestroyResponse(
                destroyed_resources=_string_map(result.outputs),
                duration_ms=int((time.monotonic() - started) * 1000),
                runner_summary=_summary(result.stdout),
                exit_code=result.exit_code,
            )
        except (TerraformError, ValueError) as exc:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(exc))

    async def GetTenantState(self, request, context):
        pb2 = _pb2()
        try:
            state = await get_runner().state(request.tenant_id)
            return pb2.GetTenantStateResponse(
                resources=_string_map(state.get("outputs") or {}),
                last_applied_at=0,
                state_path=f"tenants/{request.tenant_id}.tfstate",
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


async def start_grpc_server(settings: Settings) -> grpc.aio.Server:
    from pneuma_proto.provisioning.provisioning.v1 import provisioning_api_pb2_grpc

    server = grpc.aio.server()
    provisioning_api_pb2_grpc.add_ProvisioningServiceServicer_to_server(
        ProvisioningService(settings),
        server,
    )
    listen_addr = f"[::]:{settings.grpc_port}"
    server.add_insecure_port(listen_addr)
    await server.start()
    _LOG.info("terraformer gRPC server listening on %s", listen_addr)
    return server
