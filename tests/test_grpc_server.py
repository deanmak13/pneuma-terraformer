"""Tests for the gRPC ProvisioningService server (grpc_server.py).

Exercises the hand-written `ProvisioningService` against a mocked
`TerraformRunner` — the same "patch runner methods, not HTTP" pattern
test_routes.py uses for the HTTP surface. A fake gRPC `ServicerContext`
records `.abort(code, message)` calls (mirrors grpc.aio's real behaviour:
the servicer method exits at the abort() call) so error-path assertions
can inspect `context.abort.call_args` without booting a real
`grpc.aio.Server` for every case. The one exception is the live-server
smoke test, which DOES boot a real server on a free local port and
round-trips a request through the generated stub — proving the wiring
(interceptors, add_*_to_server, add_insecure_port) actually serves.

`RunTenantReconcile`/`RunTenantDestroy` route through `_tenant_inputs`,
which resolves the tenant via `CapabilityRegistry.find_tenant` (an httpx
call against Supabase REST) — patched at its source
(`services.terraformer.src.capability_sync.CapabilityRegistry`) per the
registry seam, never mocked at the HTTP layer. `GetTenantState` does not
touch the registry.
"""

from __future__ import annotations

import logging
import socket
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest
from pneuma_proto.provisioning.provisioning.v1 import (
    provisioning_api_pb2,
    provisioning_api_pb2_grpc,
)

from services.terraformer.src.grpc_server import ProvisioningService, start_grpc_server
from services.terraformer.src.settings import Settings, get_settings
from services.terraformer.src.terraform_runner import TerraformError, TerraformResult


class _Abort(Exception):
    """Raised by the fake context's `abort()`.

    Note: unittest.mock's exception `side_effect` instantiates the class
    with NO args when raised (`raise SomeExceptionClass` is equivalent to
    `raise SomeExceptionClass()`) — so assertions below inspect
    `context.abort.call_args`/`.await_args`, never this exception's
    payload.
    """


def _fake_context() -> MagicMock:
    ctx = MagicMock(name="ServicerContext")
    ctx.abort = AsyncMock(side_effect=_Abort)
    return ctx


def _fake_runner() -> MagicMock:
    return MagicMock(
        name="TerraformRunner",
        reconcile=AsyncMock(),
        destroy=AsyncMock(),
        state=AsyncMock(),
    )


def _patched_registry(tenant_row: dict[str, Any] | None):
    """Patch the registry at its DEFINITION module — `_tenant_inputs`
    does a function-local `from services.terraformer.src.capability_sync
    import CapabilityRegistry`, which re-resolves the name from that
    module's namespace on every call, so patching it there (rather than
    on grpc_server) is what actually intercepts the lookup."""
    mock_registry = MagicMock(name="CapabilityRegistry")
    mock_registry.find_tenant = AsyncMock(return_value=tenant_row)
    mock_registry.aclose = AsyncMock()
    return patch(
        "services.terraformer.src.capability_sync.CapabilityRegistry",
        return_value=mock_registry,
    )


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ---------------------------------------------------------------------------
# RunTenantReconcile
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_tenant_reconcile_builds_inputs_and_returns_resources() -> None:
    settings = get_settings()
    runner = _fake_runner()
    runner.reconcile.return_value = TerraformResult(
        exit_code=0,
        stdout="...\nApply complete! Resources: 3 added.\n",
        stderr="",
        outputs={"vhost": "/acme-tst", "count": 3},
    )
    servicer = ProvisioningService(settings, runner=runner)
    request = provisioning_api_pb2.RunTenantReconcileRequest(
        tenant_id="t-001", profile="standard",
    )
    context = _fake_context()

    with _patched_registry({"id": "t-001", "slug": "acme-corp", "compliance_profile": None}):
        response = await servicer.RunTenantReconcile(request, context)

    runner.reconcile.assert_awaited_once()
    inputs = runner.reconcile.await_args.args[0]
    assert inputs.tenant_id == "t-001"
    assert inputs.tenant_slug == "acme-corp"
    assert inputs.env == settings.env
    assert inputs.compliance_profile == "standard"
    assert inputs.pooled_namespace == settings.pneuma_namespace

    assert response.resources["vhost"] == "/acme-tst"
    assert response.resources["count"] == "3"
    assert response.exit_code == 0
    assert "Apply complete" in response.runner_summary
    assert response.duration_ms >= 0
    context.abort.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_tenant_reconcile_defaults_profile_to_standard() -> None:
    """An empty `profile` on the request AND an unset tenant
    compliance_profile both fall back to "standard" — matches the HTTP
    TenantRequest's default (routes/provisioning.py)."""
    settings = get_settings()
    runner = _fake_runner()
    runner.reconcile.return_value = TerraformResult(
        exit_code=0, stdout="", stderr="", outputs={},
    )
    servicer = ProvisioningService(settings, runner=runner)
    request = provisioning_api_pb2.RunTenantReconcileRequest(tenant_id="t-002")
    context = _fake_context()

    with _patched_registry({"id": "t-002", "slug": "acme", "compliance_profile": None}):
        await servicer.RunTenantReconcile(request, context)

    inputs = runner.reconcile.await_args.args[0]
    assert inputs.compliance_profile == "standard"


@pytest.mark.asyncio
async def test_run_tenant_reconcile_forwards_timeout_seconds_to_runner() -> None:
    settings = get_settings()
    runner = _fake_runner()
    runner.reconcile.return_value = TerraformResult(exit_code=0, stdout="", stderr="", outputs={})
    servicer = ProvisioningService(settings, runner=runner)
    request = provisioning_api_pb2.RunTenantReconcileRequest(tenant_id="t-008", timeout_seconds=42)
    context = _fake_context()

    with _patched_registry({"id": "t-008", "slug": "acme", "compliance_profile": "standard"}):
        await servicer.RunTenantReconcile(request, context)

    assert runner.reconcile.await_args.kwargs["timeout"] == 42


@pytest.mark.asyncio
async def test_run_tenant_reconcile_unset_timeout_seconds_forwards_none() -> None:
    """proto3 int32 default 0 means "unset" — the servicer must forward
    None (not 0) so the runner falls back to settings.apply_timeout_seconds
    instead of a 0-second timeout."""
    settings = get_settings()
    runner = _fake_runner()
    runner.reconcile.return_value = TerraformResult(exit_code=0, stdout="", stderr="", outputs={})
    servicer = ProvisioningService(settings, runner=runner)
    request = provisioning_api_pb2.RunTenantReconcileRequest(tenant_id="t-009")
    context = _fake_context()

    with _patched_registry({"id": "t-009", "slug": "acme", "compliance_profile": "standard"}):
        await servicer.RunTenantReconcile(request, context)

    assert runner.reconcile.await_args.kwargs["timeout"] is None


# ---------------------------------------------------------------------------
# RunTenantDestroy
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_tenant_destroy_returns_empty_destroyed_resources() -> None:
    settings = get_settings()
    runner = _fake_runner()
    runner.destroy.return_value = TerraformResult(
        exit_code=0,
        stdout="Destroy complete! Resources: 3 destroyed.\n",
        stderr="",
        outputs={},
    )
    servicer = ProvisioningService(settings, runner=runner)
    request = provisioning_api_pb2.RunTenantDestroyRequest(tenant_id="t-003")
    context = _fake_context()

    with _patched_registry({"id": "t-003", "slug": "acme", "compliance_profile": "standard"}):
        response = await servicer.RunTenantDestroy(request, context)

    runner.destroy.assert_awaited_once()
    inputs = runner.destroy.await_args.args[0]
    assert inputs.tenant_id == "t-003"
    assert inputs.pooled_namespace == settings.pneuma_namespace
    assert dict(response.destroyed_resources) == {}
    assert "Destroy complete" in response.runner_summary
    assert response.exit_code == 0
    context.abort.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_tenant_destroy_forwards_timeout_seconds_to_runner() -> None:
    settings = get_settings()
    runner = _fake_runner()
    runner.destroy.return_value = TerraformResult(exit_code=0, stdout="", stderr="", outputs={})
    servicer = ProvisioningService(settings, runner=runner)
    request = provisioning_api_pb2.RunTenantDestroyRequest(tenant_id="t-010", timeout_seconds=99)
    context = _fake_context()

    with _patched_registry({"id": "t-010", "slug": "acme", "compliance_profile": "standard"}):
        await servicer.RunTenantDestroy(request, context)

    assert runner.destroy.await_args.kwargs["timeout"] == 99


@pytest.mark.asyncio
async def test_run_tenant_destroy_logs_audit_line_with_authorized_by_and_reason(
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = get_settings()
    runner = _fake_runner()
    runner.destroy.return_value = TerraformResult(
        exit_code=0, stdout="Destroy complete!", stderr="", outputs={},
    )
    servicer = ProvisioningService(settings, runner=runner)
    request = provisioning_api_pb2.RunTenantDestroyRequest(
        tenant_id="t-011",
        authorized_by="dean@pneuma.io",
        reason="tenant offboarding — customer requested",
    )
    context = _fake_context()

    with _patched_registry({"id": "t-011", "slug": "acme", "compliance_profile": "standard"}):
        with caplog.at_level(logging.INFO, logger="terraformer.grpc"):
            await servicer.RunTenantDestroy(request, context)

    audit_lines = [r.getMessage() for r in caplog.records if "audit" in r.getMessage().lower()]
    assert any(
        "dean@pneuma.io" in line and "tenant offboarding" in line for line in audit_lines
    ), f"no audit line found in {caplog.records!r}"


@pytest.mark.asyncio
async def test_run_tenant_destroy_error_scrubs_secret_and_maps_to_internal() -> None:
    """Mirrors test_reconcile_error_scrubs_secret_from_abort_message for
    the destroy RPC — proves the TerraformError branch (INTERNAL +
    scrubbing) is wired the same way on both mutating RPCs."""
    settings = get_settings()
    secret = settings.openbao_admin_token
    runner = _fake_runner()
    runner.destroy.side_effect = TerraformError(
        "destroy",
        TerraformResult(exit_code=1, stdout="", stderr=f"boom {secret} leaked", outputs={}),
    )
    servicer = ProvisioningService(settings, runner=runner)
    request = provisioning_api_pb2.RunTenantDestroyRequest(tenant_id="t-013")
    context = _fake_context()

    with _patched_registry({"id": "t-013", "slug": "acme", "compliance_profile": "standard"}):
        with pytest.raises(_Abort):
            await servicer.RunTenantDestroy(request, context)

    code, message = context.abort.await_args.args
    assert code == grpc.StatusCode.INTERNAL
    assert secret not in message


@pytest.mark.asyncio
async def test_run_tenant_destroy_tenant_not_found_maps_to_invalid_argument() -> None:
    settings = get_settings()
    runner = _fake_runner()
    servicer = ProvisioningService(settings, runner=runner)
    request = provisioning_api_pb2.RunTenantDestroyRequest(tenant_id="t-ghost-2")
    context = _fake_context()

    with _patched_registry(None):
        with pytest.raises(_Abort):
            await servicer.RunTenantDestroy(request, context)

    code, message = context.abort.await_args.args
    assert code == grpc.StatusCode.INVALID_ARGUMENT
    assert "t-ghost-2" in message
    runner.destroy.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_tenant_destroy_logs_audit_line_even_when_unset(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A destroy call with no authorized_by/reason still gets an audit
    line — visibly marked <unset> rather than silently omitted."""
    settings = get_settings()
    runner = _fake_runner()
    runner.destroy.return_value = TerraformResult(exit_code=0, stdout="", stderr="", outputs={})
    servicer = ProvisioningService(settings, runner=runner)
    request = provisioning_api_pb2.RunTenantDestroyRequest(tenant_id="t-012")
    context = _fake_context()

    with _patched_registry({"id": "t-012", "slug": "acme", "compliance_profile": "standard"}):
        with caplog.at_level(logging.INFO, logger="terraformer.grpc"):
            await servicer.RunTenantDestroy(request, context)

    audit_lines = [r.getMessage() for r in caplog.records if "audit" in r.getMessage().lower()]
    assert any("<unset>" in line for line in audit_lines)


# ---------------------------------------------------------------------------
# GetTenantState
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_tenant_state_returns_state_path_and_resources() -> None:
    settings = get_settings()
    runner = _fake_runner()
    runner.state.return_value = {
        "exists": True, "outputs": {"vhost": "/acme-tst"}, "bootstrapped": True,
    }
    servicer = ProvisioningService(settings, runner=runner)
    request = provisioning_api_pb2.GetTenantStateRequest(tenant_id="t-004")
    context = _fake_context()

    response = await servicer.GetTenantState(request, context)

    runner.state.assert_awaited_once_with("t-004")
    assert response.resources["vhost"] == "/acme-tst"
    assert response.state_path == (
        f"s3://{settings.tf_state_backend_bucket}/tenants/t-004.tfstate"
    )
    assert response.last_applied_at == 0
    context.abort.assert_not_awaited()


# ---------------------------------------------------------------------------
# Security — boundary scrubbing + timeout + path-traversal
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reconcile_error_scrubs_secret_from_abort_message() -> None:
    """SECURITY: a TerraformError whose stderr embeds a live credential
    value must NOT leak that value into the gRPC abort() message —
    proves boundary scrubbing (mirrors the HTTP `_tail` scrub in
    routes/provisioning.py)."""
    settings = get_settings()
    secret = settings.hetzner_api_token
    runner = _fake_runner()
    runner.reconcile.side_effect = TerraformError(
        "apply",
        TerraformResult(
            exit_code=1, stdout="", stderr=f"oops {secret} rejected", outputs={},
        ),
    )
    servicer = ProvisioningService(settings, runner=runner)
    request = provisioning_api_pb2.RunTenantReconcileRequest(tenant_id="t-005")
    context = _fake_context()

    with _patched_registry({"id": "t-005", "slug": "acme", "compliance_profile": "standard"}):
        with pytest.raises(_Abort):
            await servicer.RunTenantReconcile(request, context)

    context.abort.assert_awaited_once()
    code, message = context.abort.await_args.args
    assert code == grpc.StatusCode.INTERNAL
    assert secret not in message


@pytest.mark.asyncio
async def test_reconcile_tenant_not_found_maps_to_invalid_argument() -> None:
    """A syntactically-valid tenant_id that the registry can't resolve
    raises ValueError from _tenant_inputs (not the top-of-method pattern
    guard) — must still map to INVALID_ARGUMENT."""
    settings = get_settings()
    runner = _fake_runner()
    servicer = ProvisioningService(settings, runner=runner)
    request = provisioning_api_pb2.RunTenantReconcileRequest(tenant_id="t-ghost")
    context = _fake_context()

    with _patched_registry(None):
        with pytest.raises(_Abort):
            await servicer.RunTenantReconcile(request, context)

    code, message = context.abort.await_args.args
    assert code == grpc.StatusCode.INVALID_ARGUMENT
    assert "t-ghost" in message
    runner.reconcile.assert_not_awaited()


@pytest.mark.asyncio
async def test_reconcile_timeout_maps_to_deadline_exceeded() -> None:
    settings = get_settings()
    runner = _fake_runner()
    runner.reconcile.side_effect = TerraformError(
        "apply",
        TerraformResult(exit_code=124, stdout="", stderr="timed out", outputs={}),
    )
    servicer = ProvisioningService(settings, runner=runner)
    request = provisioning_api_pb2.RunTenantReconcileRequest(tenant_id="t-006")
    context = _fake_context()

    with _patched_registry({"id": "t-006", "slug": "acme", "compliance_profile": "standard"}):
        with pytest.raises(_Abort):
            await servicer.RunTenantReconcile(request, context)

    code, _message = context.abort.await_args.args
    assert code == grpc.StatusCode.DEADLINE_EXCEEDED


@pytest.mark.asyncio
async def test_reconcile_rejects_path_traversal_tenant_id_without_calling_runner() -> None:
    settings = get_settings()
    runner = _fake_runner()
    servicer = ProvisioningService(settings, runner=runner)
    request = provisioning_api_pb2.RunTenantReconcileRequest(tenant_id="../../etc")
    context = _fake_context()

    with patch(
        "services.terraformer.src.capability_sync.CapabilityRegistry"
    ) as mock_registry_cls:
        with pytest.raises(_Abort):
            await servicer.RunTenantReconcile(request, context)
        mock_registry_cls.assert_not_called()

    code, _message = context.abort.await_args.args
    assert code == grpc.StatusCode.INVALID_ARGUMENT
    runner.reconcile.assert_not_awaited()


@pytest.mark.asyncio
async def test_destroy_rejects_path_traversal_tenant_id_without_calling_runner() -> None:
    settings = get_settings()
    runner = _fake_runner()
    servicer = ProvisioningService(settings, runner=runner)
    request = provisioning_api_pb2.RunTenantDestroyRequest(tenant_id="../escape")
    context = _fake_context()

    with pytest.raises(_Abort):
        await servicer.RunTenantDestroy(request, context)

    code, _message = context.abort.await_args.args
    assert code == grpc.StatusCode.INVALID_ARGUMENT
    runner.destroy.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_tenant_state_rejects_path_traversal_tenant_id() -> None:
    settings = get_settings()
    runner = _fake_runner()
    servicer = ProvisioningService(settings, runner=runner)
    request = provisioning_api_pb2.GetTenantStateRequest(tenant_id="../escape")
    context = _fake_context()

    with pytest.raises(_Abort):
        await servicer.GetTenantState(request, context)

    code, _message = context.abort.await_args.args
    assert code == grpc.StatusCode.INVALID_ARGUMENT
    runner.state.assert_not_awaited()


# ---------------------------------------------------------------------------
# Live-server smoke
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_live_server_serves_get_tenant_state_over_a_real_channel() -> None:
    """Boot a real `grpc.aio.Server` on a free local port (interceptors +
    `add_ProvisioningServiceServicer_to_server` + `add_insecure_port` all
    wired), round-trip one `GetTenantState` call through the generated
    stub, and shut the server down cleanly.
    """
    runner = _fake_runner()
    runner.state.return_value = {"exists": False, "outputs": {}}

    port = _free_port()
    settings = Settings(grpc_port=port)
    server = await start_grpc_server(settings, runner=runner)
    try:
        async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
            stub = provisioning_api_pb2_grpc.ProvisioningServiceStub(channel)
            response = await stub.GetTenantState(
                provisioning_api_pb2.GetTenantStateRequest(tenant_id="t-live-001")
            )
        assert dict(response.resources) == {}
        runner.state.assert_awaited_once_with("t-live-001")
    finally:
        await server.stop(None)
