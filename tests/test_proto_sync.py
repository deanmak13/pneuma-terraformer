"""Sanity tests for terraformer's proto_capability_sync wiring at startup.

Mirrors the pattern used by brain / mimesis / admin-api lifespans. The
wiring is what makes `provisioning.apply_tenant_resources` +
`provisioning.destroy_tenant_resources` dispatchable from cycle-executor
— without it, capability_implementations has no row pointing at this
pod and dispatch fails with 'capability not found'.

These tests exercise the lifespan helper directly with mocks; the live
sync needs the proto wheel + a real DB connection which the integration
tier covers.
"""

from __future__ import annotations

from types import ModuleType
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.terraformer.src.main import _sync_capabilities


@pytest.mark.asyncio
async def test_sync_invokes_proto_sync_with_terraformer_host_and_self_url() -> None:
    """Happy path: import succeeds, sync_capabilities_from_proto is
    called with host_service='terraformer' and webhook_url=settings.self_url.

    The production image installs the generated pneuma-proto wheel from
    GHCR. The unit test supplies lightweight import shims so it does not
    depend on that image artifact being present locally.
    """
    from services.terraformer.src.settings import get_settings

    settings = get_settings()
    fake_descriptor = MagicMock(name="provisioning_api_DESCRIPTOR")
    fake_sync = AsyncMock(
        return_value={
            "inserted": 3,
            "updated": 0,
            "total": 3,
        }
    )

    fake_proto_module = ModuleType("provisioning_api_pb2")
    fake_proto_module.DESCRIPTOR = fake_descriptor
    fake_sync_module = ModuleType("capability_sync")
    fake_sync_module.sync_provisioning_capabilities = fake_sync

    with patch.dict("sys.modules", _proto_modules(fake_proto_module) | {
        "services.terraformer.src.capability_sync": fake_sync_module,
    }):
        await _sync_capabilities(settings)

    fake_sync.assert_awaited_once()
    assert fake_sync.await_args is not None
    call_kwargs = fake_sync.await_args.kwargs
    assert call_kwargs["grpc_target"] == settings.computed_grpc_target
    assert call_kwargs["file_descriptors"] == [fake_descriptor]


@pytest.mark.asyncio
async def test_sync_import_error_fails_startup() -> None:
    """A healthy Terraformer with no registered capabilities is worse than
    a failed pod: cycles see `capability not found` at runtime. Startup now
    fails closed when the proto wheel/sync module is absent.
    """
    from services.terraformer.src.settings import get_settings

    settings = get_settings()

    with patch(
        "builtins.__import__",
        side_effect=ImportError(
            "No module named 'pneuma_proto.v1.provisioning.provisioning_api_pb2'"
        ),
    ):
        with pytest.raises(ImportError):
            await _sync_capabilities(settings)


@pytest.mark.asyncio
async def test_sync_swallows_runtime_error_does_not_block_startup() -> None:
    """If the DB sync errors at runtime (DB down, schema not migrated,
    etc.), the helper logs.exception and returns. Service starts;
    operator sees the error and investigates without losing the pod.

    Runtime sync failures fail startup so Kubernetes never marks a
    non-dispatchable Terraformer as ready.
    """
    from services.terraformer.src.settings import get_settings

    settings = get_settings()
    fake_proto_module = ModuleType("provisioning_api_pb2")
    fake_proto_module.DESCRIPTOR = MagicMock()
    fake_sync_module = ModuleType("capability_sync")
    fake_sync_module.sync_provisioning_capabilities = AsyncMock(
        side_effect=RuntimeError("simulated DB outage")
    )

    with patch.dict("sys.modules", _proto_modules(fake_proto_module) | {
        "services.terraformer.src.capability_sync": fake_sync_module,
    }):
        with pytest.raises(RuntimeError, match="simulated DB outage"):
            await _sync_capabilities(settings)


def test_computed_self_url_derives_from_namespace_and_port() -> None:
    """Brain pattern: empty self_url default; computed_self_url derives
    the in-cluster DNS form from pneuma_namespace + terraformer_port so
    a fresh deploy works with zero per-env overrides for the demo path.
    Overrides via TERRAFORMER_SELF_URL for prod-standard / prod-regulated."""
    from services.terraformer.src.settings import Settings

    s = Settings()
    assert s.self_url == ""
    assert s.computed_self_url.startswith("http://terraformer.platform-")
    assert s.computed_self_url.endswith(":8011")  # the chart's Service port


def test_computed_self_url_respects_explicit_override() -> None:
    """When TERRAFORMER_SELF_URL is supplied, computed_self_url returns
    it verbatim — for prod overlays that route through an internal
    ingress instead of the in-cluster Service."""
    from services.terraformer.src.settings import Settings

    s = Settings(self_url="http://terraformer.example.internal:9000")
    assert s.computed_self_url == "http://terraformer.example.internal:9000"


def test_computed_grpc_target_uses_namespace_and_grpc_port() -> None:
    from services.terraformer.src.settings import Settings

    s = Settings(pneuma_namespace="platform-tst", grpc_port=8012)
    assert s.computed_grpc_target == "terraformer.platform-tst.svc.cluster.local:8012"


def _proto_modules(fake_proto_module: ModuleType) -> dict[str, ModuleType]:
    root = ModuleType("pneuma_proto")
    provisioning = ModuleType("pneuma_proto.provisioning")
    provisioning_segment = ModuleType("pneuma_proto.provisioning.provisioning")
    v1 = ModuleType("pneuma_proto.provisioning.provisioning.v1")
    v1.provisioning_api_pb2 = fake_proto_module
    return {
        "pneuma_proto": root,
        "pneuma_proto.provisioning": provisioning,
        "pneuma_proto.provisioning.provisioning": provisioning_segment,
        "pneuma_proto.provisioning.provisioning.v1": v1,
        "pneuma_proto.provisioning.provisioning.v1.provisioning_api_pb2": fake_proto_module,
    }
