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

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.terraformer.src.main import _sync_capabilities


@pytest.mark.asyncio
async def test_sync_invokes_proto_sync_with_terraformer_host_and_self_url() -> None:
    """Happy path: import succeeds, sync_capabilities_from_proto is
    called with host_service='terraformer' and webhook_url=settings.self_url.

    Skipped in this standalone repo until `pneuma-common` (which ships
    `services.common.db.client` + `services.common.proto_capability_sync`)
    is a hard dep — `unittest.mock.patch` requires the target import to
    resolve, and those modules are absent here by design. The companion
    `test_sync_swallows_import_error_does_not_block_startup` test
    covers the import-failure branch without needing the real module.
    """
    pytest.importorskip(
        "services.common",
        reason="services.common ships with pneuma-common (Onboard-08 PR1) — not yet a dep here",
    )
    from services.terraformer.src.settings import get_settings

    settings = get_settings()
    fake_descriptor = MagicMock(name="provisioning_api_DESCRIPTOR")

    fake_proto_module = MagicMock()
    fake_proto_module.DESCRIPTOR = fake_descriptor

    fake_db = MagicMock(name="Database")
    fake_sync = AsyncMock(
        return_value={
            "capabilities_upserted": 3,
            "soft_deleted": 0,
            "errors": [],
        }
    )

    with patch.dict(
        "sys.modules",
        {
            "pneuma_proto.v1.provisioning.provisioning_api_pb2": fake_proto_module,
            "pneuma_proto.v1.provisioning": MagicMock(
                provisioning_api_pb2=fake_proto_module
            ),
        },
    ), patch(
        "services.common.db.client.Database.from_config", return_value=fake_db
    ), patch(
        "services.common.proto_capability_sync.sync_capabilities_from_proto",
        fake_sync,
    ):
        await _sync_capabilities(settings)

    fake_sync.assert_awaited_once()
    call_kwargs = fake_sync.await_args.kwargs
    assert call_kwargs["host_service"] == "terraformer"
    assert call_kwargs["webhook_url"] == settings.computed_self_url
    assert call_kwargs["file_descriptors"] == [fake_descriptor]


@pytest.mark.asyncio
async def test_sync_swallows_import_error_does_not_block_startup() -> None:
    """If the proto wheel hasn't been bumped (proto#36 publish), the
    import will fail. Service should still start — better to run
    degraded (no cap registration) than refuse to boot. Operator sees
    the WARN in logs and rebuilds the image after the proto bump.
    """
    from services.terraformer.src.settings import get_settings

    settings = get_settings()

    with patch(
        "builtins.__import__",
        side_effect=ImportError(
            "No module named 'pneuma_proto.v1.provisioning.provisioning_api_pb2'"
        ),
    ):
        # Should NOT raise — sync helper logs and returns
        await _sync_capabilities(settings)


@pytest.mark.asyncio
async def test_sync_swallows_runtime_error_does_not_block_startup() -> None:
    """If the DB sync errors at runtime (DB down, schema not migrated,
    etc.), the helper logs.exception and returns. Service starts;
    operator sees the error and investigates without losing the pod.

    Skipped in this standalone repo until pneuma-common is a hard dep
    — same reason as the happy-path test above.
    """
    pytest.importorskip(
        "services.common",
        reason="services.common ships with pneuma-common (Onboard-08 PR1) — not yet a dep here",
    )
    from services.terraformer.src.settings import get_settings

    settings = get_settings()
    fake_proto_module = MagicMock()
    fake_proto_module.DESCRIPTOR = MagicMock()

    with patch.dict(
        "sys.modules",
        {
            "pneuma_proto.v1.provisioning.provisioning_api_pb2": fake_proto_module,
            "pneuma_proto.v1.provisioning": MagicMock(
                provisioning_api_pb2=fake_proto_module
            ),
        },
    ), patch(
        "services.common.db.client.Database.from_config",
        return_value=MagicMock(),
    ), patch(
        "services.common.proto_capability_sync.sync_capabilities_from_proto",
        AsyncMock(side_effect=RuntimeError("simulated DB outage")),
    ):
        # Should NOT raise
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
