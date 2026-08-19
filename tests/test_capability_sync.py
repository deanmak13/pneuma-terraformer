"""Tests for capability_sync.py — Terraformer's proto-driven registration
of provisioning.* rows into public.capabilities, and the small typed
PostgREST client (CapabilityRegistry) it and grpc_server.py's
`_tenant_inputs` share.

Uses the REAL installed `pneuma_proto` DESCRIPTOR (not a hand-rolled
FileDescriptor) so `_capability_rows` is exercised against the actual
proto option annotations — the same wheel the Dockerfile bakes. HTTP
calls are mocked via respx (never live); the seam under test is the
typed client, not a raw httpx.AsyncClient at the call site (ORM-only LAW
— this is the explicitly-noted stopgap client, not app-code raw HTTP).
"""

from __future__ import annotations

import json

import httpx
import pytest
import respx
from pneuma_proto.provisioning.platform.v1 import platform_provisioning_api_pb2
from pneuma_proto.provisioning.provisioning.v1 import provisioning_api_pb2

from services.terraformer.src.capability_sync import (
    CapabilityRegistry,
    _capability_rows,
    _risk_category,
    sync_provisioning_capabilities,
)

_BASE_URL = "http://supabase-rest.test:3000"
_GRPC_TARGET = "terraformer.platform-tst.svc.cluster.local:8012"
_EXPECTED_NAMES = {
    "provisioning.apply_tenant_resources",
    "provisioning.destroy_tenant_resources",
    "provisioning.read_tenant_state",
}
_EXPECTED_PLATFORM_NAMES = {
    "provisioning.apply_platform_secrets",
    "provisioning.apply_platform_bus_topology",
}


# ---------------------------------------------------------------------------
# _risk_category
# ---------------------------------------------------------------------------


def test_risk_category_maps_read_only_to_observe() -> None:
    assert _risk_category("read_only") == "observe"


def test_risk_category_maps_acting_effects_to_act() -> None:
    assert _risk_category("perform_external_action") == "act"
    assert _risk_category("send_external_comm") == "act"


def test_risk_category_maps_mutating_effects_to_internal() -> None:
    assert _risk_category("mutate_pneuma_state") == "internal"
    assert _risk_category("mutate_tenant_state") == "internal"


# ---------------------------------------------------------------------------
# _capability_rows — driven off the real proto DESCRIPTOR
# ---------------------------------------------------------------------------


def _rows() -> list[dict]:
    return _capability_rows([provisioning_api_pb2.DESCRIPTOR], _GRPC_TARGET)


def test_capability_rows_produces_exactly_the_three_provisioning_rpcs() -> None:
    rows = _rows()
    assert len(rows) == 3
    assert {row["name"] for row in rows} == _EXPECTED_NAMES


def test_capability_rows_carry_grpc_internal_dispatch_shape() -> None:
    rows = _rows()
    for row in rows:
        assert row["connector_mode"] == "grpc_internal"
        assert row["protocol"] == "grpc"
        assert row["webhook_url"] == _GRPC_TARGET
        assert row["capability_class"] == "platform_internal"
        assert row["is_advertised"] is False
        assert row["is_active"] is True


def test_capability_rows_risk_category_matches_declared_effect() -> None:
    by_name = {row["name"]: row for row in _rows()}
    # [2026-08-15 chicken-and-egg fix] per provisioning_api.proto: apply/
    # destroy = mutate_pneuma_state (operator_only, internal) — was
    # perform_external_action/"act" until this fix, which made a brand-new
    # tier-0 tenant's own provisioning trip cycle-executor's live-authority
    # trust-tier gate (LiveAuthorityReclassifiedStricter): a tenant can
    # never earn the trust tier "act" requires without first being
    # provisioned. These RPCs create/destroy PLATFORM-owned infrastructure
    # (OpenBao paths, ESO bindings, RMQ vhost, MinIO bucket, Postgres
    # schema) executed BY the platform FOR the tenant — internal machinery,
    # not a tenant-attributed action against a third-party system, per
    # capability-taxonomy.md §3.1. read_tenant_state stays read_only
    # (observe) — a plain state read, no side effect.
    assert by_name["provisioning.apply_tenant_resources"]["effect"] == "mutate_pneuma_state"
    assert by_name["provisioning.apply_tenant_resources"]["risk_category"] == "internal"
    assert by_name["provisioning.destroy_tenant_resources"]["effect"] == "mutate_pneuma_state"
    assert by_name["provisioning.destroy_tenant_resources"]["risk_category"] == "internal"
    assert by_name["provisioning.read_tenant_state"]["effect"] == "read_only"
    assert by_name["provisioning.read_tenant_state"]["risk_category"] == "observe"


def test_capability_rows_authority_is_operator_only_for_every_row() -> None:
    """Every provisioning.* capability is infrastructure-mutating or
    infrastructure-reading — none are brain-callable. All three carry
    authority=operator_only per the proto annotations."""
    for row in _rows():
        assert row["authority"] == "operator_only"


def test_capability_rows_applies_timeout_override_for_apply_tenant_resources() -> None:
    """`provisioning.apply_tenant_resources` gets the 180s override — this
    module is the ONLY writer of that row's `public.capabilities` entry (no
    pneuma-engine service walks this proto's descriptor), so the row must
    carry the correct value directly rather than relying on the schema
    default. Regression test: prior to this fix, `timeout_seconds` was never
    set at all here, so the row silently sat at the schema's `NOT NULL
    DEFAULT 30` forever — reconcile attempts hit DEADLINE_EXCEEDED and
    Temporal retried indefinitely (the SLO-breach root cause)."""
    by_name = {row["name"]: row for row in _rows()}
    assert by_name["provisioning.apply_tenant_resources"]["timeout_seconds"] == 180


def test_capability_rows_leaves_unmapped_provisioning_rpcs_at_schema_default() -> None:
    """Every other provisioning.* row keeps the schema's 30s default —
    the override is scoped to the one measured-slow capability, not a
    blanket change."""
    by_name = {row["name"]: row for row in _rows()}
    assert by_name["provisioning.destroy_tenant_resources"]["timeout_seconds"] == 30
    assert by_name["provisioning.read_tenant_state"]["timeout_seconds"] == 30


# ---------------------------------------------------------------------------
# _capability_rows — platform-tier descriptor (P5.2)
# ---------------------------------------------------------------------------


def _platform_rows() -> list[dict]:
    return _capability_rows([platform_provisioning_api_pb2.DESCRIPTOR], _GRPC_TARGET)


def test_platform_capability_rows_produces_exactly_the_two_platform_rpcs() -> None:
    rows = _platform_rows()
    assert len(rows) == 2
    assert {row["name"] for row in rows} == _EXPECTED_PLATFORM_NAMES


def test_platform_capability_rows_carry_grpc_internal_dispatch_shape() -> None:
    rows = _platform_rows()
    for row in rows:
        assert row["connector_mode"] == "grpc_internal"
        assert row["protocol"] == "grpc"
        assert row["webhook_url"] == _GRPC_TARGET
        assert row["capability_class"] == "platform_internal"
        assert row["is_advertised"] is False
        assert row["is_active"] is True


def test_platform_capability_rows_effect_and_authority_match_proto_annotations() -> None:
    """Per provisioning/platform/v1/platform_provisioning_api.proto: both
    RPCs are perform_external_action / operator_only / data_class
    operational (NOT secrets — invariant I2 would force is_active=false,
    which would defeat proto-registering these at all; the payload never
    carries secret VALUES, only path/resource identifiers)."""
    by_name = {row["name"]: row for row in _platform_rows()}
    for name in _EXPECTED_PLATFORM_NAMES:
        assert by_name[name]["effect"] == "perform_external_action"
        assert by_name[name]["authority"] == "operator_only"
        assert by_name[name]["risk_category"] == "act"
        assert by_name[name]["data_class"] == ["operational"]


def test_platform_and_tenant_capability_rows_combine_without_name_collision() -> None:
    """main.py's _sync_capabilities passes BOTH descriptors together —
    the 3 tenant-tier + 2 platform-tier capability names must never
    collide when synced in the same call."""
    combined = _capability_rows(
        [provisioning_api_pb2.DESCRIPTOR, platform_provisioning_api_pb2.DESCRIPTOR],
        _GRPC_TARGET,
    )
    names = [row["name"] for row in combined]
    assert len(names) == 5
    assert len(names) == len(set(names))
    assert set(names) == _EXPECTED_NAMES | _EXPECTED_PLATFORM_NAMES


# ---------------------------------------------------------------------------
# sync_provisioning_capabilities — insert + update paths (respx)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_sync_provisioning_capabilities_inserts_when_absent() -> None:
    respx.get(f"{_BASE_URL}/capabilities").mock(return_value=httpx.Response(200, json=[]))
    insert_route = respx.post(f"{_BASE_URL}/capabilities").mock(
        return_value=httpx.Response(201, json=[])
    )

    result = await sync_provisioning_capabilities(
        base_url=_BASE_URL,
        service_key="svc-key",
        file_descriptors=[provisioning_api_pb2.DESCRIPTOR],
        grpc_target=_GRPC_TARGET,
    )

    assert result == {"inserted": 3, "updated": 0, "total": 3}
    assert insert_route.call_count == 3


@pytest.mark.asyncio
@respx.mock
async def test_sync_provisioning_capabilities_updates_when_present() -> None:
    existing = [
        {"id": "id-1", "name": "provisioning.apply_tenant_resources", "owning_tenant_id": None},
        {"id": "id-2", "name": "provisioning.destroy_tenant_resources", "owning_tenant_id": None},
        {"id": "id-3", "name": "provisioning.read_tenant_state", "owning_tenant_id": None},
    ]
    respx.get(f"{_BASE_URL}/capabilities").mock(return_value=httpx.Response(200, json=existing))
    update_route = respx.patch(f"{_BASE_URL}/capabilities").mock(
        return_value=httpx.Response(200, json=[])
    )

    result = await sync_provisioning_capabilities(
        base_url=_BASE_URL,
        service_key="svc-key",
        file_descriptors=[provisioning_api_pb2.DESCRIPTOR],
        grpc_target=_GRPC_TARGET,
    )

    assert result == {"inserted": 0, "updated": 3, "total": 3}
    assert update_route.call_count == 3


@pytest.mark.asyncio
@respx.mock
async def test_sync_provisioning_capabilities_insert_payload_carries_override() -> None:
    """First-ever boot (no existing row) — the INSERT payload must carry
    timeout_seconds=180 for provisioning.apply_tenant_resources directly, not
    rely on a later UPDATE to correct it."""
    respx.get(f"{_BASE_URL}/capabilities").mock(return_value=httpx.Response(200, json=[]))
    insert_route = respx.post(f"{_BASE_URL}/capabilities").mock(
        return_value=httpx.Response(201, json=[])
    )

    await sync_provisioning_capabilities(
        base_url=_BASE_URL,
        service_key="svc-key",
        file_descriptors=[provisioning_api_pb2.DESCRIPTOR],
        grpc_target=_GRPC_TARGET,
    )

    bodies = [
        json.loads(call.request.content)
        for call in insert_route.calls
    ]
    by_name = {b["name"]: b for b in bodies}
    assert by_name["provisioning.apply_tenant_resources"]["timeout_seconds"] == 180


@pytest.mark.asyncio
@respx.mock
async def test_sync_provisioning_capabilities_update_payload_reasserts_override() -> None:
    """A later reconcile (row already exists, e.g. pod restart after an
    out-of-band write left it at 30) — the UPDATE (PATCH) payload must
    re-assert timeout_seconds=180 rather than omitting the key, so the row
    is deterministically 180 regardless of write order relative to any
    other process touching this row."""
    existing = [
        {"id": "id-1", "name": "provisioning.apply_tenant_resources", "owning_tenant_id": None},
        {"id": "id-2", "name": "provisioning.destroy_tenant_resources", "owning_tenant_id": None},
        {"id": "id-3", "name": "provisioning.read_tenant_state", "owning_tenant_id": None},
    ]
    respx.get(f"{_BASE_URL}/capabilities").mock(return_value=httpx.Response(200, json=existing))
    update_route = respx.patch(f"{_BASE_URL}/capabilities").mock(
        return_value=httpx.Response(200, json=[])
    )

    await sync_provisioning_capabilities(
        base_url=_BASE_URL,
        service_key="svc-key",
        file_descriptors=[provisioning_api_pb2.DESCRIPTOR],
        grpc_target=_GRPC_TARGET,
    )

    bodies = [
        json.loads(call.request.content)
        for call in update_route.calls
    ]
    by_name = {b["name"]: b for b in bodies}
    assert by_name["provisioning.apply_tenant_resources"]["timeout_seconds"] == 180


@pytest.mark.asyncio
@respx.mock
async def test_sync_provisioning_capabilities_closes_client_on_success() -> None:
    respx.get(f"{_BASE_URL}/capabilities").mock(return_value=httpx.Response(200, json=[]))
    respx.post(f"{_BASE_URL}/capabilities").mock(return_value=httpx.Response(201, json=[]))

    # No explicit close assertion possible on the internal client without
    # reaching into private state — the real regression this guards is an
    # unhandled exception leaking an open client; respx tears down the
    # transport, so a leaked/unclosed client would surface as a
    # ResourceWarning under `pytest -W error::ResourceWarning`. Exercised
    # here as a smoke pass: the call must complete without raising.
    result = await sync_provisioning_capabilities(
        base_url=_BASE_URL,
        service_key="svc-key",
        file_descriptors=[provisioning_api_pb2.DESCRIPTOR],
        grpc_target=_GRPC_TARGET,
    )
    assert result["total"] == 3


# ---------------------------------------------------------------------------
# CapabilityRegistry.find_tenant — control-schema PostgREST profile header
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_find_tenant_uses_control_schema_profile_header() -> None:
    def _check(request: httpx.Request) -> httpx.Response:
        assert request.headers["Accept-Profile"] == "control"
        assert request.headers["Content-Profile"] == "control"
        return httpx.Response(
            200,
            json=[{"id": "t-1", "slug": "acme", "compliance_profile": None}],
        )

    respx.get(f"{_BASE_URL}/tenants").mock(side_effect=_check)

    registry = CapabilityRegistry(_BASE_URL, "svc-key")
    try:
        tenant = await registry.find_tenant("t-1")
    finally:
        await registry.aclose()

    assert tenant == {"id": "t-1", "slug": "acme", "compliance_profile": None}


@pytest.mark.asyncio
@respx.mock
async def test_find_tenant_returns_none_when_absent() -> None:
    respx.get(f"{_BASE_URL}/tenants").mock(return_value=httpx.Response(200, json=[]))

    registry = CapabilityRegistry(_BASE_URL, "svc-key")
    try:
        tenant = await registry.find_tenant("no-such-tenant")
    finally:
        await registry.aclose()

    assert tenant is None
