"""Register Terraformer-owned provisioning capabilities in public.capabilities."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx
from pneuma_proto.metadata.v1 import capability_options_pb2

_ACTING_EFFECTS = {"perform_external_action", "send_external_comm"}
_ALLOWED_EFFECTS = {
    "read_only",
    "mutate_pneuma_state",
    "mutate_tenant_state",
    "send_external_comm",
    "perform_external_action",
}

# MIRROR of pneuma-engine's `services/common/proto_capability_sync.py`
# `TIMEOUT_SECONDS_OVERRIDES` — kept in sync by hand because tf-runner has no
# import path into that repo (this service depends only on the shared
# `pneuma_proto` wheel, not on pneuma-engine's `services.common` package).
#
# `provisioning.apply_tenant_resources` (RunTenantReconcile, host_service
# "tf-runner") is registered EXCLUSIVELY by this module — no pneuma-engine
# service ever walks provisioning_api.proto's descriptor, so pneuma-engine's
# copy of this override never actually fires for this capability. This
# module is that row's ONLY writer; previously it never set `timeout_seconds`
# at all, so the row silently sat at the `public.capabilities` schema's
# `NOT NULL DEFAULT 30` forever — reproducing, permanently, the exact
# DEADLINE_EXCEEDED-retries-forever failure pneuma-engine#1868 believed it
# had fixed. RunTenantReconcile measured 45-55s per tenant (terraformer
# process-spawn logs, QA round 6, 2026-08-18); 180s is >=3x that worst case.
# Add an entry here (and mirror it in pneuma-engine's map, with a comment
# cross-referencing both files) only when a capability's real-world duration
# is measured to exceed the schema's 30s default.
_TIMEOUT_SECONDS_OVERRIDES: dict[str, int] = {
    "provisioning.apply_tenant_resources": 180,
}


def _risk_category(effect: str) -> str:
    if effect == "read_only":
        return "observe"
    if effect in _ACTING_EFFECTS:
        return "act"
    return "internal"


class CapabilityRegistry:
    """Small typed PostgREST surface for the capability registry table."""

    def __init__(self, base_url: str, service_key: str, timeout: float = 10.0):
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            headers={
                "Authorization": f"Bearer {service_key}",
                "apikey": service_key,
                "Content-Type": "application/json",
                "Accept-Profile": "public",
                "Content-Profile": "public",
            },
            timeout=timeout,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def list_by_names(self, names: list[str]) -> list[dict[str, Any]]:
        if not names:
            return []
        resp = await self._client.get(
            "/capabilities",
            params={
                "select": "id,name,owning_tenant_id",
                "name": f"in.({','.join(names)})",
                "owning_tenant_id": "is.null",
                "limit": str(len(names) + 100),
            },
        )
        resp.raise_for_status()
        return resp.json()

    async def find_tenant(self, tenant_id: str) -> dict[str, Any] | None:
        resp = await self._client.get(
            "/tenants",
            params={
                "select": "id,slug,compliance_profile",
                "id": f"eq.{tenant_id}",
                "limit": "1",
            },
            headers={"Accept-Profile": "control", "Content-Profile": "control"},
        )
        resp.raise_for_status()
        rows = resp.json()
        return rows[0] if rows else None

    async def insert(self, row: dict[str, Any]) -> None:
        resp = await self._client.post(
            "/capabilities",
            json=row,
            headers={"Prefer": "return=minimal"},
        )
        resp.raise_for_status()

    async def update(self, row_id: str, row: dict[str, Any]) -> None:
        resp = await self._client.patch(
            "/capabilities",
            params={"id": f"eq.{row_id}"},
            json=row,
            headers={"Prefer": "return=minimal"},
        )
        resp.raise_for_status()


def _capability_rows(file_descriptors: list[Any], grpc_target: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    classified_at = datetime.now(timezone.utc).isoformat()

    for fd in file_descriptors:
        for service_proto in fd.services_by_name.values():
            for method in service_proto.methods:
                opts = method.GetOptions()
                if not opts.HasExtension(capability_options_pb2.capability):
                    continue
                meta = opts.Extensions[capability_options_pb2.capability]
                name = meta.name
                if not name:
                    raise ValueError(
                        f"Provisioning method {method.name} is missing capability.name"
                    )
                effect = meta.effect or "read_only"
                if effect not in _ALLOWED_EFFECTS:
                    raise ValueError(
                        f"Provisioning capability {name!r} has invalid effect {effect!r}"
                    )
                exposure = list(meta.exposure)
                data_class = list(meta.data_class) or ["none"]
                authority = meta.authority or "operator_only"
                host = meta.host_service or "tf-runner"

                rows.append(
                    {
                        "name": name,
                        "display_name": name,
                        "description": meta.description,
                        # proto 0.84.0 dropped the separate llm_description
                        # field; description is now the single copy used for
                        # both the human-facing row and LLM context.
                        "llm_context": meta.description,
                        "exposure": exposure,
                        "effect": effect,
                        "data_class": data_class,
                        "authority": authority,
                        "tags": list(meta.tags),
                        "is_active": True,
                        "is_advertised": False,
                        "connector_mode": "grpc_internal",
                        "connector_platform": None,
                        "protocol": "grpc",
                        "provider_ref": f"grpc_internal:{host}:{method.name}",
                        "webhook_url": grpc_target,
                        "taxonomy_axes": {
                            "exposure": exposure,
                            "effect": [effect],
                            "data_class": data_class,
                            "authority": authority,
                            "confidence": "operator_declared",
                            "classified_at": classified_at,
                            "_meta": {"source": "terraformer_capability_sync"},
                        },
                        "risk_category": _risk_category(effect),
                        "capability_class": "platform_internal",
                        "health_status": "healthy",
                        "provider_schema": {},
                        # Always written explicitly (never omitted) so every
                        # sync run — insert or update, in any boot order
                        # relative to pneuma-engine's own sync — reasserts
                        # the correct value instead of silently falling back
                        # to the schema's NOT NULL DEFAULT 30 on first
                        # INSERT and never being corrected afterward.
                        "timeout_seconds": _TIMEOUT_SECONDS_OVERRIDES.get(name, 30),
                    }
                )
    return rows


async def sync_provisioning_capabilities(
    *,
    base_url: str,
    service_key: str,
    file_descriptors: list[Any],
    grpc_target: str,
) -> dict[str, int]:
    rows = _capability_rows(file_descriptors, grpc_target)
    registry = CapabilityRegistry(base_url, service_key)
    try:
        existing = await registry.list_by_names([row["name"] for row in rows])
        existing_by_name = {row["name"]: row for row in existing if row.get("name")}

        inserted = 0
        updated = 0
        for row in rows:
            existing_row = existing_by_name.get(row["name"])
            if existing_row:
                await registry.update(existing_row["id"], row)
                updated += 1
            else:
                await registry.insert(row)
                inserted += 1
        return {"inserted": inserted, "updated": updated, "total": len(rows)}
    finally:
        await registry.aclose()
