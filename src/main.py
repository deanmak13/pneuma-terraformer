"""pneuma-terraformer — FastAPI service implementing provisioning.* capabilities.

Privileged internal service: shells out to the `terraform` CLI against
the tenant TF module baked at /app/infrastructure/terraform/modules/tenant/.
State stored in MinIO (S3-compatible) under tenants/<tenant_id>.tfstate.

The cycle-executor dispatches `provisioning.apply_tenant_resources` here
via the `core:tenant_apply_resources` cycle (Workstream A #1).

Replicas pinned to 1 by chart values — the runner relies on per-tenant
process-local asyncio.Locks for serialisation. Horizontal scaling would
require state coordination (e.g. DynamoDB-style locking).
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from services.terraformer.src.routes import health, provisioning
from services.terraformer.src.settings import get_settings


def _configure_logging() -> None:
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


async def _sync_capabilities(settings) -> None:
    """Auto-register terraformer's provisioning.* capabilities from the
    proto descriptors. Mirrors the pattern used by brain / mimesis /
    admin-api: every service that owns proto `option (capability) = {...}`
    annotations calls `sync_capabilities_from_proto` at startup, which
    upserts `public.capabilities` rows pointing the cycle-executor at
    this pod's self_url. Without this, cycle dispatch of
    `provisioning.apply_tenant_resources` / `.destroy_tenant_resources`
    returns 'capability not found' because the implementation row never
    appears in the DB.
    """
    logger = logging.getLogger("terraformer.proto_sync")
    try:
        from pneuma_proto.v1.provisioning import provisioning_api_pb2
        from services.common.db.client import Database
        from services.common.proto_capability_sync import (
            sync_capabilities_from_proto,
        )
    except ImportError:
        logger.exception(
            "terraformer proto_capability_sync IMPORT FAILED — "
            "service will start without capability registration. "
            "Check pneuma-proto wheel version + services.common path."
        )
        return

    try:
        db = Database.from_config(
            base_url=settings.supabase_url,
            service_key=settings.supabase_service_key.get_secret_value(),
        )
        result = await sync_capabilities_from_proto(
            db,
            host_service="terraformer",
            file_descriptors=[provisioning_api_pb2.DESCRIPTOR],
            webhook_url=settings.computed_self_url,
        )
        logger.info(
            "terraformer proto_capability_sync complete: "
            "upserted=%d soft_deleted=%d errors=%d webhook_url=%s",
            result.get("capabilities_upserted", 0),
            result.get("soft_deleted", 0),
            len(result.get("errors", [])),
            settings.computed_self_url,
        )
    except Exception:
        logger.exception(
            "terraformer proto_capability_sync FAILED — service will start "
            "but provisioning.* capabilities will NOT be dispatchable. "
            "Operator must investigate before relying on cycle dispatch."
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    _configure_logging()
    settings = get_settings()
    settings.terraform_workdir_root.mkdir(parents=True, exist_ok=True)
    logging.getLogger("terraformer").info(
        "terraformer starting: env=%s modules=%s workdir=%s self_url=%s",
        settings.env,
        settings.terraform_modules_root,
        settings.terraform_workdir_root,
        settings.computed_self_url,
    )
    await _sync_capabilities(settings)
    yield


app = FastAPI(
    title="pneuma-terraformer",
    description="Provisioning capability service — shells out to terraform CLI.",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(health.router)
app.include_router(provisioning.router)
