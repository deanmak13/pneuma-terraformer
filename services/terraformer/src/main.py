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
    """Auto-register Terraformer's provisioning.* gRPC capabilities."""
    logger = logging.getLogger("terraformer.proto_sync")
    try:
        from pneuma_proto.provisioning.provisioning.v1 import provisioning_api_pb2

        from services.terraformer.src.capability_sync import (
            sync_provisioning_capabilities,
        )
    except ImportError:
        logger.exception(
            "terraformer proto_capability_sync IMPORT FAILED — "
            "refusing to start without provisioning capability registration."
        )
        raise

    try:
        result = await sync_provisioning_capabilities(
            base_url=settings.supabase_url,
            service_key=settings.supabase_service_key.get_secret_value(),
            file_descriptors=[provisioning_api_pb2.DESCRIPTOR],
            grpc_target=settings.computed_grpc_target,
        )
        logger.info(
            "terraformer proto_capability_sync complete: "
            "inserted=%d updated=%d total=%d grpc_target=%s",
            result["inserted"],
            result["updated"],
            result["total"],
            settings.computed_grpc_target,
        )
    except Exception:
        logger.exception(
            "terraformer proto_capability_sync FAILED — refusing to start "
            "because provisioning.* capabilities would not be dispatchable."
        )
        raise


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
    from services.terraformer.src.grpc_server import start_grpc_server

    grpc_server = await start_grpc_server(settings)
    try:
        yield
    finally:
        await grpc_server.stop(grace=5)


app = FastAPI(
    title="pneuma-terraformer",
    description="Provisioning capability service — shells out to terraform CLI.",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(health.router)
app.include_router(provisioning.router)
