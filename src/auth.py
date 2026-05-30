"""X-Admin-Key bearer auth for terraformer HTTP routes.

terraformer is a privileged internal service — it mutates third-party infra
(Postgres roles, RMQ vhosts, MinIO buckets, OpenBao paths, DNS records).
Every route except /health and /metrics requires X-Admin-Key. The caller
is always the cycle-executor pod dispatching `provisioning.apply_tenant_resources`.
"""

from __future__ import annotations

from fastapi import Header, HTTPException, status

from services.terraformer.src.settings import get_settings


async def require_admin_key(x_admin_key: str | None = Header(default=None)) -> None:
    expected = get_settings().admin_api_key
    if not x_admin_key or x_admin_key != expected:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="X-Admin-Key header missing or invalid",
        )
