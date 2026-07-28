"""HTTP routes implementing provisioning.* capabilities.

These routes are the dispatch target for the `core:tenant_apply_resources`
cycle (services/common/cycle_registries/onboarding_cycles.py). The cycle
fires from `create_tenant` step 7 with capability `provisioning.apply_tenant_resources`,
which resolves to POST /provisioning/reconcile via capability_implementations
dispatch_type=http_internal.

gRPC layer (mirroring pneuma-proto ProvisioningService) will be added in a
follow-up PR once proto #36 publishes — see PR body. The HTTP layer is
the demo-unblock surface and the long-term contract for capability dispatch.
"""

from __future__ import annotations

import logging
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Path, status
from pydantic import BaseModel, Field

from services.terraformer.src.auth import require_admin_key
from services.terraformer.src.openbao_bootstrap import ensure_platform_auth
from services.terraformer.src.settings import get_settings
from services.terraformer.src.terraform_runner import (
    PlatformSecretsInputs,
    TenantInputs,
    TerraformError,
    get_runner,
    scrub_credentials,
)

_LOG = logging.getLogger("terraformer.provisioning")

router = APIRouter(
    prefix="/provisioning",
    tags=["provisioning"],
    dependencies=[Depends(require_admin_key)],
)

# Strict allowlist for tenant_id — used as a filesystem path component
# inside `_workspace_dir`. Without this guard a caller with a valid
# admin-key could send `tenant_id="../../etc"` and escape the workdir
# root, overwriting TF modules or reading arbitrary paths. The pattern
# matches lowercase alphanumerics + hyphens, 1–63 chars (k8s name shape).
_TENANT_ID_PATTERN = r"^[a-z0-9][a-z0-9\-]{0,62}$"


class TenantRequest(BaseModel):
    # `pattern` is the path-traversal guard — see _TENANT_ID_PATTERN.
    tenant_id: str = Field(..., min_length=1, pattern=_TENANT_ID_PATTERN)
    tenant_slug: str = Field(..., min_length=1, pattern=_TENANT_ID_PATTERN)
    env: str = Field(..., min_length=1, pattern=r"^[a-z0-9][a-z0-9\-]{0,31}$")
    # Terraform's tenant module has no "standard" tier value — the
    # non-regulated contract is `profile == null` (infrastructure/terraform/
    # modules/tenant/variables.tf, mirrored by the control.tenants.
    # compliance_profile CHECK constraint in pneuma-engine). Default to
    # None, not a magic string; TerraformRunner._normalize_profile is the
    # defense-in-depth seam for any caller that still sends "standard".
    compliance_profile: str | None = Field(
        default=None, pattern=r"^[a-z0-9][a-z0-9\-_]{0,31}$"
    )
    pooled_namespace: str = Field(..., min_length=1, pattern=_TENANT_ID_PATTERN)

    def to_inputs(self) -> TenantInputs:
        return TenantInputs(
            tenant_id=self.tenant_id,
            tenant_slug=self.tenant_slug,
            env=self.env,
            compliance_profile=self.compliance_profile,
            pooled_namespace=self.pooled_namespace,
        )


class TenantResult(BaseModel):
    tenant_id: str
    outputs: dict[str, Any]
    stdout_tail: str
    stderr_tail: str


def _tail(s: str, n: int = 4000) -> str:
    """Tail the last N chars AND scrub any credential strings the TF
    output might have leaked. The runner exports admin tokens
    (Hetzner, Cloudflare, OpenBao, Postgres superuser, RMQ admin,
    MinIO admin) via ``-var`` and the AWS_*_KEY env vars — TF doesn't
    print them by default, but provider errors can include partial
    backend-config dumps. Scrub at the seam where the string crosses
    the HTTP boundary so a noisy provider error can't exfiltrate."""
    truncated = s[-n:] if len(s) > n else s
    return scrub_credentials(truncated)


@router.post("/reconcile", response_model=TenantResult)
async def reconcile(req: TenantRequest) -> TenantResult:
    runner = get_runner()
    try:
        result = await runner.reconcile(req.to_inputs())
    except TerraformError as exc:
        _LOG.error("reconcile failed for tenant=%s: %s", req.tenant_id, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "tenant_id": req.tenant_id,
                "phase": exc.command,
                "stderr_tail": _tail(exc.result.stderr),
            },
        ) from exc
    return TenantResult(
        tenant_id=req.tenant_id,
        outputs=result.outputs,
        stdout_tail=_tail(result.stdout),
        stderr_tail=_tail(result.stderr),
    )


@router.post("/destroy", response_model=TenantResult)
async def destroy(req: TenantRequest) -> TenantResult:
    runner = get_runner()
    try:
        result = await runner.destroy(req.to_inputs())
    except TerraformError as exc:
        _LOG.error("destroy failed for tenant=%s: %s", req.tenant_id, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "tenant_id": req.tenant_id,
                "phase": exc.command,
                "stderr_tail": _tail(exc.result.stderr),
            },
        ) from exc
    return TenantResult(
        tenant_id=req.tenant_id,
        outputs={},
        stdout_tail=_tail(result.stdout),
        stderr_tail=_tail(result.stderr),
    )


@router.get("/state/{tenant_id}")
async def state(
    tenant_id: str = Path(..., pattern=_TENANT_ID_PATTERN),  # noqa: B008
) -> dict[str, Any]:
    """Read the tenant's TF state — read-only. Does NOT trigger an
    init/apply. If the workspace hasn't been bootstrapped yet, the
    runner returns ``{"exists": False, "outputs": {}}``."""
    runner = get_runner()
    return {"tenant_id": tenant_id, **await runner.state(tenant_id)}


# ---------------------------------------------------------------------------
# provisioning.apply_platform_secrets
#
# Dispatch target for `core:platform_apply_secret_reconcile` cycle
# (engine#839). The cycle fires from operator_portal / scheduled triggers
# with capability `provisioning.apply_platform_secrets`, which resolves to
# POST /provisioning/reconcile-platform-secrets via
# capability_implementations dispatch_type=http_internal.
#
# Runs the standalone harness at
# `infrastructure/terraform/standalone/platform-secrets-apply` against
# the env-scoped workspace. The harness reads every chart's
# secrets.schema.yaml from settings.platform_helm_charts_dir and fans
# canonical OpenBao paths (pneuma/internal/<env>/<vendor>) into
# per-service paths (pneuma/platform/pneuma/<env>/<svc>).
# ---------------------------------------------------------------------------


class PlatformSecretsRequest(BaseModel):
    # Pydantic-validated env — closed set matches the
    # declarative-infra-via-terraform LAW + the platform-secrets
    # module's variable.tf validation rules.
    env: Literal["dev", "tst", "prod"]


class PlatformSecretsResult(BaseModel):
    env: str
    outputs: dict[str, Any]
    stdout_tail: str
    stderr_tail: str


@router.post("/reconcile-platform-secrets", response_model=PlatformSecretsResult)
async def reconcile_platform_secrets(req: PlatformSecretsRequest) -> PlatformSecretsResult:
    runner = get_runner()
    try:
        result = await runner.reconcile_platform_secrets(
            PlatformSecretsInputs(env=req.env)
        )
    except TerraformError as exc:
        _LOG.error(
            "platform-secrets reconcile failed for env=%s: %s", req.env, exc,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "env": req.env,
                "phase": exc.command,
                "stderr_tail": _tail(exc.result.stderr),
            },
        ) from exc
    return PlatformSecretsResult(
        env=req.env,
        outputs=result.outputs,
        stdout_tail=_tail(result.stdout),
        stderr_tail=_tail(result.stderr),
    )


# ---------------------------------------------------------------------------
# Admin: on-demand OpenBao auth bootstrap
#
# Exposes the exact same convergence services.terraformer.src.main runs
# automatically at every boot (see _ensure_openbao_auth there) — one source
# of truth, callable on demand for operator-triggered re-checks without a
# pod restart. Mounted on this router (final path
# /provisioning/admin/bootstrap/openbao-auth) so it shares the X-Admin-Key
# gate + error-shape conventions every other route here uses.
# ---------------------------------------------------------------------------


class OpenBaoAuthBootstrapResult(BaseModel):
    role: str
    mount: str
    action: str


@router.post("/admin/bootstrap/openbao-auth", response_model=OpenBaoAuthBootstrapResult)
async def bootstrap_openbao_auth() -> OpenBaoAuthBootstrapResult:
    settings = get_settings()
    runner = get_runner()
    try:
        action = await ensure_platform_auth(settings, runner)
    except TerraformError as exc:
        _LOG.error("openbao auth bootstrap failed at phase=%s: %s", exc.command, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"phase": exc.command, "stderr_tail": _tail(exc.result.stderr)},
        ) from exc
    except Exception as exc:  # noqa: BLE001 — surfaced verbatim; these carry no secrets
        _LOG.error("openbao auth bootstrap failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)
        ) from exc
    return OpenBaoAuthBootstrapResult(
        role=settings.vault_k8s_auth_role,
        mount=settings.vault_k8s_auth_mount,
        action=action,
    )
