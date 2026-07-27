"""terraformer settings — env-driven Pydantic configuration.

The service consumes admin credentials via ExternalSecret-mounted env vars
(see pneuma-helm-charts/charts/pneuma-terraformer/secrets.schema.yaml).
Credentials are required for the enabled provisioning surface. Provider
credentials for inactive infrastructure backends stay optional so a Contabo /
k3s cluster does not require unrelated Hetzner secrets at startup.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=None, case_sensitive=False, extra="ignore")

    env: str = Field(..., description="Cluster env: tst / prod-standard / prod-regulated")
    log_level: str = "INFO"

    admin_api_key: str = Field(..., min_length=16, description="Shared key for X-Admin-Key gate")

    # Pneuma control-plane DB access — required so the proto_capability_sync
    # at startup can upsert terraformer's provisioning.* rows into
    # public.capabilities. Without this, cycle-executor dispatch returns
    # 'capability not found'. Matches the same Settings shape as brain /
    # mimesis / admin-api.
    supabase_url: str = Field(
        ...,
        description="Supabase REST endpoint (in-cluster service URL).",
    )
    supabase_service_key: SecretStr = Field(
        ...,
        description="Supabase service-role JWT for typed-ORM access.",
    )

    terraform_binary: str = "/usr/local/bin/terraform"
    terraform_modules_root: Path = Field(
        default=Path("/app/infrastructure/terraform/modules"),
        description="Root containing tenant/ and bootstrap/ TF modules — baked into the image",
    )
    terraform_workdir_root: Path = Field(
        default=Path("/var/lib/terraformer/workspaces"),
        description="Per-tenant workspace dir; one subdirectory per tenant_id",
    )
    terraform_standalone_root: Path = Field(
        default=Path("/app/infrastructure/terraform/standalone"),
        description="Root containing standalone Terraform harnesses (platform-secrets-apply/, etc.). Sibling of terraform_modules_root.",
    )
    tf_cli_config_file: str = Field(
        default="/app/tf/cli.tfrc",
        description=(
            "Path to the Terraform CLI config (tf/cli.tfrc, baked into the "
            "image) that points provider installation at the filesystem "
            "plugin mirror baked at /opt/tf-plugin-mirror — set as "
            "TF_CLI_CONFIG_FILE on every terraform subprocess. A plain "
            "string field (not Path) so tests can point it at a tmp_path "
            "fixture without touching the real /app/tf/cli.tfrc."
        ),
    )
    platform_helm_charts_dir: Path = Field(
        default=Path("/charts"),
        description="Mount path of the pneuma-helm-charts checkout — read by the platform-secrets module's fileset() at plan time. Chart values mount this from a git-sync sidecar or read-only ConfigMap.",
    )

    tf_state_backend_endpoint: str = Field(..., description="MinIO S3-compatible endpoint URL")
    tf_state_backend_bucket: str = Field(default="pneuma-tf-state")
    tf_state_backend_region: str = Field(default="us-east-1")
    tf_state_backend_access_key: str = Field(..., min_length=3)
    tf_state_backend_secret_key: str = Field(..., min_length=8)

    tenant_infra_provider: Literal["in_cluster", "hetzner"] = Field(
        default="in_cluster",
        description=(
            "Tenant resource provider. in_cluster manages the current k3s "
            "data-plane resources; hetzner additionally requires external "
            "provider credentials."
        ),
    )
    hetzner_api_token: str | None = Field(default=None, min_length=32)
    cloudflare_api_token: str | None = Field(default=None, min_length=32)
    postgres_superuser_password: str = Field(..., min_length=8)
    rabbitmq_admin_password: str = Field(..., min_length=8)
    minio_admin_password: str = Field(..., min_length=8)
    openbao_admin_token: str = Field(..., min_length=8)

    apply_timeout_seconds: int = 600
    destroy_timeout_seconds: int = 600

    #: Max terraform subprocesses running at once. Each run loads ~5 provider
    #: plugins as child processes, so this is the process's real memory knob.
    #: Unbounded spawning OOMKilled the pod (2026-07-27): six concurrent
    #: applies in 13s against a 1Gi limit. Tunable per environment/pod size.
    #: ge=1: this is projected via envFrom: configMapRef, so a values-file
    #: typo of "0" is one edit away — asyncio.Semaphore(0) blocks every
    #: acquire forever, and /health+/ready never touch the runner, so the
    #: service would wedge with green health checks and no error logged.
    max_concurrent_terraform_runs: int = Field(default=2, ge=1)

    #: Max seconds a terraform dispatch will wait to ACQUIRE a concurrency
    #: slot before failing fast with exit_code=124 — bounds the wait, not
    #: the run. Without this, a read-only `terraform output -json` (its
    #: own timeout is 30s) can queue behind concurrent 600s applies for
    #: up to ~20 minutes. Default kept below the ~60s per-attempt gRPC
    #: deadline the cycle-executor sets on provisioning dispatches
    #: (pneuma-engine services/cycle_executor/src/connectors/
    #: grpc_internal.py:252, activities.py:746-752) so a saturated runner
    #: fails cleanly from our own code — with a log line and a
    #: TerraformResult — instead of the caller's transport silently
    #: cancelling the handler with nothing in our logs.
    spawn_queue_timeout_seconds: int = Field(default=45, ge=1)

    metrics_port: int = 9001

    # Self-URL projected into capability_implementations.webhook_url at
    # proto-sync time. Cycle-executor dispatches `provisioning.*`
    # capabilities by reading this URL from the row. Empty default
    # follows the brain pattern: `computed_self_url` derives the in-
    # cluster DNS form from pneuma_namespace + terraformer_port unless
    # an explicit override is provided via TERRAFORMER_SELF_URL.
    pneuma_namespace: str = Field(
        default="platform-tst",
        description="In-cluster namespace; used to derive self_url when not overridden.",
    )
    terraformer_port: int = 8011
    grpc_port: int = 8012
    self_url: str = Field(
        default="",
        description=(
            "Optional override for the URL that cycle-executor hits to "
            "dispatch provisioning.* capabilities. Empty = derive from "
            "pneuma_namespace + terraformer_port at registration time."
        ),
    )

    @property
    def computed_self_url(self) -> str:
        if self.self_url:
            return self.self_url
        return f"http://terraformer.{self.pneuma_namespace}.svc.cluster.local:{self.terraformer_port}"

    @property
    def computed_grpc_target(self) -> str:
        return f"terraformer.{self.pneuma_namespace}.svc.cluster.local:{self.grpc_port}"

    @model_validator(mode="after")
    def _validate_enabled_provider_credentials(self) -> Settings:
        if self.tenant_infra_provider != "hetzner":
            return self
        missing = [
            name
            for name in ("HETZNER_API_TOKEN", "CLOUDFLARE_API_TOKEN")
            if not getattr(self, name.lower())
        ]
        if missing:
            joined = ", ".join(missing)
            raise ValueError(
                f"TENANT_INFRA_PROVIDER=hetzner requires seeded {joined}"
            )
        return self


_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
