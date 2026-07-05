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
