# syntax=docker/dockerfile:1.6
#
# pneuma-terraformer — privileged FastAPI service that executes terraform
# CLI against the per-tenant module to apply / destroy tenant resources.
#
# Image:    ghcr.io/deanmak13/pneuma-terraformer
# Source:   pneuma-engine/services/terraformer/Dockerfile
# Consumed by: pneuma-helm-charts/charts/pneuma-terraformer/
#
# Capability surface: provisioning.apply_tenant_resources /
# provisioning.destroy_tenant_resources / provisioning.read_tenant_state.
# Dispatched by `core:tenant_apply_resources` cycle (Workstream A #1).
#
# The tenant Terraform module is baked into the image at
# /app/infrastructure/terraform/modules/tenant/ — copied from the
# pneuma-deployments submodule at build time. State backend is S3-
# compatible (MinIO) configured at runtime via env vars.

# ---- Terraform binary ----
FROM hashicorp/terraform:1.9 AS tf

# ---- Builder stage ----
FROM ghcr.io/deanmak13/pneuma-builder:latest AS builder
COPY pyproject.toml ./
COPY services/common/ ./services/common/
COPY services/terraformer/src/ ./services/terraformer/src/
RUN --mount=type=cache,target=/root/.cache/uv pip-install.sh

# ---- Runtime stage ----
FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -m -d /var/lib/terraformer -s /bin/false appuser \
    && mkdir -p /var/lib/terraformer/workspaces /app/infrastructure/terraform/modules \
    && chown -R appuser:appuser /var/lib/terraformer

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin/uvicorn /usr/local/bin/uvicorn
COPY --from=tf      /bin/terraform /usr/local/bin/terraform

COPY services/common/ ./services/common/
COPY services/terraformer/src/ ./services/terraformer/src/

# The tenant TF module is sourced from the pneuma-deployments submodule
# at build time. The build context MUST include
# pneuma-deployments/infrastructure/terraform/modules/tenant/ — the CI
# workflow checks out submodules: true to satisfy this.
COPY pneuma-deployments/infrastructure/terraform/modules/tenant/ \
     /app/infrastructure/terraform/modules/tenant/

USER appuser
EXPOSE 8011

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8011/health')"]

CMD ["uvicorn", "services.terraformer.src.main:app", "--host", "0.0.0.0", "--port", "8011"]
