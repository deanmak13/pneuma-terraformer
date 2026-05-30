# syntax=docker/dockerfile:1.6
#
# pneuma-terraformer — privileged FastAPI service that shells out to the
# `terraform` CLI against the per-tenant Terraform module.
#
# Image:        ghcr.io/deanmak13/pneuma-terraformer
# Origin:       pneuma-engine/services/terraformer/ (pre-relocation,
#               2026-05-31, see pneuma#301)
# Consumed by:  pneuma-helm-charts/charts/pneuma-terraformer/
# Deployed via: pneuma-deployments/platform/overlays/<env>/pneuma-terraformer/
#
# Capability surface (proto-served):
#   - provisioning.apply_tenant_resources
#   - provisioning.destroy_tenant_resources
#   - provisioning.read_tenant_state
#
# Dispatched as cycle steps from `core:tenant_apply_resources` /
# `core:tenant_destroy_resources` in pneuma-engine. The destroy path is
# gated by the cycle-executor pre-run validator chain (PR 4 + 6 of the
# cycle-prerun plan) — never invoke directly.
#
# Tenant TF module: NOT baked into the image. Mounted at runtime by the
# helm chart from a sidecar / init-container at
# /app/infrastructure/terraform/modules/tenant/. Keeps the image
# vendor-agnostic so a module-shape change does not require a rebuild.

# ---- Terraform binary ----
FROM hashicorp/terraform:1.9 AS tf

# ---- Builder stage ----
# Resolves the pyproject.toml deps into a venv. `services.common.*` and
# `pneuma_proto` are OPTIONAL imports — main.py's _sync_capabilities is
# wrapped in try/except ImportError and the service starts (degraded,
# without capability auto-registration) if either is absent. Once
# `pneuma-common` (Onboard-08 PR1) publishes, the dep can be added here
# and the fallback removed.
FROM python:3.12-slim AS builder

WORKDIR /build
RUN pip install --no-cache-dir --upgrade pip wheel build

COPY pyproject.toml ./
COPY services/ ./services/
RUN pip install --no-cache-dir --prefix=/install .

# ---- Runtime stage ----
FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -m -d /var/lib/terraformer -s /bin/false appuser \
    && mkdir -p /var/lib/terraformer/workspaces /app/infrastructure/terraform/modules \
    && chown -R appuser:appuser /var/lib/terraformer

WORKDIR /app

# Resolved site-packages from builder.
COPY --from=builder /install/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /install/bin /usr/local/bin

# Terraform binary.
COPY --from=tf      /bin/terraform /usr/local/bin/terraform

# Application source — same `services/terraformer/...` layout the engine
# used pre-relocation, so the verbatim-copied imports still resolve.
COPY services/ /app/services/

USER appuser
EXPOSE 8011

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8011/health')"]

CMD ["uvicorn", "services.terraformer.src.main:app", "--host", "0.0.0.0", "--port", "8011"]
