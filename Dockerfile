# syntax=docker/dockerfile:1.6
#
# pneuma-terraformer — privileged FastAPI + gRPC service that shells out to
# the `terraform` CLI against the per-tenant Terraform module.
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
# Tenant TF module: BAKED into the image at build time from a pinned
# `pneuma-deployments` ref (see the DEPLOYMENTS_REF build-arg and the
# org.pneuma.deployments-ref image label below). CI (build-and-publish.yml)
# checks out deanmak13/pneuma-deployments at that ref into `_deployments/`
# (build context, gitignored) before invoking `docker build`; a local
# verification build performs the equivalent sparse checkout — see
# README.md "Local build". A module-shape change now requires an image
# rebuild (either a push to main, or `gh workflow run ... -f
# deployments_ref=<sha>`) — the trade-off is a reproducible,
# offline-capable image with zero runtime mount/sidecar dependency and a
# pinned Terraform provider plugin mirror (see the `mirror` stage below).

# ---- Terraform binary ----
FROM hashicorp/terraform:1.9 AS tf

# ---- Proto wheel ----
# Pinned (not :latest) — see docs/plans/2026-07-11-terraformer-onboarding-
# provisioning.md §2 "verify-before-pin" gate. Bumped 0.44.0 → 0.49.0
# (P5.2): 0.49.0 is the tag that ships
# `pneuma_proto.provisioning.platform.v1.platform_provisioning_api_pb2`
# — the `PlatformProvisioningService` service carrying
# `ApplyPlatformSecrets`/`ApplyPlatformBusTopology` — verified by pulling
# the GHCR wheel-carrier image and introspecting the extracted wheel
# directly (present at 0.49.0; absent below). Prior gate's fields
# (RunTenantDestroyRequest.authorized_by/.reason/.timeout_seconds,
# RunTenantReconcileRequest.timeout_seconds) remain present — no
# regression. pr-checks.yml guards against this regressing back to
# :latest.
# 0.86.0 (proto PR #155, published as v0.86.0/#156 — 0.84.0/0.85.0 were
# stale GHCR tags predating #155's fix; proto only publishes on a pushed
# git tag, not a plain merge to main): apply_tenant_resources/
# destroy_tenant_resources reclassified effect=mutate_pneuma_state (was
# `act`) — flips the served provisioning capability metadata's
# risk_category to internal on the next capability sync, pairing with
# engine main 9bdbfe9f's trust-tier gate fix. Verified via descriptor
# introspection of the published wheel before pinning.
FROM ghcr.io/deanmak13/pneuma-proto:0.86.0 AS proto

# ---- Terraform provider plugin mirror ----
# Pre-fetches every provider the tenant module's versions.tf declares so
# `terraform init` at apply-time never reaches the public registry — kills
# the ~15-45s/provider cold-download tax on every fresh tenant workspace.
# Only versions.tf (the required_providers block) is needed here, not the
# whole module — keeps this stage's build-context slice minimal.
FROM tf AS mirror
WORKDIR /mirror
COPY _deployments/infrastructure/terraform/modules/tenant/versions.tf ./
RUN terraform providers mirror -platform=linux_amd64 /opt/tf-plugin-mirror

# ---- Builder stage ----
# Resolves the pyproject.toml deps into a venv and installs the generated
# pneuma-proto wheel copied from the GHCR wheel image. Capability
# registration is fail-closed at startup; a Terraformer without proto stubs
# is not a dispatchable Terraformer.
#
# The proto wheel and the local package are installed in ONE pip
# invocation, both targeting --prefix=/install. `pip install --prefix=X`
# only installs a package into X if it isn't already satisfiable
# elsewhere on sys.path — so installing them as two separate commands
# (the first with no --prefix, landing in the builder stage's default
# site-packages) let pip silently skip copying transitive dependencies
# pulled in by the first install (e.g. the proto wheel's `pydantic` →
# `typing_extensions`) into /install on the second command, since pip
# considered them "already satisfied" globally. The runtime stage only
# COPYs /install, so those skipped packages were never in the final
# image — first surfaced as a `ModuleNotFoundError: No module named
# 'typing_extensions'` crash-loop on first-ever boot (2026-07-17). A
# single combined install resolves the full dependency graph together
# and lands every transitive package in /install exactly once — the fix
# is the build mechanism, not pinning the one package that happened to
# go missing this time.
#
# --ignore-installed forces the FULL resolved graph into --prefix
# regardless of what's already satisfiable on the ambient sys.path — the
# combined install alone only closes the skip for packages absent from
# the *pre-app* environment; `pip install --upgrade pip wheel build`
# above still leaves pip/wheel/build (+ their deps: packaging,
# pyproject_hooks, and possibly setuptools) on that ambient path before
# the app graph resolves, so an app/proto dependency that happens to
# overlap one of THOSE would hit the identical skip. --ignore-installed
# closes the mechanism completely rather than narrowing the window.
FROM python:3.12-slim AS builder

WORKDIR /build
RUN pip install --no-cache-dir --upgrade pip wheel build

COPY pyproject.toml ./
COPY services/ ./services/
COPY --from=proto /wheels /tmp/proto-wheels
RUN pip install --no-cache-dir --ignore-installed --prefix=/install /tmp/proto-wheels/pneuma_proto-*.whl .

# ---- Runtime stage ----
FROM python:3.12-slim

ARG DEPLOYMENTS_REF=unknown
LABEL org.pneuma.deployments-ref=$DEPLOYMENTS_REF

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 65532 appuser \
    && useradd --system --uid 65532 --gid 65532 -m -d /var/lib/terraformer -s /bin/false appuser \
    && mkdir -p /var/lib/terraformer/workspaces \
    && chown -R appuser:appuser /var/lib/terraformer

WORKDIR /app

# Resolved site-packages from builder.
COPY --from=builder /install/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /install/bin /usr/local/bin

# Terraform binary + pre-fetched provider plugin mirror.
COPY --from=tf     /bin/terraform /usr/local/bin/terraform
COPY --from=mirror /opt/tf-plugin-mirror /opt/tf-plugin-mirror

# Application source — same `services/terraformer/...` layout the engine
# used pre-relocation, so the verbatim-copied imports still resolve.
COPY services/ /app/services/

# Baked Terraform modules — pinned pneuma-deployments ref (see the header
# comment + DEPLOYMENTS_REF build-arg above). NOT mounted at runtime.
COPY _deployments/infrastructure/terraform/modules /app/infrastructure/terraform/modules
COPY _deployments/infrastructure/terraform/standalone /app/infrastructure/terraform/standalone

# CLI config wiring the runtime `terraform` binary to the baked filesystem
# provider mirror above — no registry reachability needed at apply time.
# See settings.tf_cli_config_file / TerraformRunner._spawn().
COPY tf/cli.tfrc /app/tf/cli.tfrc

# Ownership pass AFTER every COPY above lands — doing this here (instead
# of pre-creating /app/infrastructure in the earlier RUN block) avoids
# clobbering the COPYed tree with an empty placeholder dir, and ensures
# the non-root runtime uid can read the baked modules + plugin mirror.
RUN chown -R appuser:appuser /app/infrastructure /opt/tf-plugin-mirror /app/tf

USER 65532:65532
EXPOSE 8011 8012

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8011/health')"]

CMD ["uvicorn", "services.terraformer.src.main:app", "--host", "0.0.0.0", "--port", "8011"]
