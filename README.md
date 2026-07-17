# pneuma-terraformer

Pneuma's Terraform runner service — declarative tenant-resource lifecycle.

**Capability surface**: tenant-tier `provisioning.apply_tenant_resources` /
`provisioning.destroy_tenant_resources` / `provisioning.read_tenant_state`
(dispatched by the `core:tenant_apply_resources` / `core:tenant_destroy_resources`
cycles, gRPC `ProvisioningService`) plus platform-tier
`provisioning.apply_platform_secrets` / `provisioning.apply_platform_bus_topology`
(gRPC `PlatformProvisioningService`, a separate service — see
`docs/standards/declarative-infra-via-terraform.md` §2.0 "never mesh
tenant and platform tiers"). Dispatched by `core:platform_apply_secret_reconcile` /
`core:platform_apply_bus_topology` in `pneuma-engine` — both cycles stay
`status: "draft"` until their respective activation gates (P7.3) flip
them. `ApplyPlatformBusTopology`'s handler is implemented and tested;
`ApplyPlatformSecrets`' gRPC handler is NOT yet built (only proto- and
capability-registered) — it currently answers UNIMPLEMENTED, still
reachable only via the pre-existing `POST /provisioning/reconcile-platform-secrets`
HTTP route.

**Image**: `ghcr.io/deanmak13/pneuma-terraformer`
**Port**: 8011 (HTTP — health + the `apply_platform_secrets` route) /
8012 (gRPC — 3 tenant-tier `ProvisioningService` RPCs +
2 platform-tier `PlatformProvisioningService` RPCs)
**Consumed by**: `pneuma-helm-charts/charts/pneuma-terraformer/`
**Deployed via**: `pneuma-deployments/platform/overlays/<env>/pneuma-terraformer/`

---

## Repo state (2026-07-11)

**Buildable — P3 shipped.** The image bakes a pinned `pneuma-deployments`
Terraform module tree (`infrastructure/terraform/modules` +
`infrastructure/terraform/standalone`, at the `DEPLOYMENTS_REF` build-arg
/ `org.pneuma.deployments-ref` image label) plus a filesystem Terraform
provider plugin mirror (`terraform providers mirror`, baked at
`/opt/tf-plugin-mirror` and wired via `tf/cli.tfrc` +
`TF_CLI_CONFIG_FILE`) — `terraform init` never reaches the public
registry or a runtime mount/sidecar at apply time. State is
S3-compatible (MinIO), reconfigured per-workspace via `-backend-config`
using the verified TF 1.9 key set (`endpoints.s3` / `use_path_style` /
`skip_requesting_account_id` — the legacy `endpoint` /
`force_path_style` keys are deprecated as of the AWS-SDK-v2-backed S3
backend). Provider credentials (Postgres / RabbitMQ / MinIO / Vault /
Kubernetes) reach the `terraform` subprocess as environment variables
only — never argv, never written to a var file — via
`TerraformRunner._provider_env()`.

`pyproject.toml` still installs `pneuma-proto` from the pinned GHCR
wheel-carrier image (see the Dockerfile's `proto` stage) rather than
PyPI — the wheel is not published there. `services.common.db.client` /
`services.common.proto_capability_sync` imports remain unsatisfied;
`capability_sync.py` uses a typed-but-raw-`httpx` PostgREST client
(`CapabilityRegistry`) as a stopgap, noted inline at every call site.
**P4 owns replacing that stopgap with the published `pneuma-common`
typed ORM** — see
`pneuma/docs/plans/2026-07-11-terraformer-onboarding-provisioning.md`
§4 P4. Do not hand-roll additional raw-httpx call sites here before
that lands; extend `CapabilityRegistry` instead.

### Local build

The Dockerfile's build context needs a sparse `pneuma-deployments`
checkout at `_deployments/` (gitignored — build-context-only, never
committed):

```sh
git clone --depth 1 --filter=blob:none --sparse \
  https://github.com/deanmak13/pneuma-deployments _deployments
(cd _deployments && git sparse-checkout set \
  infrastructure/terraform/modules infrastructure/terraform/standalone)
DEPLOYMENTS_REF=$(git -C _deployments rev-parse HEAD)

docker buildx build \
  --build-arg DEPLOYMENTS_REF="$DEPLOYMENTS_REF" \
  -t terraformer:local --load .
```

CI (`build-and-publish.yml`) performs the equivalent checkout via a
second `actions/checkout` against a read-only PAT (`DEPLOYMENTS_RO_TOKEN`
— human gate, see the plan) pinned to `env.DEPLOYMENTS_REF` (overridable
per-run via `workflow_dispatch(deployments_ref)`).

The plan: `pneuma/docs/plans/2026-07-11-terraformer-onboarding-provisioning.md`.

---

## License

Apache-2.0 (matching the parent Pneuma project).
