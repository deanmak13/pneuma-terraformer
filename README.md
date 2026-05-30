# pneuma-terraformer

Pneuma's Terraform runner service — declarative tenant-resource lifecycle.

**Capability surface**: `provisioning.apply_tenant_resources` /
`provisioning.destroy_tenant_resources` / `provisioning.read_tenant_state`.
Dispatched by the `core:tenant_apply_resources` / `core:tenant_destroy_resources`
cycles in `pneuma-engine`.

**Image**: `ghcr.io/deanmak13/pneuma-terraformer`
**Port**: 8011 (gRPC + FastAPI)
**Consumed by**: `pneuma-helm-charts/charts/pneuma-terraformer/`
**Deployed via**: `pneuma-deployments/platform/overlays/<env>/pneuma-terraformer/`

---

## Repo state (2026-05-31)

**Bootstrap commit only.** This repo was just created via the
`pneuma-terraformer` relocation plan (`pneuma#301`) — Dean approved
moving the service out of `pneuma-engine`'s `services/terraformer/`
into its own top-level repo, mirroring the `pneuma-mem0` pattern.

The initial commit contains a **verbatim copy** of
`pneuma-engine/services/terraformer/`. The `src/` Python still imports
`services.common.db.client` and `services.common.proto_capability_sync`
and the typed proto stubs from `pneuma_proto` — those are not yet
satisfied by this repo's `pyproject.toml`.

### Outstanding work (follow-up session)

1. **`pyproject.toml`** — pin `pneuma-proto` and add a dep on
   `pneuma-common` (the shared library being extracted under
   `Onboard-08 PR1`, task #183). Until that ships, vendor the
   imported modules (`services.common.db.client`,
   `services.common.proto_capability_sync`) inline as a stop-gap.
2. **`.github/workflows/build-and-publish.yml`** — Docker image build
   on `push` to `main`, tag `sha-<short>` + `:latest`, publish to
   `ghcr.io/deanmak13/pneuma-terraformer`.
3. **`.github/workflows/pr-checks.yml`** — `pytest tests/` + `ruff` +
   `mypy` on every PR.
4. **Dockerfile path rewrite** — current Dockerfile uses
   `services/common/` / `services/terraformer/src/` paths that mirror
   the engine repo layout. Either restructure the repo to mirror them
   (keep `services/terraformer/`) or rewrite the COPY paths to
   `pneuma_terraformer/`.
5. **Engine PR** — `pneuma-engine` deletes
   `services/terraformer/` + removes any `pyproject.toml` testpaths
   entry referencing it.
6. **Deployments PR** — `pneuma-deployments` bumps
   `platform/overlays/<env>/pneuma-terraformer/values.yaml` image tag
   to the first SHA the new CI publishes.

The plan: `pneuma/docs/plans/2026-05-30-pneuma-terraformer-relocation.md`.

---

## License

Apache-2.0 (matching the parent Pneuma project).
