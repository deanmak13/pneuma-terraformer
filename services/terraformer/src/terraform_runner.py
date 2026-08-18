"""Async subprocess wrapper around the `terraform` CLI.

One workspace per tenant_id, isolated under settings.terraform_workdir_root.
The actual TF module ships in pneuma-deployments/infrastructure/terraform/modules/tenant/
and is baked into the runtime image at /app/infrastructure/terraform/modules/tenant/.

State backend: S3-compatible (MinIO inside the cluster). Workspace key is
`tenants/<tenant_id>.tfstate`. Backend config is rendered to a partial file
on every init so per-tenant access keys can be rotated without rebuilding
the workspace dir.

Concurrency: per-tenant asyncio.Lock prevents overlapping apply/destroy on
the same workspace. The lock is process-local — multiple replicas WOULD
race, so the chart pins replicas: 1 for terraformer.

Security note: all terraform invocations use asyncio.create_subprocess_exec
with an explicit argv list (no shell, no string interpolation into a command
line). Inputs are bound through -var-file (JSON, written by us) and -backend-config
key=value pairs (we control the keys; values come from typed Settings).

LAW alignment:
  - declarative-infra-via-terraform.md §2.0 (tenant tier — cycle dispatches here)
  - tenant-provisioning-via-terraform-cycle.md (the cycle calls this service)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shlex
import shutil
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from services.terraformer.src.settings import Settings, get_settings

_LOG = logging.getLogger("terraformer.terraform")

_TENANT_MODULE = "tenant"

# Floor on the run time left after the queue wait, below which _spawn
# refuses to start the subprocess at all (see _effective_run_timeout).
# The caller's Temporal step timeout covers the WHOLE RPC — queue wait
# included, deliberately aligned in pneuma-engine's onboarding_cycles.py
# — so a run that spent nearly its whole budget queuing must not then
# spend its last second starting `terraform apply` against real tfstate:
# that does partial, stateful work and then gets killed anyway, which is
# strictly worse than failing fast with nothing started. 1s is a
# deliberately small floor — this guards the degenerate "queue wait ate
# (almost) the whole budget" case, not a general minimum operation
# timeout, so it only trips when there is truly no meaningful time left.
_MIN_RUN_SECONDS_AFTER_QUEUE = 1

# ---------------------------------------------------------------------------
# Transient infrastructure-provider conflict registry — LAW: design for N,
# never for 1 (~/.claude/docs/design-for-n-not-1.md). `max_concurrent_
# terraform_runs` stays > 1 on purpose (tenant signup concurrency is a
# scaling requirement, not a bug to fix by serializing), which means two
# concurrent `terraform apply` runs legitimately contend on infrastructure
# that is SHARED across tenants even though each tenant owns distinct
# rows/schemas. Postgres's catalog tables (pg_authid, pg_shdepend, ...) are
# the first instance of this, live 2026-07-29 against tenant b6c10c08:
#
#   Error: could not execute revoke query: pq: tuple concurrently updated (XX000)
#     with postgresql_grant.tenant_admin_schema_all,
#     on postgres.tf line 49, in resource "postgresql_grant" "tenant_admin_schema_all"
#
# Each row below names ONE provider's transient-conflict fingerprint by its
# SQLSTATE (or vendor equivalent) plus the condition's human name, matched
# case-insensitively against a failed run's combined stdout+stderr. A 4th
# signature — another Postgres SQLSTATE, or a RabbitMQ/MinIO/OpenBao
# equivalent raised by a different provider block — is a NEW ROW here,
# never an `if "..." in err:` branch at the `_spawn` call site: `_spawn`
# treats every row identically.
@dataclass(frozen=True)
class _TransientConflictSignature:
    name: str
    # Case-insensitive substrings; ANY match is a hit. Each row carries
    # both the raw SQLSTATE/vendor code and the condition's human name so
    # a driver/provider that surfaces only one of the two (bare message
    # text vs. a code appended in parens, as in the live example above) is
    # still recognised.
    patterns: tuple[str, ...]


_TRANSIENT_CONFLICT_SIGNATURES: tuple[_TransientConflictSignature, ...] = (
    # Postgres SQLSTATE XX000 (internal_error) — this specific message is
    # MVCC contention on the *shared* catalogs (pg_authid/pg_shdepend) that
    # every CREATE ROLE / GRANT / REVOKE touches, even though each
    # tenant's own role and schema are distinct. This is the live
    # 2026-07-29 error quoted above.
    _TransientConflictSignature(
        name="postgres_tuple_concurrently_updated",
        patterns=("XX000", "tuple concurrently updated"),
    ),
    # Postgres SQLSTATE 40001 (serialization_failure) — a concurrent
    # transaction's write won the race under REPEATABLE READ/SERIALIZABLE;
    # Postgres's own documented recovery is to retry the transaction.
    _TransientConflictSignature(
        name="postgres_serialization_failure",
        patterns=("40001", "serialization_failure"),
    ),
    # Postgres SQLSTATE 40P01 (deadlock_detected) — two transactions (e.g.
    # two tenants' concurrent GRANT/REVOKE lock ordering) waited on each
    # other; Postgres kills one, safe to retry the whole apply.
    _TransientConflictSignature(
        name="postgres_deadlock_detected",
        patterns=("40P01", "deadlock_detected"),
    ),
)


def _match_transient_conflict(stdout: str, stderr: str) -> _TransientConflictSignature | None:
    """Return the first registry row whose pattern appears (case-
    insensitively) in the combined stdout+stderr of a failed run, or None
    if nothing matches. None is what tells `_spawn` a failure is NOT safe
    to blindly retry — a genuine HCL/schema error, a bad credential, a
    real provider rejection, etc. must never be retried."""
    combined = f"{stdout}\n{stderr}".lower()
    for signature in _TRANSIENT_CONFLICT_SIGNATURES:
        if any(pattern.lower() in combined for pattern in signature.patterns):
            return signature
    return None


# Bounded attempts for a `_spawn` dispatch that keeps failing with a
# matched transient-conflict signature — small and fixed, not a Settings
# field: terraform apply/init/destroy are idempotent so re-running is
# safe, but nothing about a deployment's environment should change how
# many times we blindly retry the SAME queued request before surfacing
# the failure.
_MAX_TRANSIENT_CONFLICT_ATTEMPTS = 3

# Backoff before each transient-conflict retry, doubling per retry (1s
# then 2s, for the 3-attempt cap above) — these are catalog-level lock-
# contention windows measured in milliseconds to low seconds, not a
# rate-limited external API, so a longer backoff would only eat further
# into the caller's already-shared timeout budget for no benefit.
_TRANSIENT_CONFLICT_BACKOFF_BASE_SECONDS = 1.0

# Kubernetes ServiceAccount projection paths — module-level constants
# (not Settings fields) so tests can monkeypatch them directly onto this
# module without threading a new Settings field through every call site.
# Both must exist for _provider_env() to populate KUBE_*: a pod running
# without a mounted SA token has no business reconciling k8s-backed
# tenant resources (ESO SecretStore bindings, RMQ Operator CRDs, ...).
# ALSO imported by kube_lease_mutex.py — the reconcile-on-change
# automation's Lease-based single-flight mutex authenticates to the
# Kubernetes API the same way, so these two paths are this pod's one
# source of truth for "how do I prove I'm this ServiceAccount", not
# duplicated per consumer.
_KUBE_SA_TOKEN_PATH = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")
_KUBE_SA_CA_CERT_PATH = Path("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")

# The pneuma-deployments tenant module's `profile` variable (infrastructure/
# terraform/modules/tenant/variables.tf) models "non-regulated" as `null` —
# `default = null`, validated by `var.profile == null || contains(["gdpr-
# special-uk", "fca-uk"], var.profile)`. There is no "standard" tier value
# in that contract (confirmed by the matching `control.tenants.
# compliance_profile` CHECK constraint in pneuma-engine, which likewise
# only allows NULL / 'gdpr-special-uk' / 'fca-uk'). Any falsy value or the
# legacy "standard" sentinel therefore normalizes to None here — the one
# seam every caller's TenantInputs crosses on the way into the var-file —
# so a caller or default that reintroduces the literal string can't break
# `terraform apply` again.
_NON_REGULATED_SENTINELS = frozenset({"", "standard"})


def _normalize_profile(profile: str | None) -> str | None:
    if profile is None:
        return None
    return None if profile.strip().lower() in _NON_REGULATED_SENTINELS else profile


# ---------------------------------------------------------------------------
# Re-apply-drift import registry — LAW: design for N, never for 1. A
# partially-applied tenant (apply killed mid-run, or the caller retried
# after a failure) can leave a resource CREATED against the real provider
# but absent from Terraform state (state write happens only after a
# resource's create call returns). The next apply then tries to create it
# again and the provider fatally rejects the duplicate — live 2026-08-15
# against tenant 831acdc5:
#
#   [FATAL] bucket already exists! (journey-co-w31786796546-...-media):
#     with minio_s3_bucket.tenant_media
#
# Each row below names ONE resource address in the tenant module (
# pneuma-deployments infrastructure/terraform/modules/tenant/) that is
# known to commonly pre-exist this way, plus how to compute that
# resource's real-world ID from TenantInputs. `_import_preexisting_
# resources` treats every row identically — a second resource (e.g. the
# RabbitMQ vhost, or a Postgres role) that starts exhibiting the same
# failure mode is a NEW ROW here, never a bespoke import call at the
# apply call site.
@dataclass(frozen=True)
class _ImportOnExistsResource:
    name: str
    # Terraform resource address exactly as declared in the tenant module.
    resource_address: str
    # Computes the provider-native ID `terraform import` expects for this
    # resource address, from TenantInputs. Must match the module's own
    # naming convention exactly (see minio.tf's `bucket_prefix` local /
    # README.md "Bucket convention").
    resource_id: Callable[["TenantInputs"], str]


def _tenant_media_bucket_id(inputs: "TenantInputs") -> str:
    # Mirrors minio.tf's `local.bucket_prefix = coalesce(var.minio_bucket_
    # prefix, var.tenant_slug)` — the runner never sets minio_bucket_prefix
    # (see _tf_vars), so var.minio_bucket_prefix is always its HCL default
    # ("") and the coalesce always resolves to tenant_slug. If a future
    # change threads a prefix override through TenantInputs, this must be
    # updated in lockstep or the import ID will silently stop matching the
    # real bucket name.
    return f"{inputs.tenant_slug}-{inputs.env}-media"


def _tenant_reader_sa_id(inputs: "TenantInputs") -> str:
    # terraform-provider-kubernetes import ID convention for namespaced
    # resources: "<namespace>/<name>" (see hashicorp/terraform-provider-
    # kubernetes docs, `terraform import kubernetes_service_account.x
    # <namespace>/<name>`). Mirrors eso.tf's
    # `kubernetes_service_account.tenant_reader` metadata block exactly —
    # name = "tenant-${var.tenant_slug}-reader", namespace = var.pooled_
    # namespace (the runner's own `pooled_namespace` field, unconditionally
    # set from settings.pneuma_namespace in _tenant_inputs).
    return f"{inputs.pooled_namespace}/tenant-{inputs.tenant_slug}-reader"


def _tenant_app_role_id(inputs: "TenantInputs") -> str:
    # cyrilgdn/postgresql provider: a postgresql_role's import ID IS the
    # role name (`terraform import postgresql_role.x <rolename>`) — no
    # namespace/composite prefix. Mirrors postgres.tf's
    # `postgresql_role.tenant_app.name = "tenant_${var.tenant_slug}_app"`.
    return f"tenant_{inputs.tenant_slug}_app"


def _tenant_admin_role_id(inputs: "TenantInputs") -> str:
    # Same convention as _tenant_app_role_id — mirrors postgres.tf's
    # `postgresql_role.tenant_admin.name = "tenant_${var.tenant_slug}_admin"`.
    return f"tenant_{inputs.tenant_slug}_admin"


def _tenant_vhost_id(inputs: "TenantInputs") -> str:
    # cyrilgdn/rabbitmq provider: a rabbitmq_vhost's import ID IS the vhost
    # name, leading slash included (`terraform import rabbitmq_vhost.x
    # "/some-vhost"`). Mirrors rabbitmq.tf's `rabbitmq_vhost.tenant.name =
    # "/${var.tenant_slug}-${var.env}"`. Not currently observed to fail
    # live (RabbitMQ's vhost PUT is idempotent) — registered defensively
    # per the "if importable" ask, since a future provider/version could
    # tighten that.
    return f"/{inputs.tenant_slug}-{inputs.env}"


def _tenant_rmq_user_id(inputs: "TenantInputs") -> str:
    # cyrilgdn/rabbitmq provider: a rabbitmq_user's import ID IS the
    # username (`terraform import rabbitmq_user.x someuser`). Mirrors
    # rabbitmq.tf's `rabbitmq_user.tenant.name = "tenant_${var.tenant_slug}"`.
    # Same defensive registration rationale as _tenant_vhost_id.
    return f"tenant_{inputs.tenant_slug}"


# Deliberately NOT registered: minio_iam_policy.tenant_bucket_rw,
# minio_iam_user_policy_attachment.tenant, vault_kubernetes_auth_backend_
# role.tenant, postgresql_grant.* , vault_kv_secret_v2.* — every one of
# these is backed by a provider-side UPSERT (MinIO policy PUT, Vault KV
# write, Postgres GRANT, Vault auth-role write), so re-applying over a
# pre-existing one overwrites cleanly instead of erroring "already
# exists". Only CREATE-ONLY provider calls (S3 bucket PutBucket, k8s
# ServiceAccount POST, Postgres CREATE ROLE) belong in this registry — an
# UPSERT resource added here would be harmless but pointless churn on
# every apply (an unnecessary import attempt), so it's excluded, not
# merely unconfirmed.
_IMPORT_ON_EXISTS_RESOURCES: tuple[_ImportOnExistsResource, ...] = (
    _ImportOnExistsResource(
        name="tenant_media_bucket",
        resource_address="minio_s3_bucket.tenant_media",
        resource_id=_tenant_media_bucket_id,
    ),
    _ImportOnExistsResource(
        name="tenant_reader_service_account",
        resource_address="kubernetes_service_account.tenant_reader",
        resource_id=_tenant_reader_sa_id,
    ),
    _ImportOnExistsResource(
        name="tenant_app_role",
        resource_address="postgresql_role.tenant_app",
        resource_id=_tenant_app_role_id,
    ),
    _ImportOnExistsResource(
        name="tenant_admin_role",
        resource_address="postgresql_role.tenant_admin",
        resource_id=_tenant_admin_role_id,
    ),
    _ImportOnExistsResource(
        name="tenant_rmq_vhost",
        resource_address="rabbitmq_vhost.tenant",
        resource_id=_tenant_vhost_id,
    ),
    _ImportOnExistsResource(
        name="tenant_rmq_user",
        resource_address="rabbitmq_user.tenant",
        resource_id=_tenant_rmq_user_id,
    ),
)


@dataclass(frozen=True)
class TenantInputs:
    tenant_id: str
    tenant_slug: str
    env: str
    compliance_profile: str | None
    pooled_namespace: str


@dataclass(frozen=True)
class PlatformSecretsInputs:
    """Inputs to the platform-secrets reconcile harness.

    Single env-scoped workspace per cluster — no per-tenant axis. The
    platform-secrets module fans canonical OpenBao paths out into
    per-service paths, all cluster-shared. Workspace key:
    `platform-secrets/<env>.tfstate`.
    """

    env: str  # dev | tst | prod


@dataclass(frozen=True)
class PlatformResourcesInputs:
    """Inputs to the platform-resources reconcile harness (inter-service
    HMAC pairs + the ActivePieces least-privilege Postgres role).

    Single env-scoped workspace per cluster, mirrors PlatformSecretsInputs.
    Workspace key: `platform-resources/<env>.tfstate`.
    """

    env: str  # dev | tst | prod


@dataclass(frozen=True)
class PlatformBusTopologyInputs:
    """Inputs to the platform-bus-topology reconcile harness.

    Single env-scoped workspace per cluster — no per-tenant axis, mirrors
    PlatformSecretsInputs above. Workspace key: `platform/bus-topology/<env>.tfstate`
    (see pneuma-deployments' modules/platform-bus-topology/README.md
    "State-key convention" — deliberately disjoint from both
    `platform-secrets/<env>.tfstate` and every per-tenant
    `tenants/<tenant_id>.tfstate` key).
    """

    env: str  # dev | tst | prod


@dataclass(frozen=True)
class TerraformResult:
    exit_code: int
    stdout: str
    stderr: str
    outputs: dict[str, Any]


# Number of trailing lines kept in the gRPC-propagated error string and in
# the on-failure log line — enough to show the actual terraform provider
# error (which is often several lines: resource address, error message,
# provider diagnostic detail) without dumping an entire multi-hundred-line
# apply transcript into provisioning_runs.error_detail or the pod log.
_ERROR_TAIL_LINES = 50


def _tail(text: str, n: int = _ERROR_TAIL_LINES) -> str:
    if not text:
        return ""
    return "\n".join(text.strip().splitlines()[-n:])


# Best-effort scrub for secret-SHAPED substrings in raw terraform
# stdout/stderr before it ever reaches a log line or the gRPC error detail
# surfaced to provisioning_runs.error_detail. This is defence-in-depth on
# top of scrub_credentials() (which redacts KNOWN credential VALUES from
# Settings) — that function requires the caller to hand it a Settings
# instance and only catches values this process itself holds. A provider
# error can also echo back a value we never held explicitly (e.g. a
# generated password embedded in a resource attribute dump), so this also
# blanket-redacts any token that LOOKS like a bearer/API key/password
# assignment, independent of whether we know the value.
_SECRET_SHAPED_RE = re.compile(
    r"(?i)\b((?:api[_-]?key|token|password|secret|access[_-]?key)\s*[=:]\s*)"
    r"(\"?[A-Za-z0-9+/_.\-]{8,}\"?)"
)


def _scrub_secret_shaped(text: str) -> str:
    if not text:
        return text
    return _SECRET_SHAPED_RE.sub(lambda m: f"{m.group(1)}<REDACTED>", text)


# Defect (2026-08-15 TST outage, tenant 831acdc5 — follow-up sweep):
# `_LOG.warning("... tail=\n%s", ..., tail)` puts the tail on lines AFTER
# the "tail=" prefix. `logging.StreamHandler` writes that whole formatted
# message — embedded newlines included — in ONE write() call, but the
# container runtime's log driver splits stdout on every newline into
# SEPARATE log records. So a line-oriented view (`kubectl logs | grep
# "tf spawn FAILED"`) only ever surfaces the FIRST line, which ends at
# "tail=" with nothing after it — the tail content is really there, just
# on unattributed, unsearched-for follow-up log lines. Evidence: the
# live 17:46 run's `last_apply.log` (a plain multi-line FILE, not
# constrained to one log record) had the full stderr the whole time.
# Flattening to one line before logging (last_apply.log stays multi-line
# — it's a file, not a log record) makes the tail actually greppable.
def _flatten_for_log(text: str) -> str:
    if not text:
        return text
    return " | ".join(line for line in text.splitlines() if line.strip())


class TerraformError(RuntimeError):
    def __init__(self, command: str, result: TerraformResult):
        self.command = command
        self.result = result
        # Prefer stderr (where terraform puts provider errors under
        # -no-color); fall back to stdout for the rarer case where the
        # failure is reported there instead (e.g. some plan-time errors).
        snippet = _tail(result.stderr) or _tail(result.stdout)
        snippet = _scrub_secret_shaped(snippet)
        super().__init__(f"terraform {command} failed (exit={result.exit_code}): {snippet}")


_SECRET_FIELDS = (
    "hetzner_api_token",
    "cloudflare_api_token",
    "postgres_superuser_password",
    "rabbitmq_admin_password",
    "minio_admin_password",
    "tf_state_backend_access_key",
    "tf_state_backend_secret_key",
    "admin_api_key",
)


def scrub_credentials(s: str, settings: Settings | None = None) -> str:
    """Replace any occurrence of a known credential VALUE in ``s`` with
    ``<REDACTED>``. Used at the HTTP boundary in routes/provisioning.py
    to defang TF provider errors that may include partial backend-config
    or env-var dumps. Idempotent + safe on already-scrubbed strings.

    Settings is resolved lazily so unit tests can scrub without booting
    the full settings cache."""
    if not s:
        return s
    cfg = settings or get_settings()
    for name in _SECRET_FIELDS:
        value = getattr(cfg, name, None)
        if value and isinstance(value, str) and len(value) >= 8:
            s = s.replace(value, "<REDACTED>")
    return s


class TerraformRunner:
    def __init__(self, settings: Settings | None = None):
        self._settings = settings or get_settings()
        self._locks: dict[str, asyncio.Lock] = {}
        # Lazily created (see _spawn_semaphore): an instance constructed
        # outside a running event loop must not bind loop state at
        # construction time.
        self._sem: asyncio.Semaphore | None = None
        # Separate, wider semaphore for read-only, no-provider-plugin
        # spawns (`terraform state list` with no address — see
        # _read_spawn_semaphore). Sharing the heavy apply/import semaphore
        # here is what caused the 2026-08-15 19:01-19:11 stall: 4 tenants'
        # cheap state-list probes queued behind the SAME limit=2 slots as
        # every apply, burning the whole ~590s RunTenantReconcile budget
        # on queueing before apply ever started.
        self._read_sem: asyncio.Semaphore | None = None

    def _lock_for(self, tenant_id: str) -> asyncio.Lock:
        if tenant_id not in self._locks:
            self._locks[tenant_id] = asyncio.Lock()
        return self._locks[tenant_id]

    def _spawn_semaphore(self) -> asyncio.Semaphore:
        """Process-wide cap on concurrent terraform subprocesses (get_runner()
        is a cached module-level singleton, so this one instance-level
        semaphore bounds every init/apply/destroy/plan the process runs).
        Each terraform run loads ~5 provider plugins as separate child
        processes — unbounded spawning OOMKilled the pod (2026-07-27): six
        concurrent applies within 13s against a 1Gi limit. Created lazily
        so constructing a TerraformRunner outside a running event loop
        doesn't bind loop state prematurely."""
        if self._sem is None:
            self._sem = asyncio.Semaphore(self._settings.max_concurrent_terraform_runs)
        return self._sem

    # Fixed multiplier, not a Settings field: read-only spawns
    # (`terraform state list`, no address) never load provider plugins —
    # they only read the state file already fetched by init — so the
    # OOMKill hazard the heavy semaphore guards against (_spawn_semaphore's
    # docstring: ~5 provider-plugin child processes per heavy spawn) does
    # not apply. 8x is generous headroom without being unbounded (an
    # unbounded read semaphore would remove the queue-timeout safety net
    # entirely for a busy pod).
    _READ_SEMAPHORE_MULTIPLIER = 8

    def _read_spawn_semaphore(self) -> asyncio.Semaphore:
        """Wider semaphore for cheap, read-only, no-provider-plugin spawns
        — currently only the bulk `terraform state list` probe in
        `_import_preexisting_resources`. See the 2026-08-15 19:01-19:11
        incident note on `self._read_sem` for why this must NOT share
        `_spawn_semaphore`'s limit=2: that stall was 4 tenants' cheap
        state-list probes queued behind the same slots as every heavy
        apply/import, burning the whole RunTenantReconcile budget on
        queueing before any apply started."""
        if self._read_sem is None:
            self._read_sem = asyncio.Semaphore(
                self._settings.max_concurrent_terraform_runs * self._READ_SEMAPHORE_MULTIPLIER
            )
        return self._read_sem

    def _spawn_queue_budget(self, timeout: float) -> int:
        """Seconds a given `_spawn` call may wait to ACQUIRE a concurrency
        slot before giving up with exit_code=124 — a fraction of that
        call's OWN `timeout`, never a flat constant (see settings.py's
        `spawn_queue_timeout_fraction` docstring for why a flat value
        can't simultaneously suit a 30s read and a 600s apply). Floored
        at 1s so a very short-timeout op still gets one real queue
        attempt instead of a 0s budget that fails before ever trying.
        `timeout` is a float (not just int) because a transient-conflict
        retry (see `_spawn`) passes the BUDGET REMAINING after prior
        attempts, which is a subtraction of monotonic-clock reads."""
        return max(1, int(timeout * self._settings.spawn_queue_timeout_fraction))

    def _effective_run_timeout(self, timeout: float, queued_elapsed: float) -> float:
        """Run-time budget remaining AFTER the queue wait is deducted from
        the operation's own `timeout` — NOT the full `timeout` again.

        The caller's Temporal step timeout covers the whole RPC, queue
        wait included (onboarding_cycles.py deliberately aligns the step
        timeout with the `timeout` value passed to us): if the run
        budget were the full `timeout` on top of however long we already
        queued, `queue_wait + run` could exceed what the caller is
        actually willing to wait, and Temporal would kill the activity
        mid-`apply` — burning a retry AND leaving partial infrastructure
        work to reconcile. Deducting here keeps `queue_wait + run <=
        timeout` always, by construction."""
        return timeout - queued_elapsed

    def _workspace_dir(self, tenant_id: str) -> Path:
        """Resolve the per-tenant workspace path AND assert it is
        contained within ``terraform_workdir_root``. Defence-in-depth
        on top of the route-level ``pattern`` validator: even if a
        future code path bypasses the Pydantic guard, a path-traversal
        ``tenant_id`` is still caught here. Raises ValueError on
        traversal."""
        root = self._settings.terraform_workdir_root.resolve()
        candidate = (root / tenant_id).resolve()
        if not str(candidate).startswith(str(root) + "/") and candidate != root:
            raise ValueError(
                f"tenant_id {tenant_id!r} escapes workdir root {root}"
            )
        return candidate

    def _module_source(self) -> Path:
        return self._settings.terraform_modules_root / _TENANT_MODULE

    def _backend_config(self, tenant_id: str) -> dict[str, str]:
        # ONLY flat primitive (string/bool) attributes belong here —
        # Terraform treats CLI `-backend-config k=v` VALUES as literal
        # strings (cty.StringVal), never HCL, so an object-typed
        # attribute like the S3 backend's nested `endpoints` CANNOT be
        # set via CLI at all (hashicorp/terraform#34616, #36911): an
        # inline `{s3="..."}` literal fails type conversion at init.
        # The S3 endpoint therefore travels via AWS_ENDPOINT_URL_S3 on
        # the subprocess env (see _spawn), which the AWS-SDK-v2-backed
        # S3 backend (TF >= 1.6) reads as `endpoints.s3`. Credentials
        # likewise stay env-only (AWS_ACCESS_KEY_ID /
        # AWS_SECRET_ACCESS_KEY in _spawn) — never argv, where they'd
        # be world-readable in /proc/<pid>/cmdline.
        #
        # Key semantics verified locally against hashicorp/terraform:1.9:
        # the legacy `force_path_style` argument is deprecated in favour
        # of `use_path_style`, and `skip_requesting_account_id=true` is
        # required against a non-AWS S3-compatible endpoint (MinIO) —
        # without it the backend's AWS account-ID lookup 403s and init
        # fails outright even with skip_credentials_validation=true.
        s = self._settings
        return {
            "bucket": s.tf_state_backend_bucket,
            "key": f"tenants/{tenant_id}.tfstate",
            "region": s.tf_state_backend_region,
            "use_path_style": "true",
            "skip_credentials_validation": "true",
            "skip_region_validation": "true",
            "skip_metadata_api_check": "true",
            "skip_requesting_account_id": "true",
        }

    def _tf_vars(self, inputs: TenantInputs) -> dict[str, str | None]:
        s = self._settings
        vars_: dict[str, str | None] = {
            "tenant_id": inputs.tenant_id,
            "tenant_slug": inputs.tenant_slug,
            "env": inputs.env,
            "profile": _normalize_profile(inputs.compliance_profile),
            "pooled_namespace": inputs.pooled_namespace,
            "postgres_superuser_password": s.postgres_superuser_password,
            "rabbitmq_admin_password": s.rabbitmq_admin_password,
            "minio_admin_password": s.minio_admin_password,
        }
        if s.tenant_infra_provider == "hetzner":
            if not s.hetzner_api_token or not s.cloudflare_api_token:
                raise ValueError(
                    "TENANT_INFRA_PROVIDER=hetzner requires HETZNER_API_TOKEN "
                    "and CLOUDFLARE_API_TOKEN"
                )
            vars_["hetzner_api_token"] = s.hetzner_api_token
            vars_["cloudflare_api_token"] = s.cloudflare_api_token
        return vars_

    async def _spawn(
        self,
        workdir: Path,
        args: list[str],
        timeout: float,
        extra_env: dict[str, str] | None = None,
    ) -> TerraformResult:
        """Bounded-attempt wrapper around `_spawn_once` (below): retries a
        run that BOTH failed AND whose combined stdout+stderr matched a
        registered transient-conflict signature (see
        _TRANSIENT_CONFLICT_SIGNATURES) — e.g. two concurrent tenant
        applies' `CREATE ROLE` / `GRANT` contending on Postgres's shared
        catalogs (pg_authid/pg_shdepend), live 2026-07-29 against tenant
        b6c10c08. `terraform apply`/`init`/`destroy` are idempotent, so
        re-running is safe. A clean success, or a clean failure with no
        signature match (a genuine HCL/schema error, a bad credential, a
        real provider rejection, ...), returns on the FIRST attempt —
        only a matched, retryable failure ever loops.

        Stays INSIDE the caller's own `timeout` budget by construction:
        `timeout` here is the same "whole RPC" budget `_spawn_once`
        already treats as queue-wait + run (see _effective_run_timeout /
        _spawn_queue_budget). Each retry hands `_spawn_once` what's LEFT
        of that budget — elapsed time since entering `_spawn`, backoff
        sleeps included, subtracted from `timeout` — never the full
        `timeout` again. If the remaining budget can't fit the mandatory
        backoff PLUS `_spawn_once`'s own refuse-to-start floor
        (_MIN_RUN_SECONDS_AFTER_QUEUE), retrying stops and the original
        failure is returned: starting one more attempt with no real time
        left would just repeat the exact "partial work then killed mid-
        run" failure mode `_spawn_once` already guards against.
        """
        overall_start = time.monotonic()
        attempt = 1
        # The FIRST attempt always gets the caller's raw `timeout`
        # unchanged — never `timeout` minus this wrapper's own
        # negligible bookkeeping overhead — so the common, no-retry-
        # needed case hands `_spawn_once` bit-for-bit the same budget as
        # calling it directly. Only a RETRY needs its budget shrunk by
        # what earlier attempts (and their backoff) actually spent.
        timeout_for_attempt = timeout
        while True:
            result = await self._spawn_once(workdir, args, timeout_for_attempt, extra_env)
            if result.exit_code == 0:
                return result
            signature = _match_transient_conflict(result.stdout, result.stderr)
            if signature is None or attempt >= _MAX_TRANSIENT_CONFLICT_ATTEMPTS:
                return result

            backoff = _TRANSIENT_CONFLICT_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
            remaining = timeout - (time.monotonic() - overall_start)
            if remaining <= backoff + _MIN_RUN_SECONDS_AFTER_QUEUE:
                _LOG.warning(
                    "tf spawn NOT retrying transient conflict signature=%s "
                    "(attempt %d/%d): only %.1fs of the %.1fs budget remains, "
                    "below backoff+floor (%.1fs+%ds): workdir=%s cmd=%s",
                    signature.name, attempt, _MAX_TRANSIENT_CONFLICT_ATTEMPTS,
                    remaining, timeout, backoff, _MIN_RUN_SECONDS_AFTER_QUEUE,
                    workdir, args[0] if args else "<no-args>",
                )
                return result

            attempt += 1
            _LOG.info(
                "tf spawn retrying after transient conflict: signature=%s "
                "attempt=%d/%d workdir=%s cmd=%s",
                signature.name, attempt, _MAX_TRANSIENT_CONFLICT_ATTEMPTS,
                workdir, args[0] if args else "<no-args>",
            )
            await asyncio.sleep(backoff)
            timeout_for_attempt = timeout - (time.monotonic() - overall_start)

    async def _spawn_once(
        self,
        workdir: Path,
        args: list[str],
        timeout: float,
        extra_env: dict[str, str] | None = None,
        failure_expected: bool = False,
        light: bool = False,
    ) -> TerraformResult:
        # light: this call spawns no provider plugins (currently only the
        # bulk `terraform state list` probe) — queue on the wider
        # _read_spawn_semaphore instead of the heavy apply/import one. See
        # _read_spawn_semaphore's docstring.
        # failure_expected: a non-zero exit is a NORMAL outcome for this
        # call site (e.g. `_import_preexisting_resources` probing a
        # resource that turns out not to pre-exist), so it logs at DEBUG
        # instead of the WARNING "tf spawn FAILED" line below — that line
        # exists to flag a REAL apply/destroy failure, and firing it on
        # every fresh-tenant apply's expected "not found, will create"
        # import probes would bury the genuine signal in noise.
        cmd = [self._settings.terraform_binary, *args]
        # Redact any -backend-config=key=value pairs that carry secrets
        # before logging. Without this, every init logs the MinIO
        # secret_key inline at INFO level.
        safe_args: list[str] = []
        skip_next = False
        for arg in args:
            if skip_next:
                if "=" in arg:
                    k, _ = arg.split("=", 1)
                    if k in ("secret_key", "access_key"):
                        safe_args.append(f"{k}=<REDACTED>")
                    else:
                        safe_args.append(arg)
                else:
                    safe_args.append(arg)
                skip_next = False
                continue
            if arg == "-backend-config":
                skip_next = True
            safe_args.append(arg)
        safe_cmd = shlex.join([self._settings.terraform_binary, *safe_args])

        env = os.environ.copy()
        env["TF_IN_AUTOMATION"] = "1"
        env["TF_INPUT"] = "0"
        # Wires the baked filesystem provider mirror (Dockerfile `mirror`
        # stage + tf/cli.tfrc) — terraform init never reaches the public
        # registry.
        env["TF_CLI_CONFIG_FILE"] = self._settings.tf_cli_config_file
        env["AWS_ACCESS_KEY_ID"] = self._settings.tf_state_backend_access_key
        env["AWS_SECRET_ACCESS_KEY"] = self._settings.tf_state_backend_secret_key
        # The S3 backend's `endpoints.s3` attribute is object-typed and
        # cannot be set via CLI -backend-config (values are literal
        # strings, never HCL — hashicorp/terraform#34616, #36911). The
        # AWS-SDK-v2-backed backend (TF >= 1.6) documents this env var as
        # its equivalent source, so the MinIO endpoint travels env-only,
        # exactly like the credentials above.
        env["AWS_ENDPOINT_URL_S3"] = self._settings.tf_state_backend_endpoint
        # Provider-plugin credentials (Postgres/RMQ/MinIO/Vault/Kubernetes)
        # — env-only, never argv, so they never appear in a process
        # listing or the redacted argv log line above.
        env.update(self._provider_env())
        # Per-call overrides — used exactly once today: apply_platform_auth
        # injects a transient break-glass VAULT_TOKEN for its own single
        # apply, without ever folding that token into _provider_env()
        # (which every other terraform invocation on this runner shares).
        if extra_env:
            env.update(extra_env)

        # Bound concurrent subprocesses (see _spawn_semaphore) — held across
        # the FULL subprocess lifetime, including the timeout/kill branch
        # below. Releasing early would bound nothing: that's the whole
        # point of the fix.
        #
        # The acquire itself is ALSO bounded, via a budget SCALED to this
        # call's own `timeout` (see _spawn_queue_budget) — without that, a
        # read-only `terraform output -json` (own timeout 30s) could queue
        # behind concurrent 600s applies for ~20 minutes, while a flat
        # budget short enough to stop that would in turn abort a 600s
        # apply that merely queued behind another long-running apply. A
        # queue-timeout expiry never spawns a process, so there is
        # nothing to reap on that branch — we just return exit_code=124.
        #
        # We do NOT use `async with sem:` here: a bounded acquire needs a
        # manual try/except around `sem.acquire()` (to distinguish "never
        # acquired, nothing to release" from "acquired, must release"),
        # so the permit is released via an explicit `finally` below,
        # exactly once, only on the branch that actually acquired it.
        sem = self._read_spawn_semaphore() if light else self._spawn_semaphore()
        sem_limit = (
            self._settings.max_concurrent_terraform_runs * self._READ_SEMAPHORE_MULTIPLIER
            if light else self._settings.max_concurrent_terraform_runs
        )
        queue_budget = self._spawn_queue_budget(timeout)
        if sem.locked():
            _LOG.info(
                "tf spawn queued (waiting, limit=%d, light=%s): workdir=%s cmd=%s",
                sem_limit, light, workdir, safe_cmd,
            )
        _t0 = time.monotonic()
        try:
            await asyncio.wait_for(sem.acquire(), timeout=queue_budget)
        except asyncio.TimeoutError:
            _LOG.warning(
                "tf spawn queue timeout after %.1fs (budget=%ds, limit=%d, light=%s): "
                "workdir=%s cmd=%s",
                time.monotonic() - _t0, queue_budget, sem_limit, light, workdir, safe_cmd,
            )
            return TerraformResult(
                exit_code=124,
                stdout="",
                stderr=(
                    f"terraform {args[0]} queued too long behind concurrent "
                    f"terraform runs (waited >{queue_budget}s, limit="
                    f"{self._settings.max_concurrent_terraform_runs})"
                ),
                outputs={},
            )

        # Permit acquired from here on — every path below must release it
        # exactly once, including on cancellation (asyncio.CancelledError
        # is a BaseException, not caught by any `except` clause here, so
        # only a `finally` reliably runs on it).
        try:
            _queued_s = time.monotonic() - _t0
            if _queued_s > 1.0:
                _LOG.info(
                    "tf spawn queued %.1fs behind concurrency limit %d",
                    _queued_s, self._settings.max_concurrent_terraform_runs,
                )

            # The caller's Temporal step timeout covers the WHOLE RPC —
            # queue wait included (deliberately aligned with `timeout` in
            # pneuma-engine's onboarding_cycles.py) — so the run gets
            # what's LEFT of `timeout` after queuing, never `timeout`
            # again on top of it. If that leaves next to nothing, do not
            # start the subprocess at all: a long `apply` given only a
            # few seconds would do real, partial, stateful work against
            # the tfstate and then get killed mid-run anyway — strictly
            # worse than failing fast here with nothing started.
            remaining = self._effective_run_timeout(timeout, _queued_s)
            if remaining <= _MIN_RUN_SECONDS_AFTER_QUEUE:
                _LOG.warning(
                    "tf spawn refusing to start: queue wait %.1fs left only "
                    "%.1fs of the %ds budget (floor=%ds): workdir=%s cmd=%s",
                    _queued_s, remaining, timeout, _MIN_RUN_SECONDS_AFTER_QUEUE,
                    workdir, safe_cmd,
                )
                return TerraformResult(
                    exit_code=124,
                    stdout="",
                    stderr=(
                        f"terraform {args[0]} queue wait ({_queued_s:.1f}s) "
                        f"consumed the {timeout}s budget, leaving only "
                        f"{remaining:.1f}s to run — refusing to start rather "
                        f"than begin work that would be killed mid-run"
                    ),
                    outputs={},
                )

            # Logged AFTER acquisition (and the refuse-to-start check
            # above) so this line reliably means "a process actually
            # started" — moved here (from before the acquire) because the
            # log signature that identified the 2026-07-27 OOMKill
            # incident was six of these lines in 13s; logging before the
            # acquire would keep printing one line per dispatch even once
            # concurrency is bounded, misleading the next investigator
            # into thinking the bound isn't working.
            _LOG.info("tf spawn: workdir=%s cmd=%s", workdir, safe_cmd)

            proc = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=str(workdir),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
            )
            try:
                stdout_b, stderr_b = await asyncio.wait_for(proc.communicate(), timeout=remaining)
            except asyncio.TimeoutError:
                return TerraformResult(
                    exit_code=124,
                    stdout="",
                    stderr=(
                        f"terraform {args[0]} timed out after {remaining:.1f}s "
                        f"(of {timeout}s budget; {_queued_s:.1f}s spent queuing)"
                    ),
                    outputs={},
                )
            finally:
                # ALWAYS reap, including on cancellation: a cancelled
                # await (e.g. the gRPC caller's deadline expiring) raises
                # CancelledError here, which only `asyncio.TimeoutError`
                # above was catching pre-fix — leaving terraform and its
                # ~5 provider children alive and unreaped while the freed
                # permit let a queued run start, so live processes
                # silently exceeded the concurrency limit.
                if proc.returncode is None:
                    proc.kill()
                    await proc.wait()

            stdout_s = stdout_b.decode("utf-8", errors="replace")
            stderr_s = stderr_b.decode("utf-8", errors="replace")
            if proc.returncode:
                # Defect (2026-08 TST outage, tenant 831acdc5): a failed
                # apply previously logged ONLY the "tf spawn: ..." dispatch
                # line above — the actual terraform stdout/stderr was
                # discarded, leaving no trace anywhere of why an apply
                # failed repeatedly. Log the tail here so a failure is
                # diagnosable from pod logs alone, without needing to
                # reproduce the run. One line, flattened (see
                # _flatten_for_log) — an embedded-newline message gets
                # split across separate container-log records by the log
                # driver, so a line-oriented `grep "tf spawn FAILED"`
                # would see only the empty prefix up to "tail=".
                tail = _flatten_for_log(_scrub_secret_shaped(_tail(stderr_s) or _tail(stdout_s)))
                log = _LOG.debug if failure_expected else _LOG.warning
                log(
                    "tf spawn %s exit=%s workdir=%s cmd=%s tail=%s",
                    "failed (expected)" if failure_expected else "FAILED",
                    proc.returncode, workdir, safe_cmd, tail,
                )
            return TerraformResult(
                exit_code=proc.returncode or 0,
                stdout=stdout_s,
                stderr=stderr_s,
                outputs={},
            )
        finally:
            sem.release()

    def _vault_provider_hcl(self) -> str:
        """Render the generated `provider "vault" {}` block every
        workspace gets (see _write_vault_provider_file). Authenticates via
        OpenBao's Kubernetes auth method instead of a stored token: a
        static OPENBAO_ADMIN_TOKEN expired and 403'd `auth/token/lookup-
        self` on every tenant apply (2026-07 incident) — a stored,
        expirable credential is the failure class, not a particular
        expiry date. Exchanging the pod's own identity for a short-lived
        token on every run means there is nothing to go stale across a
        teardown/rebuild.

        Role and mount come from Settings (never a literal in this
        string) so a second cluster/role needs a config change, not a
        code change — the companion OpenBao config
        (vault_kubernetes_auth_backend_role.terraformer /
        vault_kubernetes_auth_backend.kubernetes) must name the same
        values.
        """
        s = self._settings
        # Already slash-normalised by Settings._normalise_auth_mount — the
        # single place that owns what a valid mount looks like.
        mount = s.vault_k8s_auth_mount
        return (
            '# GENERATED by terraform_runner._write_vault_provider_file — do not\n'
            '# hand-edit; rewritten into this workspace on every _ensure_workspace()\n'
            '# call. See terraform_runner.py:_vault_provider_hcl for the full\n'
            '# rationale (static-token expiry incident, 2026-07).\n'
            'provider "vault" {\n'
            '  # LOAD-BEARING — do NOT remove. Without this, the vault provider\n'
            '  # calls auth/token/create even when authenticating via the\n'
            '  # kubernetes auth method, which 403s under the least-privilege\n'
            '  # `terraformer` OpenBao policy (that policy grants only the KV\n'
            '  # paths this runner touches, not token-management endpoints).\n'
            '  skip_child_token = true\n'
            '\n'
            '  # GENERIC auth_login — NOT `auth_login_kubernetes`. The provider\n'
            '  # ships method-specific blocks only for userpass/aws/azure/cert/\n'
            '  # gcp/jwt/kerberos/oci/oidc/radius/token_file; kubernetes is not\n'
            '  # among them and must go through the generic block with an\n'
            '  # explicit login path. Emitting the non-existent block name made\n'
            '  # EVERY tenant apply die at provider-parse time with "Blocks of\n'
            '  # type auth_login_kubernetes are not expected here" — the tenant\n'
            '  # sat in `provisioning` until the caller timed out (2026-07-28).\n'
            '  auth_login {\n'
            f'    path = {json.dumps(f"auth/{mount}/login")}\n'
            '\n'
            '    parameters = {\n'
            f'      role = {json.dumps(s.vault_k8s_auth_role)}\n'
            '      # file(), not a baked/interpolated value: re-read from disk on\n'
            '      # every plan/apply, so kubelet\'s automatic projected-token\n'
            '      # rotation is picked up for free — no restart, no re-issue.\n'
            '      jwt = file("/var/run/secrets/kubernetes.io/serviceaccount/token")\n'
            '    }\n'
            '  }\n'
            '}\n'
        )

    def _write_vault_provider_file(self, workdir: Path) -> None:
        (workdir / "provider_vault.tf").write_text(self._vault_provider_hcl())

    async def _ensure_workspace(self, tenant_id: str) -> Path:
        workdir = self._workspace_dir(tenant_id)
        if workdir.exists():
            return workdir
        workdir.mkdir(parents=True, exist_ok=True)
        src = self._module_source()
        if not src.exists():
            raise FileNotFoundError(f"tenant TF module not found at {src}")
        for item in src.iterdir():
            target = workdir / item.name
            if item.is_dir():
                shutil.copytree(item, target)
            else:
                shutil.copy2(item, target)
        # The tenant module ships with no backend block by design (reusable-
        # module convention — see versions.tf's header comment in
        # pneuma-deployments); the runner supplies the backend shape and
        # binds it at init time via -backend-config. Without this stub,
        # -backend-config args have no `backend "s3" {}` block to attach to.
        (workdir / "backend.tf").write_text('terraform {\n  backend "s3" {}\n}\n')
        # Same convention extends to the vault provider block: the tenant
        # module declares no provider {} blocks of its own (see
        # versions.tf), so the runner supplies this one too, generated
        # fresh into every workspace.
        self._write_vault_provider_file(workdir)
        return workdir

    async def _init(self, workdir: Path, tenant_id: str) -> TerraformResult:
        backend_args: list[str] = []
        for k, v in self._backend_config(tenant_id).items():
            backend_args.extend(["-backend-config", f"{k}={v}"])
        result = await self._spawn(
            workdir,
            ["init", "-reconfigure", *backend_args],
            timeout=120,
        )
        if result.exit_code != 0:
            raise TerraformError("init", result)
        return result

    async def _tfvars_file(self, workdir: Path, inputs: TenantInputs) -> Path:
        path = workdir / "terraform.auto.tfvars.json"
        # Write with 0600 so only the runner uid can read; the file
        # carries plaintext admin tokens and lives on the pod filesystem
        # for the duration of the apply.
        path.write_text(json.dumps(self._tf_vars(inputs)))
        try:
            path.chmod(0o600)
        except OSError:
            # Non-POSIX filesystems (e.g. some emptyDir mediums) reject
            # chmod — accept since the workspace is per-pod ephemeral
            # and the readOnlyRootFilesystem securityContext on the
            # terraformer chart still applies.
            pass
        return path

    def _wipe_tfvars(self, workdir: Path) -> None:
        """Delete every credential-laden root-level tfvars file between
        runs: the fixed ``terraform.tfvars.json`` (written by the
        platform-secrets path) plus every root-level ``*.auto.tfvars.json``
        (written by the tenant path — glob catches the primary
        ``terraform.auto.tfvars.json`` and any future root-level
        additions). These embed admin tokens in plaintext; leaving them on
        disk past the apply means a future workspace read (or pod
        compromise) exfiltrates the full credential set.

        Non-recursive by design: ``_generated/*.auto.tfvars.json`` (the
        module var-files written by an upstream generator — see
        ``_module_var_files``) is declarative topology data, not
        credentials, and must survive between runs so the next apply
        still has it. Idempotent — silent if a file is already gone."""
        paths = [workdir / "terraform.tfvars.json", *sorted(workdir.glob("*.auto.tfvars.json"))]
        for path in paths:
            try:
                path.unlink()
            except FileNotFoundError:
                pass
            except OSError as exc:
                _LOG.warning("could not wipe tfvars file %s: %s", path, exc)

    def _module_var_files(self, workdir: Path) -> list[str]:
        """Flatten every ``_generated/*.auto.tfvars.json`` file (declarative
        topology data written by an upstream generator — e.g. the RMQ bus
        topology render step — never by this runner) into ``-var-file``
        apply/destroy arguments. Empty list when the directory is absent,
        which is the common case for tenants with no generated topology
        overrides."""
        generated = workdir / "_generated"
        if not generated.exists():
            return []
        args: list[str] = []
        for path in sorted(generated.glob("*.auto.tfvars.json")):
            args.extend(["-var-file", f"_generated/{path.name}"])
        return args

    def _provider_env(self) -> dict[str, str]:
        """Environment variables consumed directly by the terraform
        provider plugins (postgresql/rabbitmq/minio/vault/kubernetes) so
        their ``provider {}`` blocks in the tenant module can rely on each
        plugin's own env-var convention instead of an explicit credential
        attribute. Set on the subprocess environment only (see _spawn) —
        never argv, never written to a var file — so they never appear in
        a process listing or a log line.

        No VAULT_TOKEN here, deliberately: a static OpenBao token is the
        failure class (it expired and 403'd every tenant apply — 2026-07
        incident), so the vault provider now authenticates via
        auth_login_kubernetes in the generated `provider_vault.tf` (see
        _vault_provider_hcl / _ensure_workspace) instead of a stored
        credential. VAULT_ADDR is NOT set here either, but for the
        opposite reason: it is env-invariant, cluster config (a
        configmap-sourced value, not a secret), so it already reaches the
        subprocess for free via `env = os.environ.copy()` in _spawn — the
        Vault provider reads it directly when `address` is omitted from
        the provider block.

        The Kubernetes provider vars are populated only when this pod has
        a projected ServiceAccount token (the standard in-cluster
        signal) — a pod without one has no business reconciling
        k8s-backed tenant resources, and probing KUBERNETES_SERVICE_HOST
        unconditionally would silently point the provider at a stale/
        wrong cluster in a local dev shell that happens to have that env
        var set for an unrelated reason.
        """
        s = self._settings
        env: dict[str, str] = {
            "PGPASSWORD": s.postgres_superuser_password,
            "RABBITMQ_PASSWORD": s.rabbitmq_admin_password,
            "MINIO_USER": s.tf_state_backend_access_key,
            "MINIO_PASSWORD": s.minio_admin_password,
        }
        if _KUBE_SA_TOKEN_PATH.exists() and _KUBE_SA_CA_CERT_PATH.exists():
            host = os.environ.get("KUBERNETES_SERVICE_HOST", "")
            port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
            if host:
                env["KUBE_HOST"] = f"https://{host}:{port}"
            env["KUBE_TOKEN"] = _KUBE_SA_TOKEN_PATH.read_text().strip()
            env["KUBE_CLUSTER_CA_CERT_DATA"] = _KUBE_SA_CA_CERT_PATH.read_text()
        return env

    async def _output_json(self, workdir: Path) -> dict[str, Any]:
        result = await self._spawn(workdir, ["output", "-json"], timeout=30)
        if result.exit_code != 0:
            _LOG.warning("terraform output -json failed: %s", result.stderr)
            return {}
        try:
            raw: dict[str, Any] = json.loads(result.stdout) if result.stdout.strip() else {}
        except json.JSONDecodeError:
            _LOG.exception("terraform output JSON parse failed")
            return {}
        return {k: v.get("value") for k, v in raw.items()}

    async def _state_addresses(self, workdir: Path) -> set[str]:
        """ONE bulk `terraform state list` (no address filter) — replaces
        one per-registry-entry `state list <addr>` probe (2026-08-15
        19:01-19:11 incident: 4 tenants' per-resource probes queued on the
        heavy limit=2 semaphore burned the entire ~590s RunTenantReconcile
        budget before apply ever started; last_apply.log never got
        written). Runs on the wide, cheap `light=True` semaphore (see
        _read_spawn_semaphore) since it spawns no provider plugins.

        A non-zero exit (uninitialised/empty state, or a genuine read
        failure) returns an empty set rather than raising — conservative:
        every registry entry then gets its import attempted, exactly the
        old per-resource-probe fallback behaviour, and a real apply-time
        failure still surfaces from the `apply` step right after this.
        """
        result = await self._spawn_once(
            workdir, ["state", "list"], timeout=30, failure_expected=True, light=True,
        )
        if result.exit_code != 0:
            return set()
        return {line.strip() for line in result.stdout.splitlines() if line.strip()}

    async def _import_preexisting_resources(self, workdir: Path, inputs: TenantInputs) -> None:
        """Re-apply-drift convergence (see _IMPORT_ON_EXISTS_RESOURCES):
        for every registered resource not already in this workspace's
        state, attempt `terraform import`. A provider that reports the ID
        does NOT exist fails the import — that failure is swallowed here,
        since it just means the upcoming `apply` will create the resource
        fresh, exactly as before this fix. Only the ONE bulk state listing
        (`_state_addresses`) and this best-effort import run ahead of
        `apply`; nothing here can turn a real create-time error into a
        false convergence, because a genuine apply-time failure still
        surfaces from the `apply` step itself right after this.
        """
        existing = await self._state_addresses(workdir)
        for entry in _IMPORT_ON_EXISTS_RESOURCES:
            if entry.resource_address in existing:
                continue
            resource_id = entry.resource_id(inputs)
            result = await self._spawn_once(
                workdir,
                ["import", "-input=false", entry.resource_address, resource_id],
                timeout=60,
                failure_expected=True,
            )
            if result.exit_code == 0:
                _LOG.warning(
                    "tf import: adopted pre-existing %s (%s=%s) into state for tenant_id=%s "
                    "— re-apply drift recovery",
                    entry.name, entry.resource_address, resource_id, inputs.tenant_id,
                )
            else:
                _LOG.debug(
                    "tf import: %s (%s=%s) not found for tenant_id=%s — apply will create it",
                    entry.name, entry.resource_address, resource_id, inputs.tenant_id,
                )

    def _persist_last_apply_log(self, workdir: Path, command: str, result: TerraformResult) -> None:
        """Persist the full (scrubbed) combined stdout+stderr of the most
        recent apply/destroy to `<workdir>/last_apply.log` — written on
        BOTH success and failure, overwriting the previous run's log.
        Unlike the truncated tail in TerraformError/the failure log line,
        this is the FULL transcript, for postmortems where the 50-line
        tail isn't enough context. Best-effort: a write failure here must
        never mask the real apply/destroy result."""
        try:
            body = (
                f"# command: terraform {command}\n"
                f"# exit_code: {result.exit_code}\n"
                f"\n--- stdout ---\n{_scrub_secret_shaped(result.stdout)}\n"
                f"\n--- stderr ---\n{_scrub_secret_shaped(result.stderr)}\n"
            )
            (workdir / "last_apply.log").write_text(body)
        except OSError:
            _LOG.warning("could not persist last_apply.log in %s", workdir, exc_info=True)

    async def reconcile(self, inputs: TenantInputs, timeout: int | None = None) -> TerraformResult:
        effective_timeout = timeout or self._settings.apply_timeout_seconds
        async with self._lock_for(inputs.tenant_id):
            workdir = await self._ensure_workspace(inputs.tenant_id)
            await self._init(workdir, inputs.tenant_id)
            await self._tfvars_file(workdir, inputs)
            await self._import_preexisting_resources(workdir, inputs)
            try:
                result = await self._spawn(
                    workdir,
                    ["apply", "-auto-approve", "-no-color", *self._module_var_files(workdir)],
                    timeout=effective_timeout,
                )
                self._persist_last_apply_log(workdir, "apply", result)
                if result.exit_code != 0:
                    raise TerraformError("apply", result)
                outputs = await self._output_json(workdir)
                return TerraformResult(
                    exit_code=result.exit_code,
                    stdout=result.stdout,
                    stderr=result.stderr,
                    outputs=outputs,
                )
            finally:
                # Wipe the credential-laden tfvars file regardless of
                # success/failure — never leave admin tokens on disk
                # between runs.
                self._wipe_tfvars(workdir)

    async def destroy(self, inputs: TenantInputs, timeout: int | None = None) -> TerraformResult:
        effective_timeout = timeout or self._settings.destroy_timeout_seconds
        async with self._lock_for(inputs.tenant_id):
            workdir = await self._ensure_workspace(inputs.tenant_id)
            await self._init(workdir, inputs.tenant_id)
            await self._tfvars_file(workdir, inputs)
            try:
                result = await self._spawn(
                    workdir,
                    ["destroy", "-auto-approve", "-no-color", *self._module_var_files(workdir)],
                    timeout=effective_timeout,
                )
                self._persist_last_apply_log(workdir, "destroy", result)
                if result.exit_code != 0:
                    raise TerraformError("destroy", result)
                shutil.rmtree(workdir, ignore_errors=True)
                self._locks.pop(inputs.tenant_id, None)
                return result
            finally:
                # Belt-and-braces: if shutil.rmtree didn't fire (early
                # raise), at least wipe the tfvars file.
                self._wipe_tfvars(workdir)

    async def state(self, tenant_id: str) -> dict[str, Any]:
        """Read-only state inspection. Does NOT init or apply — runs
        ``terraform output -json`` against the existing workspace. If
        the workspace hasn't been bootstrapped (no `.terraform/`
        subdir), returns the unbootstrapped sentinel without trying
        to init (which would write admin tokens to disk for a read).
        """
        workdir = self._workspace_dir(tenant_id)
        if not workdir.exists():
            return {"exists": False, "outputs": {}}
        # `.terraform/` is created by init — its presence means the
        # backend config is already on disk and `output -json` will
        # work. Absence means the workspace has never been bootstrapped;
        # do NOT init here (read path).
        if not (workdir / ".terraform").exists():
            return {
                "exists": True,
                "outputs": {},
                "bootstrapped": False,
            }
        outputs = await self._output_json(workdir)
        return {"exists": True, "outputs": outputs, "bootstrapped": True}


    # --- Platform-secrets reconcile (provisioning.apply_platform_secrets) ---
    def _platform_secrets_workdir(self, env: str) -> Path:
        """Single env-scoped workspace per cluster — distinct from
        per-tenant workspaces. Held under workdir_root/_platform/<env>
        so the same path-traversal defence kicks in if a malformed env
        value reaches this method."""
        if env not in ("dev", "tst", "prod"):
            raise ValueError(f"invalid env {env!r}")
        root = self._settings.terraform_workdir_root.resolve()
        candidate = (root / "_platform" / env).resolve()
        if not str(candidate).startswith(str(root) + "/"):
            raise ValueError(f"platform-secrets env {env!r} escapes workdir root")
        return candidate

    def _platform_secrets_source(self) -> Path:
        return self._settings.terraform_standalone_root / "platform-secrets-apply"

    def _platform_secrets_backend_config(self, env: str) -> dict[str, str]:
        # Same flat-primitives-only key set as _backend_config() above —
        # kept identical so the two workspaces (per-tenant,
        # platform-secrets) never drift onto different-and-untested key
        # shapes. Endpoint + credentials travel env-only via _spawn
        # (AWS_ENDPOINT_URL_S3 / AWS_ACCESS_KEY_ID /
        # AWS_SECRET_ACCESS_KEY) — see _backend_config's comment for why
        # nested `endpoints` cannot be a CLI -backend-config argument.
        s = self._settings
        return {
            "bucket": s.tf_state_backend_bucket,
            "key": f"platform-secrets/{env}.tfstate",
            "region": s.tf_state_backend_region,
            "use_path_style": "true",
            "skip_credentials_validation": "true",
            "skip_region_validation": "true",
            "skip_metadata_api_check": "true",
            "skip_requesting_account_id": "true",
        }

    def _platform_secrets_tfvars(self, inputs: "PlatformSecretsInputs") -> dict[str, str]:
        return {
            "env": inputs.env,
            "platform_helm_charts_dir": str(self._settings.platform_helm_charts_dir),
        }

    async def _ensure_platform_workspace(self, env: str) -> Path:
        workdir = self._platform_secrets_workdir(env)
        workdir.mkdir(parents=True, exist_ok=True)
        source = self._platform_secrets_source()
        if not source.exists():
            raise TerraformError(
                "init",
                TerraformResult(
                    exit_code=1,
                    stdout="",
                    stderr=f"platform-secrets harness not found at {source}",
                    outputs={},
                ),
            )
        for f in source.iterdir():
            if f.is_file():
                shutil.copy2(f, workdir / f.name)
        # platform-secrets-apply's module (platform-secrets/) provisions
        # vault_kv_secret_v2 resources — it needs the same generated vault
        # provider block as the tenant workspace, for the same reason
        # (auth_login_kubernetes replaces the static OPENBAO_ADMIN_TOKEN).
        # REQUIRES the companion pneuma-deployments change that drops the
        # standalone harness's own hardcoded `provider "vault" {}` from
        # main.tf — two provider "vault" blocks in one root is a Terraform
        # "Duplicate provider configuration" error, so this harness's
        # main.tf must declare none of its own (mirrors the tenant
        # module's no-provider-blocks convention in versions.tf).
        self._write_vault_provider_file(workdir)
        return workdir

    async def _init_platform(self, workdir: Path, env: str) -> None:
        backend_args = []
        for k, v in self._platform_secrets_backend_config(env).items():
            backend_args.extend(["-backend-config", f"{k}={v}"])
        result = await self._spawn(
            workdir,
            ["init", "-input=false", "-no-color", *backend_args],
            timeout=self._settings.apply_timeout_seconds,
        )
        if result.exit_code != 0:
            raise TerraformError("init", result)

    async def _platform_tfvars_file(
        self, workdir: Path, inputs: "PlatformSecretsInputs"
    ) -> Path:
        path = workdir / "terraform.tfvars.json"
        path.write_text(json.dumps(self._platform_secrets_tfvars(inputs)))
        return path

    async def reconcile_platform_secrets(
        self, inputs: "PlatformSecretsInputs"
    ) -> TerraformResult:
        """Dispatch target for `provisioning.apply_platform_secrets`.

        Runs the standalone harness at
        `infrastructure/terraform/standalone/platform-secrets-apply` against
        the env-scoped workspace. The harness reads every chart's
        `secrets.schema.yaml` from settings.platform_helm_charts_dir and
        fans canonical OpenBao paths into per-service paths. Idempotent.
        """
        env = inputs.env
        async with self._lock_for(f"_platform_secrets_{env}"):
            workdir = await self._ensure_platform_workspace(env)
            await self._init_platform(workdir, env)
            await self._platform_tfvars_file(workdir, inputs)
            try:
                result = await self._spawn(
                    workdir,
                    ["apply", "-auto-approve", "-no-color"],
                    timeout=self._settings.apply_timeout_seconds,
                )
                if result.exit_code != 0:
                    raise TerraformError("apply", result)
                outputs = await self._output_json(workdir)
                return TerraformResult(
                    exit_code=result.exit_code,
                    stdout=result.stdout,
                    stderr=result.stderr,
                    outputs=outputs,
                )
            finally:
                self._wipe_tfvars(workdir)

    # --- Platform-resources reconcile (provisioning.apply_platform_resources) ---
    def _platform_resources_workdir(self, env: str) -> Path:
        """Single env-scoped workspace per cluster — mirrors
        _platform_secrets_workdir. Held under workdir_root/_platform_resources/<env>
        so the same path-traversal defence kicks in on a malformed env."""
        if env not in ("dev", "tst", "prod"):
            raise ValueError(f"invalid env {env!r}")
        root = self._settings.terraform_workdir_root.resolve()
        candidate = (root / "_platform_resources" / env).resolve()
        if not str(candidate).startswith(str(root) + "/"):
            raise ValueError(f"platform-resources env {env!r} escapes workdir root")
        return candidate

    def _platform_resources_source(self) -> Path:
        return self._settings.terraform_standalone_root / "platform-resources-apply"

    def _platform_resources_backend_config(self, env: str) -> dict[str, str]:
        # Same flat-primitives-only shape as every other workspace on this
        # runner. Key is `platform-resources/<env>.tfstate` — disjoint from
        # `platform-secrets/<env>.tfstate`, `platform/bus-topology/<env>.tfstate`,
        # and every per-tenant `tenants/<tenant_id>.tfstate` key.
        s = self._settings
        return {
            "bucket": s.tf_state_backend_bucket,
            "key": f"platform-resources/{env}.tfstate",
            "region": s.tf_state_backend_region,
            "use_path_style": "true",
            "skip_credentials_validation": "true",
            "skip_region_validation": "true",
            "skip_metadata_api_check": "true",
            "skip_requesting_account_id": "true",
        }

    def _platform_resources_tfvars(self, inputs: "PlatformResourcesInputs") -> dict[str, str]:
        # Only `env` — the postgresql provider's host/port/superuser
        # inputs travel as TF_VAR_* on the subprocess environment (see
        # _platform_resources_extra_env), never written to this file, for
        # the same reason PGPASSWORD never lands in a tfvars file
        # (_provider_env's docstring): a credential-laden tfvars.json
        # would sit on disk between the write and the _wipe_tfvars in the
        # `finally` block below.
        return {"env": inputs.env}

    def _platform_resources_extra_env(self) -> dict[str, str]:
        """TF_VAR_pg_host / TF_VAR_pg_port / TF_VAR_pg_superuser_password
        for the platform-resources-apply harness's `postgresql` provider
        blocks (see that harness's variables — it takes host/port/
        superuser/password as explicit vars, unlike the tenant module's
        provider-block-free convention). pg_host/pg_port are read from
        the SAME ambient PGHOST/PGPORT env vars the pod already carries
        (pneuma-terraformer chart values.yaml, non-secret cluster config
        — see PGHOST's REQUIRED comment there); pg_superuser_password
        reuses the exact credential _provider_env already injects as
        PGPASSWORD for the tenant module's postgresql provider. Passed
        as a per-call extra_env (mirrors apply_platform_auth's transient
        VAULT_TOKEN) rather than folded into _provider_env, since no
        other workspace on this runner needs a TF_VAR_-prefixed Postgres
        credential."""
        s = self._settings
        host = os.environ.get("PGHOST", "")
        if not host:
            raise ValueError(
                "PGHOST is not set on the terraformer pod environment — "
                "required by platform-resources-apply's postgresql provider"
            )
        return {
            "TF_VAR_pg_host": host,
            "TF_VAR_pg_port": os.environ.get("PGPORT", "5432"),
            "TF_VAR_pg_superuser_password": s.postgres_superuser_password,
        }

    async def _ensure_platform_resources_workspace(self, env: str) -> Path:
        workdir = self._platform_resources_workdir(env)
        workdir.mkdir(parents=True, exist_ok=True)
        source = self._platform_resources_source()
        if not source.exists():
            raise TerraformError(
                "init",
                TerraformResult(
                    exit_code=1,
                    stdout="",
                    stderr=f"platform-resources harness not found at {source}",
                    outputs={},
                ),
            )
        for f in source.iterdir():
            if f.is_file():
                shutil.copy2(f, workdir / f.name)
        # Same vault-provider-block-injection convention as
        # _ensure_platform_workspace — the harness's own main.tf declares
        # no `provider "vault" {}` of its own, this runner generates one
        # fresh into every workspace so auth_login_kubernetes (not a
        # static token) is what every apply on this runner uses.
        self._write_vault_provider_file(workdir)
        return workdir

    async def _init_platform_resources(self, workdir: Path, env: str) -> None:
        backend_args = []
        for k, v in self._platform_resources_backend_config(env).items():
            backend_args.extend(["-backend-config", f"{k}={v}"])
        result = await self._spawn(
            workdir,
            ["init", "-input=false", "-no-color", *backend_args],
            timeout=self._settings.apply_timeout_seconds,
        )
        if result.exit_code != 0:
            raise TerraformError("init", result)

    async def _platform_resources_tfvars_file(
        self, workdir: Path, inputs: "PlatformResourcesInputs"
    ) -> Path:
        path = workdir / "terraform.tfvars.json"
        path.write_text(json.dumps(self._platform_resources_tfvars(inputs)))
        return path

    async def reconcile_platform_resources(
        self, inputs: "PlatformResourcesInputs"
    ) -> TerraformResult:
        """Dispatch target for `provisioning.apply_platform_resources`.

        Runs the standalone harness at
        `infrastructure/terraform/standalone/platform-resources-apply`
        against the env-scoped workspace — the platform-tier sibling of
        `reconcile_platform_secrets`. Currently reconciles two things in
        one module: the inter-service-HMAC pair seed
        (modules/platform-resources/inter-service-hmac.tf, `ignore_changes
        = [data_json]` so a later apply never clobbers an operator's
        90-day rotation) and the ActivePieces least-privilege Postgres
        role (AIS-3/INJ-1). Both are create-only/idempotent — a re-apply
        is a no-op once the pairs and role already exist. Mirrors
        reconcile_platform_secrets's shape exactly except for the
        Postgres provider credentials, which travel as TF_VAR_* extra_env
        (see _platform_resources_extra_env) rather than the tfvars file.
        """
        env = inputs.env
        async with self._lock_for(f"_platform_resources_{env}"):
            workdir = await self._ensure_platform_resources_workspace(env)
            await self._init_platform_resources(workdir, env)
            await self._platform_resources_tfvars_file(workdir, inputs)
            extra_env = self._platform_resources_extra_env()
            try:
                result = await self._spawn(
                    workdir,
                    ["apply", "-auto-approve", "-no-color"],
                    timeout=self._settings.apply_timeout_seconds,
                    extra_env=extra_env,
                )
                if result.exit_code != 0:
                    raise TerraformError("apply", result)
                outputs = await self._output_json(workdir)
                return TerraformResult(
                    exit_code=result.exit_code,
                    stdout=result.stdout,
                    stderr=result.stderr,
                    outputs=outputs,
                )
            finally:
                self._wipe_tfvars(workdir)

    # --- Platform-bus-topology reconcile (provisioning.apply_platform_bus_topology) ---
    def _platform_bus_topology_workdir(self, env: str) -> Path:
        """Single env-scoped workspace per cluster — distinct from both
        the per-tenant workspaces AND the platform-secrets workspace.
        Held under workdir_root/_platform_bus_topology/<env> so the same
        path-traversal defence kicks in if a malformed env value reaches
        this method (mirrors _platform_secrets_workdir)."""
        if env not in ("dev", "tst", "prod"):
            raise ValueError(f"invalid env {env!r}")
        root = self._settings.terraform_workdir_root.resolve()
        candidate = (root / "_platform_bus_topology" / env).resolve()
        if not str(candidate).startswith(str(root) + "/"):
            raise ValueError(f"platform-bus-topology env {env!r} escapes workdir root")
        return candidate

    def _platform_bus_topology_source(self) -> Path:
        return self._settings.terraform_standalone_root / "platform-bus-topology-apply"

    def _platform_bus_topology_backend_config(self, env: str) -> dict[str, str]:
        # Same flat-primitives-only key set as _backend_config() /
        # _platform_secrets_backend_config() above — every workspace on
        # this runner shares the identical, TF-1.9-verified S3
        # backend-config shape (see _backend_config's comment for why
        # nested `endpoints` cannot be a CLI -backend-config argument).
        # Key is `platform/bus-topology/<env>.tfstate` — deliberately
        # disjoint from `platform-secrets/<env>.tfstate` and every
        # per-tenant `tenants/<tenant_id>.tfstate` key. See
        # pneuma-deployments' modules/platform-bus-topology/README.md
        # "State-key convention" — this is the canonical key that doc
        # names for the future backend-config wiring; this is that PR.
        s = self._settings
        return {
            "bucket": s.tf_state_backend_bucket,
            "key": f"platform/bus-topology/{env}.tfstate",
            "region": s.tf_state_backend_region,
            "use_path_style": "true",
            "skip_credentials_validation": "true",
            "skip_region_validation": "true",
            "skip_metadata_api_check": "true",
            "skip_requesting_account_id": "true",
        }

    def _platform_bus_topology_tfvars(
        self, inputs: "PlatformBusTopologyInputs"
    ) -> dict[str, str]:
        # The standalone harness's only required root variable is `env`;
        # `platform_vhost` defaults to null (computes /pneuma-<env>) and
        # is intentionally not overridden here — no caller-supplied
        # vhost override surface.
        return {"env": inputs.env}

    async def _ensure_platform_bus_topology_workspace(self, env: str) -> Path:
        workdir = self._platform_bus_topology_workdir(env)
        workdir.mkdir(parents=True, exist_ok=True)
        source = self._platform_bus_topology_source()
        if not source.exists():
            raise TerraformError(
                "init",
                TerraformResult(
                    exit_code=1,
                    stdout="",
                    stderr=f"platform-bus-topology harness not found at {source}",
                    outputs={},
                ),
            )
        for f in source.iterdir():
            if f.is_file():
                shutil.copy2(f, workdir / f.name)
        # Deliberately NO _write_vault_provider_file() call here: unlike
        # the tenant workspace and platform-secrets-apply, this harness's
        # `required_providers` is rabbitmq-only (pneuma-deployments'
        # standalone/platform-bus-topology-apply/main.tf declares a
        # `provider "rabbitmq" {}` block and no `vault` provider at all —
        # it never touches OpenBao). Retiring VAULT_TOKEN therefore has no
        # effect on this root; confirmed by grep against that harness's
        # main.tf, not assumed.
        return workdir

    async def _init_platform_bus_topology(self, workdir: Path, env: str) -> None:
        backend_args: list[str] = []
        for k, v in self._platform_bus_topology_backend_config(env).items():
            backend_args.extend(["-backend-config", f"{k}={v}"])
        result = await self._spawn(
            workdir,
            ["init", "-input=false", "-no-color", *backend_args],
            timeout=self._settings.apply_timeout_seconds,
        )
        if result.exit_code != 0:
            raise TerraformError("init", result)

    async def _platform_bus_topology_tfvars_file(
        self, workdir: Path, inputs: "PlatformBusTopologyInputs"
    ) -> Path:
        path = workdir / "terraform.tfvars.json"
        path.write_text(json.dumps(self._platform_bus_topology_tfvars(inputs)))
        return path

    async def reconcile_platform_bus_topology(
        self, inputs: "PlatformBusTopologyInputs"
    ) -> TerraformResult:
        """Dispatch target for `provisioning.apply_platform_bus_topology`.

        Runs the standalone harness at
        `infrastructure/terraform/standalone/platform-bus-topology-apply`
        against the env-scoped workspace. The harness reconciles the
        shared pooled `/pneuma-<env>` RabbitMQ vhost topology (exchanges,
        queues, bindings, per-service ACLs) from the vendored bus-topology
        SSOT (`modules/platform-bus-topology/_generated/bus_topology.auto.tfvars.json`,
        read by the harness itself via `jsondecode(file(...))` — never
        passed as a runner var-file). Idempotent.

        BUILD-ONLY / inert today (see pneuma-deployments'
        modules/platform-bus-topology/README.md "BUILD-ONLY — not yet
        live"): a real `terraform apply` against a live environment is
        gated on a separate, later, Dean-approved `terraform import` +
        Helm-range-removal step (plan §P5.3). This method — and the gRPC
        handler that calls it — exist so that later activation is a
        cycle-status flip (`core:platform_apply_bus_topology`
        `draft` → `active`, plan §P7.3), not a code change. Nothing
        dispatches this method today.
        """
        env = inputs.env
        async with self._lock_for(f"_platform_bus_topology_{env}"):
            workdir = await self._ensure_platform_bus_topology_workspace(env)
            await self._init_platform_bus_topology(workdir, env)
            await self._platform_bus_topology_tfvars_file(workdir, inputs)
            try:
                result = await self._spawn(
                    workdir,
                    ["apply", "-auto-approve", "-no-color"],
                    timeout=self._settings.apply_timeout_seconds,
                )
                if result.exit_code != 0:
                    raise TerraformError("apply", result)
                outputs = await self._output_json(workdir)
                return TerraformResult(
                    exit_code=result.exit_code,
                    stdout=result.stdout,
                    stderr=result.stderr,
                    outputs=outputs,
                )
            finally:
                self._wipe_tfvars(workdir)


    # --- Platform-auth bootstrap (services.terraformer.src.openbao_bootstrap) ---
    #
    # Break-glass apply target for ensure_platform_auth's cold-start path.
    # Creates vault_policy.terraformer + vault_kubernetes_auth_backend_role.
    # terraformer — the two resources modules/bootstrap/terraformer_auth.tf
    # (pneuma-deployments@290c443) added to the ONE-SHOT cluster-bootstrap
    # module. That module needs full cluster-admin kubeconfig and a dozen
    # unrelated inputs (Cloudflare, ArgoCD, Kyverno, ...), so it cannot be
    # what a running pod re-applies at every boot. This harness instead
    # extracts just those two resources into their own standalone root —
    # `platform-auth-bootstrap`, baked from pneuma-deployments'
    # infrastructure/terraform/standalone/platform-auth-bootstrap/ via the
    # SAME Dockerfile mechanism that already bakes platform-secrets-apply /
    # platform-bus-topology-apply (DEPLOYMENTS_REF build-arg — see
    # settings.terraform_standalone_root). It references the existing
    # "kubernetes" auth-backend MOUNT by path (var.vault_k8s_auth_mount)
    # rather than managing a vault_auth_backend resource itself: that mount
    # already exists live (created once by modules/bootstrap/
    # external_secrets.tf's vault_auth_backend.kubernetes, which ESO's role
    # already authenticates against), and a standalone harness with its own
    # Terraform state cannot reference a resource owned by a different
    # state.
    _PLATFORM_AUTH_HARNESS = "platform-auth-bootstrap"

    def _platform_auth_workdir(self) -> Path:
        """Single per-pod workspace. Unlike _platform_secrets_workdir /
        _platform_bus_topology_workdir (which serve a cycle-dispatched
        capability that names an `env` per-request), this bootstraps THIS
        pod's own OpenBao identity in THIS pod's own cluster (settings.env)
        — there is no caller-supplied env axis to parameterize."""
        root = self._settings.terraform_workdir_root.resolve()
        candidate = (root / "_platform_auth").resolve()
        if not str(candidate).startswith(str(root) + "/"):
            raise ValueError("platform-auth workdir escapes workdir root")
        return candidate

    def _platform_auth_source(self) -> Path:
        return self._settings.terraform_standalone_root / self._PLATFORM_AUTH_HARNESS

    def _platform_auth_backend_config(self) -> dict[str, str]:
        # Same flat-primitives-only shape as every other backend-config
        # dict on this runner (see _backend_config's comment for why
        # nested `endpoints` cannot travel via CLI -backend-config). Key
        # is `platform/auth-bootstrap/<env>.tfstate` — disjoint from every
        # other state key this runner uses.
        s = self._settings
        return {
            "bucket": s.tf_state_backend_bucket,
            "key": f"platform/auth-bootstrap/{s.env}.tfstate",
            "region": s.tf_state_backend_region,
            "use_path_style": "true",
            "skip_credentials_validation": "true",
            "skip_region_validation": "true",
            "skip_metadata_api_check": "true",
            "skip_requesting_account_id": "true",
        }

    def _platform_auth_tfvars(self) -> dict[str, str]:
        s = self._settings
        return {
            "pooled_namespace": s.pneuma_namespace,
            "vault_k8s_auth_mount": s.vault_k8s_auth_mount,
            "vault_k8s_auth_role": s.vault_k8s_auth_role,
            "terraformer_service_account_name": s.terraformer_service_account_name,
        }

    async def _ensure_platform_auth_workspace(self) -> Path:
        workdir = self._platform_auth_workdir()
        workdir.mkdir(parents=True, exist_ok=True)
        source = self._platform_auth_source()
        if not source.exists():
            raise TerraformError(
                "init",
                TerraformResult(
                    exit_code=1,
                    stdout="",
                    stderr=f"platform-auth-bootstrap harness not found at {source}",
                    outputs={},
                ),
            )
        for f in source.iterdir():
            if f.is_file():
                shutil.copy2(f, workdir / f.name)
        # The harness ships with no backend block (same reusable-root
        # convention as the tenant module's versions.tf) — stub one so
        # -backend-config args have something to bind to.
        (workdir / "backend.tf").write_text('terraform {\n  backend "s3" {}\n}\n')
        # Deliberately NO _write_vault_provider_file() here: that generated
        # block authenticates via auth_login_kubernetes against the very
        # role this harness's job is to create — using it here would be
        # circular (the role never exists yet on the only occasion this
        # harness runs: a cold start). This harness's own main.tf instead
        # declares a bare `provider "vault" {}` that reads VAULT_ADDR
        # (ambient — see _spawn) and VAULT_TOKEN (the transient break-glass
        # root token, injected only for this one apply via _spawn's
        # extra_env — see apply_platform_auth) straight from the
        # subprocess environment.
        return workdir

    async def _init_platform_auth(self, workdir: Path) -> None:
        backend_args: list[str] = []
        for k, v in self._platform_auth_backend_config().items():
            backend_args.extend(["-backend-config", f"{k}={v}"])
        result = await self._spawn(
            workdir,
            ["init", "-input=false", "-no-color", *backend_args],
            timeout=120,
        )
        if result.exit_code != 0:
            raise TerraformError("init", result)

    async def _platform_auth_tfvars_file(self, workdir: Path) -> Path:
        path = workdir / "terraform.tfvars.json"
        path.write_text(json.dumps(self._platform_auth_tfvars()))
        return path

    async def apply_platform_auth(self, root_token: str, timeout: int = 120) -> TerraformResult:
        """Break-glass entrypoint — one step of the converge flow in
        services.terraformer.src.openbao_bootstrap.ensure_platform_auth.

        Applies the platform-auth-bootstrap harness using a transient root
        token the CALLER already minted via OpenBao's generate-root flow —
        never the (not-yet-existing) k8s-auth role this very apply
        creates. The token travels via VAULT_TOKEN on THIS ONE
        subprocess's env only (see _spawn's extra_env): never written to a
        var file, never logged, never folded into _provider_env() (which
        every other terraform invocation on this runner shares and which
        deliberately carries no VAULT_TOKEN — see _provider_env's
        docstring on why a stored token is the failure class this whole
        feature removes).
        """
        async with self._lock_for("_platform_auth"):
            workdir = await self._ensure_platform_auth_workspace()
            await self._init_platform_auth(workdir)
            await self._platform_auth_tfvars_file(workdir)
            try:
                result = await self._spawn(
                    workdir,
                    ["apply", "-auto-approve", "-no-color"],
                    timeout=timeout,
                    extra_env={"VAULT_TOKEN": root_token},
                )
                if result.exit_code != 0:
                    raise TerraformError("apply", result)
                return result
            finally:
                self._wipe_tfvars(workdir)


_runner: TerraformRunner | None = None


def get_runner() -> TerraformRunner:
    global _runner
    if _runner is None:
        _runner = TerraformRunner()
    return _runner
