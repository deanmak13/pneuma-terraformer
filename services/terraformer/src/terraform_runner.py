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
import fcntl
import json
import logging
import os
import re
import shlex
import shutil
import signal
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
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

# Cross-process half of the `terraform init` single-flight gate (see
# TerraformRunner._init_gate): an flock on this file inside the plugin
# cache dir. Today every process has its own emptyDir, so only the
# in-process asyncio.Lock ever contends — but `terraform_plugin_cache_dir`
# may point at a volume several pods/processes share, and Terraform's
# concurrent-init hazard follows the DIRECTORY, not the process. Polled
# (LOCK_NB) rather than blocking so the event loop keeps serving.
_PLUGIN_CACHE_LOCK_FILE = ".init.lock"
_PLUGIN_CACHE_LOCK_POLL_SECONDS = 0.25

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
# A second, unrelated failure class joined this registry 2026-08-18 (see
# `terraform_provider_grpc_epoll1_abort` below): the SAME "matched
# signature -> bounded retry" mechanism, but with its OWN attempt count
# and backoff window, since a fork-timing race clears on a different
# timescale than a sub-second Postgres catalog lock. Each row below names
# ONE transient failure's fingerprint (a SQLSTATE/vendor code, a crash
# assertion string, ...), matched case-insensitively against a failed
# run's combined stdout+stderr, PLUS that row's own retry shape. A 4th
# signature is a NEW ROW here, never an `if "..." in err:` branch at the
# `_spawn` call site: `_spawn` treats every row identically, reading its
# retry shape from the matched row rather than a shared global.

# Bounded attempts for a `_spawn` dispatch that keeps failing with a
# matched transient-conflict signature — small and fixed, not a Settings
# field: terraform apply/init/destroy are idempotent so re-running is
# safe, but nothing about a deployment's environment should change how
# many times we blindly retry the SAME queued request before surfacing
# the failure. This is the DEFAULT every registry row gets unless it sets
# its own `max_attempts` (see `_TransientConflictSignature` below).
_MAX_TRANSIENT_CONFLICT_ATTEMPTS = 3

# Backoff before each transient-conflict retry, doubling per retry (1s
# then 2s, for the 3-attempt default above) — these are catalog-level
# lock-contention windows measured in milliseconds to low seconds, not a
# rate-limited external API, so a longer backoff would only eat further
# into the caller's already-shared timeout budget for no benefit. The
# DEFAULT every registry row gets unless it sets its own
# `backoff_base_seconds`.
_TRANSIENT_CONFLICT_BACKOFF_BASE_SECONDS = 1.0


@dataclass(frozen=True)
class _TransientConflictSignature:
    name: str
    # Case-insensitive substrings; ANY match is a hit. Each row carries
    # both the raw SQLSTATE/vendor code and the condition's human name so
    # a driver/provider that surfaces only one of the two (bare message
    # text vs. a code appended in parens, as in the live example above) is
    # still recognised.
    patterns: tuple[str, ...]
    # Per-signature retry shape — defaults reproduce the original
    # (Postgres catalog-conflict) rows' behaviour exactly, so adding a new
    # row with a different failure class (see the epoll1-abort row below)
    # never changes an existing row's attempts/backoff.
    max_attempts: int = _MAX_TRANSIENT_CONFLICT_ATTEMPTS
    backoff_base_seconds: float = _TRANSIENT_CONFLICT_BACKOFF_BASE_SECONDS
    # Ceiling on the doubling backoff (backoff_base_seconds * 2**(attempt-
    # 1)) — None (the original rows' default) leaves it uncapped, fine
    # when max_attempts is small enough that doubling never grows large.
    # A row with more attempts (the epoll1-abort row: 5) sets this so
    # backoff doesn't balloon to minutes on the later attempts.
    backoff_cap_seconds: float | None = None


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
    # gRPC C-core epoll1-poller fork-safety abort — live 2026-08-18 against
    # tenant 72f36de4 (and an earlier, unattributed 2026-08 "round5b"
    # occurrence with the identical signature): `terraform init`/`apply`
    # exits -6 (SIGABRT) with stderr
    #   F ev_epoll1_linux.cc:1121 Check failed: next_worker->state == KICKED
    #
    # True crash source is THIS PROCESS, not terraform or a provider
    # plugin: main.py's lifespan starts a live `grpc.aio.server()`
    # (grpc_server.py) in the SAME OS process that `_spawn_once` forks via
    # `asyncio.create_subprocess_exec(..., cwd=str(workdir), ...)` below.
    # grpcio wraps grpc-core (a C library, not grpc-go) — its background
    # poller threads are not fork-safe unless fork support is explicitly
    # enabled (GRPC_ENABLE_FORK_SUPPORT + GRPC_POLL_STRATEGY=poll, set at
    # the container level — see the terraformer chart's configmap). Passing
    # `cwd=` forces CPython's subprocess machinery onto the fork+exec path
    # instead of posix_spawn, so EVERY terraform invocation forks this
    # process; without fork support the child inherits a torn copy of
    # grpc-core's epoll1 worker-queue state and asserts before it ever
    # reaches execve(). This is a documented gRPC bug class (grpc/grpc
    # #29044, #17253, doc/fork_support.md), not a genuine terraform/HCL
    # failure — re-running is safe (terraform init/apply are idempotent)
    # and typically clears on the very next attempt, since it is a
    # fork-timing race rather than a systemic outage. 5 attempts / 10-30s
    # backoff (vs. the Postgres rows' 3/1-2s) because the chart-level
    # mitigation above may not have rolled out to every pod yet — a
    # longer, more patient retry window covers that gap without leaving a
    # tenant stuck `provisioning` for the ~15-minute sweeper cycle
    # (pneuma-engine core:tenant_provisioning_sweeper) to pick up.
    _TransientConflictSignature(
        name="terraform_provider_grpc_epoll1_abort",
        patterns=("ev_epoll1_linux.cc", "next_worker->state == kicked"),
        max_attempts=5,
        backoff_base_seconds=10.0,
        backoff_cap_seconds=30.0,
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


@asynccontextmanager
async def _no_op_async_cm() -> AsyncIterator[None]:
    """`async with`-compatible no-op — the out-of-cluster/unit-test arm of
    `TerraformRunner._tenant_lease_cm` below, where there is no in-cluster
    Kubernetes API to lease against and the process-local `_lock_for`
    asyncio.Lock is this environment's only (and, for a single always-up
    process, already-sufficient) guard. `contextlib.nullcontext()` is NOT
    usable here — it only implements sync `__enter__`/`__exit__`, not the
    `__aenter__`/`__aexit__` an `async with` requires."""
    yield


# Margin added to a tenant apply/destroy's own `effective_timeout` when
# sizing how long a WAITER polls for the cross-process Lease
# `_tenant_lease_cm` acquires (see that method) — NOT a margin on the
# timed `_spawn` call itself. The holder keeps the Lease from BEFORE
# `_ensure_workspace`/`_init`/`_import_preexisting_resources` even start,
# none of which are bounded by `effective_timeout` (that budget is only
# ever passed to the timed `apply`/`destroy` _spawn call), so a waiter
# that gave up after `effective_timeout` alone would fail while the
# holder is still legitimately doing that up-front work.
_TENANT_LEASE_MARGIN_SECONDS = 300

# How long the tenant Lease stays valid without a renewal, and how often
# the holder renews it. These used to be one number — the acquire
# timeout above (900s) — with no renewal, so a holder that died mid-apply
# left its tenant locked for the full 15 minutes: TST 2026-09-06 10:02Z,
# pod terraformer-79c74b9fcd-2ddqj evicted 14s into tenant 0270dc40's
# apply, its successor polled the dead holder's lease until ~10:17Z and
# the owner sat on /signup/created the whole time. With renewal every
# `_TENANT_LEASE_RENEW_SECONDS` a live holder never expires (the
# 2026-08-18 tenant-72f36de4 collision `_kill_process_group` documents
# stays impossible — the duration below is only ever reached by a holder
# that has actually stopped), while a dead holder frees the tenant
# within one duration. Four renewal attempts fit inside one duration, so
# a single transient API failure cannot cost the lease.
_TENANT_LEASE_DURATION_SECONDS = 60
_TENANT_LEASE_RENEW_SECONDS = 15

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
class _PlatformResourcesImportEntry:
    name: str
    resource_address: str
    resource_id: str


# Mirrors pneuma-deployments infrastructure/terraform/modules/platform-
# resources/inter-service-hmac.tf's `local.inter_service_hmac_pairs` KEYS
# exactly — that .tf file's own header comment already documents this
# "kept in sync by inspection at PR time" convention against
# services/common/rpc/service_pairs.py (pneuma-engine); this is the SAME
# convention one level removed. Verified against pneuma-deployments
# origin/main at authoring time (2026-08-19).
_INTER_SERVICE_HMAC_PAIRS: tuple[str, ...] = (
    "brain-brain",
    "connector-gateway-agno",
    "connector-gateway-connector-gateway",
    "cycle-executor-brain",
    "cycle-executor-connector-gateway",
    "harness-api-mimesis",
    "brain-brain-api",
    "brain-cycle-api",
    "connector-gateway-brain-api",
    "conversation-api-brain-api",
    "cycle-executor-brain-api",
    "mimesis-api-brain-api",
    "tenant-api-brain",
    "connector-api-connector-gateway",
    "mimesis-connector-gateway",
    "tenant-api-connector-gateway",
    "brain-connector-gateway",
    "platform-api-connector-gateway",
    "portal-brain-api",
    "mimesis-tenant-api",
)


def _platform_resources_import_entries(env: str) -> tuple[_PlatformResourcesImportEntry, ...]:
    """Registry for `_import_preexisting_platform_resources` — same
    design as `_IMPORT_ON_EXISTS_RESOURCES` (LAW: design for N, never for
    1), for the platform-resources workspace. `env` is accepted (not
    currently used in any ID) for symmetry with the tenant registry's
    per-input callables and because a future entry may need it (this
    workspace IS env-scoped, unlike the tenant one which is per-tenant).
    """
    entries = [
        _PlatformResourcesImportEntry(
            name="activepieces_app_role",
            resource_address="postgresql_role.activepieces_app",
            # Mirrors modules/platform-resources's `var.activepieces_
            # role_name` default ("activepieces_app") — the standalone
            # harness never overrides it. cyrilgdn/postgresql provider: a
            # postgresql_role's import ID IS the role name, no composite
            # prefix (same convention as the tenant registry's
            # _tenant_app_role_id).
            resource_id="activepieces_app",
        ),
        _PlatformResourcesImportEntry(
            name="activepieces_database",
            resource_address="postgresql_database.activepieces",
            # Mirrors `var.activepieces_database` default ("activepieces").
            # Same CREATE-ONLY failure class as the role (postgres.tf
            # documents this module CREATEs the database declaratively —
            # a partial apply that created it but died before the state
            # write hits the identical "already exists" class as the role).
            # cyrilgdn/postgresql provider: a postgresql_database's import
            # ID IS the database name.
            resource_id="activepieces",
        ),
    ]
    for pair in _INTER_SERVICE_HMAC_PAIRS:
        entries.append(
            _PlatformResourcesImportEntry(
                name=f"inter_service_hmac_{pair}",
                resource_address=f'vault_kv_secret_v2.inter_service_hmac["{pair}"]',
                # hashicorp/vault provider: a vault_kv_secret_v2's import
                # ID is "<mount>/<path>" (the bare KV-v2 logical path, NOT
                # the "/data/" HTTP-API form used elsewhere in this file
                # for `vault_kv_secret_v2` DATA SOURCE reads). Mount is
                # hardcoded "pneuma" by the standalone harness's
                # `module "platform_resources" { vault_kv_mount = "pneuma" }`
                # block (infrastructure/terraform/standalone/platform-
                # resources-apply/main.tf) — the same singular platform
                # mount every other workspace on this runner uses.
                resource_id=f"pneuma/infra/inter-service-hmac/{pair}",
            )
        )
    return tuple(entries)


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


def _kill_process_group(proc: Any) -> None:
    """Kill the ENTIRE process group `_spawn_once` started `proc` into
    (see `start_new_session=True` on its `asyncio.create_subprocess_exec`
    call) — not just `proc`'s own PID. `terraform` spawns each provider
    (postgresql/rabbitmq/minio/vault/kubernetes) as a SEPARATE child
    process over HashiCorp's go-plugin protocol; a bare `proc.kill()`
    (SIGKILL to `terraform`'s own PID only) never touches those
    grandchildren, which can then survive as orphans — reparented to this
    container's PID 1 — and keep running against a live provider
    connection.

    Live 2026-08-18 against tenant 72f36de4: a caller-side retry
    cancelled an in-flight `reconcile()` at 20:06; the cancelled
    `_spawn_once` correctly killed the immediate `terraform` PID, but its
    already-forked `terraform-provider-postgresql` plugin process
    survived as an orphan and kept executing its REVOKE. A fresh 20:10
    retry for the SAME tenant then collided with it —
    `pq: tuple concurrently updated (XX000)` — two attempts for one
    tenant genuinely running concurrently despite `_lock_for`'s per-tenant
    asyncio.Lock, because that Lock only ever serialises NEW `_spawn`
    calls; it cannot reach into an already-orphaned previous attempt's
    surviving subprocess tree.

    `os.getpgid`/`os.killpg` require a real PID — deliberately broad
    `except` below so a `proc` that HAS no real PID (every existing unit
    test's fake process double) or has already exited falls straight back
    to the always-correct, already-tested `proc.kill()`. Production
    `proc` objects are real `asyncio.subprocess.Process` instances with a
    real `pid`, so the group-kill path is what actually fires there.
    """
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        return
    except (AttributeError, ProcessLookupError, PermissionError, OSError):
        pass
    try:
        proc.kill()
    except ProcessLookupError:
        pass


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
        # Single-flight gate for `terraform init` — the only command that
        # WRITES the shared TF_PLUGIN_CACHE_DIR (see _init_gate). Lazily
        # created like the semaphores above.
        self._init_lock: asyncio.Lock | None = None

    def _lock_for(self, tenant_id: str) -> asyncio.Lock:
        if tenant_id not in self._locks:
            self._locks[tenant_id] = asyncio.Lock()
        return self._locks[tenant_id]

    def _tenant_lease_cm(self, tenant_id: str, effective_timeout: float) -> Any:
        """`async with` this around the WHOLE plan/apply (or destroy)
        cycle, nested INSIDE `_lock_for`'s process-local lock — see
        reconcile()/destroy()'s call sites. Cross-process/cross-restart
        single-flight, on top of (never instead of) that local lock:
        `_lock_for`'s asyncio.Lock lives only in THIS process's memory,
        so it offers zero protection across a pod restart (exactly what
        the epoll1-abort crash mitigated elsewhere in this module can
        trigger) or, if this chart is ever scaled beyond `replicas: 1`,
        across a second replica. A Kubernetes Lease is durable
        orchestration/coordination state — the SAME category
        `kube_lease_mutex.py`'s reconcile_cli.py caller already uses for
        the platform-secrets/platform-resources reconcile (NOT a
        Terraform-CLI-bypassing infra mutation; see that module's
        docstring) — so it survives exactly the restart the local lock
        does not.

        No-ops (returns `_no_op_async_cm()`) when this pod has no
        projected ServiceAccount token — mirrors `_provider_env`'s
        identical in-cluster check — so every existing unit test (none
        of which stands up an in-cluster Kubernetes API) keeps
        exercising only the already-proven `_lock_for` path, unchanged.
        """
        if not (_KUBE_SA_TOKEN_PATH.exists() and _KUBE_SA_CA_CERT_PATH.exists()):
            return _no_op_async_cm()
        from services.terraformer.src.kube_lease_mutex import KubeLeaseMutex

        return KubeLeaseMutex(
            f"tf-tenant-{tenant_id}",
            # Short and renewed (see the constants' comment): a live
            # holder renews every _TENANT_LEASE_RENEW_SECONDS, a dead one
            # frees the tenant within _TENANT_LEASE_DURATION_SECONDS.
            lease_duration_seconds=_TENANT_LEASE_DURATION_SECONDS,
            renew_interval_seconds=_TENANT_LEASE_RENEW_SECONDS,
            # Bounded, not unbounded: a caller that waited this long for
            # another holder to finish was already past any sane RPC
            # deadline — failing fast with a clear LeaseAcquireTimeout
            # beats hanging silently past it.
            acquire_timeout_seconds=int(effective_timeout)
            + _TENANT_LEASE_MARGIN_SECONDS,
            # Faster than KubeLeaseMutex's own 5s default — a tenant
            # apply that's actually queued behind another one should
            # notice the lease freed up promptly rather than adding up
            # to another ~5s of dead waiting on top of whatever the held
            # apply's own real runtime already cost it.
            poll_interval_seconds=2.0,
        )

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

    def _init_gate(self) -> asyncio.Lock:
        """Process-wide single-flight gate for `terraform init`, held by
        `_spawn` for the subprocess's whole lifetime whenever the command
        is `init` — regardless of which of the init call sites (tenant,
        standalone, platform harnesses) spawned it.

        Terraform documents TF_PLUGIN_CACHE_DIR as NOT concurrency-safe:
        "behavior in environments with multiple terraform init calls is
        undefined" (hashicorp/terraform#31964, #33497) — two inits that
        both miss the cache unpack the same provider into it at once,
        and the loser can leave a half-written package that every later
        init then symlinks to. The heavy semaphore admits
        `max_concurrent_terraform_runs` (default 2) spawns, so two
        tenants' inits DO overlap without this. Only `init` touches the
        cache — apply/plan/output/state read the symlinks init left in
        the workspace — so serialising init alone keeps every other
        command's concurrency intact. Lazily created for the same reason
        as `_spawn_semaphore`.

        This is the in-process half; `_lock_plugin_cache` is the
        cross-process half (an flock inside the cache dir itself), taken
        right after it — the hazard belongs to the directory, and the
        `terraform_plugin_cache_dir` override lets several processes or
        pods share one."""
        if self._init_lock is None:
            self._init_lock = asyncio.Lock()
        return self._init_lock

    async def _lock_plugin_cache(self, cache_dir: Path, budget: float) -> int | None:
        """Take the cross-process `terraform init` lock: an exclusive
        flock on `_PLUGIN_CACHE_LOCK_FILE` inside `cache_dir`, polled
        non-blocking so the event loop keeps serving while another
        process's init finishes unpacking. Returns the fd holding the
        lock (release with `_unlock_plugin_cache`), or None once
        `budget` seconds pass without it — the caller then fails with
        exit 124, the same contract as the semaphore queue. flock is
        per open file description, so this also serialises against any
        other holder in THIS process (tests, or a second runner
        instance) — `_init_gate` merely keeps in-process waiters off
        the poll loop."""
        fd = os.open(cache_dir / _PLUGIN_CACHE_LOCK_FILE, os.O_RDWR | os.O_CREAT, 0o644)
        deadline = time.monotonic() + budget
        try:
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        os.close(fd)
                        return None
                    await asyncio.sleep(_PLUGIN_CACHE_LOCK_POLL_SECONDS)
                else:
                    return fd  # caller owns the fd (and the lock) from here
        except BaseException:
            # Cancelled mid-poll (the gRPC caller's deadline) — never
            # leak the fd; it holds no lock on this branch.
            os.close(fd)
            raise

    @staticmethod
    def _unlock_plugin_cache(fd: int) -> None:
        # Closing the fd drops the flock on its own; the explicit unlock
        # is the fast path. Never let it keep the fd open.
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    def _refuse_to_start(
        self,
        args: list[str],
        timeout: float,
        queued_s: float,
        remaining: float,
        workdir: Path,
        safe_cmd: str,
    ) -> TerraformResult:
        """The exit-124 result for a spawn whose queue wait — for the
        permit, or for `init` also the two halves of the init gate —
        consumed its budget down to the `_MIN_RUN_SECONDS_AFTER_QUEUE`
        floor. One helper so every wait in `_spawn_once` applies the
        SAME floor with the same message: a doomed run must not start
        just because it queued on the gate rather than on the permit."""
        _LOG.warning(
            "tf spawn refusing to start: queue wait %.1fs left only %.1fs of the "
            "%ds budget (floor=%ds): workdir=%s cmd=%s",
            queued_s, remaining, timeout, _MIN_RUN_SECONDS_AFTER_QUEUE, workdir, safe_cmd,
        )
        return TerraformResult(
            exit_code=124,
            stdout="",
            stderr=(
                f"terraform {args[0]} queue wait ({queued_s:.1f}s) consumed the "
                f"{timeout}s budget, leaving only {remaining:.1f}s to run — refusing "
                "to start rather than begin work that would be killed mid-run"
            ),
            outputs={},
        )

    @staticmethod
    def _init_gate_timeout(behind: str, waited: float, timeout: float) -> TerraformResult:
        """The exit-124 result for an `init` that waited out its whole
        remaining budget on one half of the init gate (`behind` names
        which) without ever getting it."""
        return TerraformResult(
            exit_code=124,
            stdout="",
            stderr=(
                f"terraform init queued too long behind {behind} "
                f"(waited >{waited:.1f}s of the {timeout}s budget)"
            ),
            outputs={},
        )

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
            # Attempts/backoff come from the MATCHED signature, never a
            # shared global — see _TransientConflictSignature's fields:
            # a fork-timing crash (5 attempts, 10-30s) and a sub-second
            # Postgres catalog lock (3 attempts, 1-2s) clear on genuinely
            # different timescales.
            if signature is None or attempt >= signature.max_attempts:
                return result

            backoff = signature.backoff_base_seconds * (2 ** (attempt - 1))
            if signature.backoff_cap_seconds is not None:
                backoff = min(backoff, signature.backoff_cap_seconds)
            remaining = timeout - (time.monotonic() - overall_start)
            if remaining <= backoff + _MIN_RUN_SECONDS_AFTER_QUEUE:
                _LOG.warning(
                    "tf spawn NOT retrying transient conflict signature=%s "
                    "(attempt %d/%d): only %.1fs of the %.1fs budget remains, "
                    "below backoff+floor (%.1fs+%ds): workdir=%s cmd=%s",
                    signature.name, attempt, signature.max_attempts,
                    remaining, timeout, backoff, _MIN_RUN_SECONDS_AFTER_QUEUE,
                    workdir, args[0] if args else "<no-args>",
                )
                return result

            attempt += 1
            _LOG.info(
                "tf spawn retrying after transient conflict: signature=%s "
                "attempt=%d/%d workdir=%s cmd=%s",
                signature.name, attempt, signature.max_attempts,
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
        # One unpacked provider set per pod, symlinked into every
        # workspace's .terraform/providers — instead of one full copy
        # per workspace. 104 kubelet evictions on TST (2026-08-19 →
        # 2026-09-06, every one `Usage of EmptyDir volume "workspace"
        # exceeds the limit "2Gi"`) were this: ~10 tenant inits filled
        # the volume, the pod died mid-apply, and the next signup waited
        # out the dead pod's lease. Terraform refuses a cache dir that
        # does not exist yet, so create it here rather than only at pod
        # startup — the reconcile CronJob runs this same code path from
        # a fresh container with no lifespan hook.
        plugin_cache_dir = self._settings.computed_plugin_cache_dir
        plugin_cache_dir.mkdir(parents=True, exist_ok=True)
        env["TF_PLUGIN_CACHE_DIR"] = str(plugin_cache_dir)
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
        #
        # `init` additionally holds both halves of the single-flight init
        # gate (see _init_gate / _lock_plugin_cache) — each set only once
        # actually acquired, so the same `finally` releases exactly what
        # was taken.
        held_init_gate: asyncio.Lock | None = None
        held_cache_lock: int | None = None
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
                return self._refuse_to_start(args, timeout, _queued_s, remaining, workdir, safe_cmd)

            # Single-flight `terraform init` — the only command that
            # writes the shared plugin cache, which Terraform documents
            # as unsafe under concurrent inits (see _init_gate). Taken
            # INSIDE the concurrency permit so the process cap above
            # still bounds everything. Two halves, same order every
            # time: the in-process gate, then the cross-process flock
            # on the cache dir. Each wait is bounded by what is left of
            # this call's own budget, and the budget — and the floor
            # check — are re-derived afterwards: an init that queued
            # here for most of its budget must fail fast exactly like
            # one that queued for the permit, not start and get killed
            # mid-unpack (which is how a half-written package lands in
            # the cache for every later init to symlink to).
            if args[0] == "init":
                init_gate = self._init_gate()
                if init_gate.locked():
                    _LOG.info(
                        "tf init queued behind another init (shared plugin cache): "
                        "workdir=%s",
                        workdir,
                    )
                try:
                    await asyncio.wait_for(init_gate.acquire(), timeout=remaining)
                except asyncio.TimeoutError:
                    return self._init_gate_timeout(
                        "another terraform init in this process", remaining, timeout,
                    )
                held_init_gate = init_gate
                remaining = self._effective_run_timeout(timeout, time.monotonic() - _t0)
                held_cache_lock = await self._lock_plugin_cache(
                    plugin_cache_dir, budget=max(remaining, 0.0),
                )
                if held_cache_lock is None:
                    return self._init_gate_timeout(
                        "another terraform init on the shared plugin cache",
                        remaining, timeout,
                    )
                _queued_s = time.monotonic() - _t0
                remaining = self._effective_run_timeout(timeout, _queued_s)
                if remaining <= _MIN_RUN_SECONDS_AFTER_QUEUE:
                    return self._refuse_to_start(
                        args, timeout, _queued_s, remaining, workdir, safe_cmd,
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
                # New session (setsid) so `terraform` and every provider
                # plugin process it forks (postgresql/rabbitmq/minio/
                # vault/kubernetes, over HashiCorp's go-plugin protocol)
                # land in a process group OF THEIR OWN — never this
                # service's own group. Required for `_kill_process_group`
                # below to be able to kill the WHOLE tree via
                # `os.killpg` without risking hitting this process itself.
                start_new_session=True,
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
                # silently exceeded the concurrency limit. Kills the
                # WHOLE process group (see _kill_process_group), not just
                # `terraform`'s own PID — a bare proc.kill() left orphaned
                # provider-plugin grandchildren running past this point
                # (live 2026-08-18, tenant 72f36de4 — see
                # _kill_process_group's docstring).
                if proc.returncode is None:
                    _kill_process_group(proc)
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
            # Reverse acquisition order: cache flock, in-process gate,
            # then the permit — each step isolated so a raise in one
            # (an flock/close failure on the fd) can never skip the
            # others: the gate and the permit are process-wide, and a
            # stranded one locks every later init in this pod until it
            # restarts.
            try:
                if held_cache_lock is not None:
                    self._unlock_plugin_cache(held_cache_lock)
            finally:
                try:
                    if held_init_gate is not None:
                        held_init_gate.release()
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
        from services.terraformer.src.kube_lease_mutex import LeaseAcquireTimeout

        async with self._lock_for(inputs.tenant_id):
            try:
                async with self._tenant_lease_cm(inputs.tenant_id, effective_timeout):
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
                        # Wipe the credential-laden tfvars file regardless
                        # of success/failure — never leave admin tokens on
                        # disk between runs.
                        self._wipe_tfvars(workdir)
            except LeaseAcquireTimeout as exc:
                # Reuses the exact exit_code=124 sentinel _spawn's own
                # timeout paths already return — grpc_server.py's
                # _terraform_error_code and routes/provisioning.py both
                # already classify that as "ran out of time", so a caller
                # gets the same DEADLINE_EXCEEDED treatment for "another
                # apply is still holding this tenant" as for any other
                # timeout, with zero changes needed at either call site.
                raise TerraformError(
                    "lease_acquire",
                    TerraformResult(exit_code=124, stdout="", stderr=str(exc), outputs={}),
                ) from exc

    async def destroy(self, inputs: TenantInputs, timeout: int | None = None) -> TerraformResult:
        effective_timeout = timeout or self._settings.destroy_timeout_seconds
        from services.terraformer.src.kube_lease_mutex import LeaseAcquireTimeout

        async with self._lock_for(inputs.tenant_id):
            try:
                async with self._tenant_lease_cm(inputs.tenant_id, effective_timeout):
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
                        # Belt-and-braces: if shutil.rmtree didn't fire
                        # (early raise), at least wipe the tfvars file.
                        self._wipe_tfvars(workdir)
            except LeaseAcquireTimeout as exc:
                raise TerraformError(
                    "lease_acquire",
                    TerraformResult(exit_code=124, stdout="", stderr=str(exc), outputs={}),
                ) from exc

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


    def _ensure_workspace_modules_link(self) -> None:
        """Every standalone harness's root module references its module as
        `source = "../../modules/<name>"` — correct in the pneuma-deployments
        checkout layout, but the runner copies only the harness's top-level
        FILES into `<workdir_root>/_<harness>/<env>/`, so from there
        `../../modules` resolves to `<workdir_root>/modules`, which never
        existed (live incident 2026-08-19: every platform-secrets reconcile
        died at init with "Unreadable module directory"). One symlink
        `<workdir_root>/modules -> terraform_modules_root` (the modules tree
        baked into the image) makes the relative source resolve for every
        harness workspace at once, without forking harness main.tf between
        operator-checkout and in-pod layouts."""
        root = self._settings.terraform_workdir_root.resolve()
        root.mkdir(parents=True, exist_ok=True)
        link = root / "modules"
        target = self._settings.terraform_modules_root.resolve()
        if link.is_symlink():
            if link.resolve() == target:
                return
            link.unlink()
        elif link.exists():
            # A real directory here is operator-placed — leave it alone.
            return
        link.symlink_to(target, target_is_directory=True)

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
        self._ensure_workspace_modules_link()
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
        self._ensure_workspace_modules_link()
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

    async def _import_preexisting_platform_resources(self, workdir: Path, env: str) -> None:
        """Platform-resources sibling of `_import_preexisting_resources`
        (see `_IMPORT_ON_EXISTS_RESOURCES` above for the full design
        rationale — LAW: design for N, never for 1). Same best-effort
        import-before-apply convergence, extended to the
        platform-resources workspace's CREATE-ONLY resources:

          pq: role "activepieces_app" already exists (42710)  -- postgres.tf
          Vault check-and-set on infra/inter-service-hmac/<pair> -- inter-service-hmac.tf

        (2026-08-19 defect: a `platform-resources/<env>.tfstate` apply
        killed/retried mid-run left `activepieces_app` created against the
        real Postgres server but absent from state — every re-apply then
        fatally errored on CREATE ROLE instead of converging. Partial
        applies are also now in play for the HMAC pair KV secrets, so
        those are registered too — see `_platform_resources_import_
        entries`.)

        Threads `_platform_resources_extra_env()` (TF_VAR_pg_host/
        pg_port/pg_superuser_password) into every import attempt — unlike
        the tenant module (no provider{} blocks, credentials arrive via
        `_provider_env()` alone), this workspace's postgresql provider IS
        explicitly configured from those TF_VAR_* vars (see the
        standalone harness's `provider "postgresql" { host = var.pg_host
        ... }`). Without them here, EVERY postgresql_role/postgresql_
        database import would fail on a connection/auth error rather than
        a genuine "does not exist" — silently defeating this fix for
        exactly the two resources it exists to cover.
        """
        existing = await self._state_addresses(workdir)
        extra_env = self._platform_resources_extra_env()
        for entry in _platform_resources_import_entries(env):
            if entry.resource_address in existing:
                continue
            result = await self._spawn_once(
                workdir,
                ["import", "-input=false", entry.resource_address, entry.resource_id],
                timeout=60,
                extra_env=extra_env,
                failure_expected=True,
            )
            if result.exit_code == 0:
                _LOG.warning(
                    "tf import: adopted pre-existing %s (%s=%s) into state for "
                    "platform-resources env=%s — re-apply drift recovery",
                    entry.name, entry.resource_address, entry.resource_id, env,
                )
            else:
                _LOG.debug(
                    "tf import: %s (%s=%s) not found for platform-resources env=%s "
                    "— apply will create it",
                    entry.name, entry.resource_address, entry.resource_id, env,
                )

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
            await self._import_preexisting_platform_resources(workdir, env)
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
        self._ensure_workspace_modules_link()
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
