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
import shlex
import shutil
import time
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

# Kubernetes ServiceAccount projection paths — module-level constants
# (not Settings fields) so tests can monkeypatch them directly onto this
# module without threading a new Settings field through every call site.
# Both must exist for _provider_env() to populate KUBE_*: a pod running
# without a mounted SA token has no business reconciling k8s-backed
# tenant resources (ESO SecretStore bindings, RMQ Operator CRDs, ...).
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


class TerraformError(RuntimeError):
    def __init__(self, command: str, result: TerraformResult):
        self.command = command
        self.result = result
        snippet = result.stderr.strip().splitlines()[-20:] if result.stderr else []
        super().__init__(f"terraform {command} failed (exit={result.exit_code}): " + "\n".join(snippet))


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

    def _spawn_queue_budget(self, timeout: int) -> int:
        """Seconds a given `_spawn` call may wait to ACQUIRE a concurrency
        slot before giving up with exit_code=124 — a fraction of that
        call's OWN `timeout`, never a flat constant (see settings.py's
        `spawn_queue_timeout_fraction` docstring for why a flat value
        can't simultaneously suit a 30s read and a 600s apply). Floored
        at 1s so a very short-timeout op still gets one real queue
        attempt instead of a 0s budget that fails before ever trying."""
        return max(1, int(timeout * self._settings.spawn_queue_timeout_fraction))

    def _effective_run_timeout(self, timeout: int, queued_elapsed: float) -> float:
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
        timeout: int,
    ) -> TerraformResult:
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
        sem = self._spawn_semaphore()
        queue_budget = self._spawn_queue_budget(timeout)
        if sem.locked():
            _LOG.info(
                "tf spawn queued (waiting, limit=%d): workdir=%s cmd=%s",
                self._settings.max_concurrent_terraform_runs, workdir, safe_cmd,
            )
        _t0 = time.monotonic()
        try:
            await asyncio.wait_for(sem.acquire(), timeout=queue_budget)
        except asyncio.TimeoutError:
            _LOG.warning(
                "tf spawn queue timeout after %.1fs (budget=%ds, limit=%d): "
                "workdir=%s cmd=%s",
                time.monotonic() - _t0, queue_budget,
                self._settings.max_concurrent_terraform_runs, workdir, safe_cmd,
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

            return TerraformResult(
                exit_code=proc.returncode or 0,
                stdout=stdout_b.decode("utf-8", errors="replace"),
                stderr=stderr_b.decode("utf-8", errors="replace"),
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
        return (
            '# GENERATED by terraform_runner._write_vault_provider_file — do not\n'
            '# hand-edit; rewritten into this workspace on every _ensure_workspace()\n'
            '# call. See terraform_runner.py:_vault_provider_hcl for the full\n'
            '# rationale (static-token expiry incident, 2026-07).\n'
            'provider "vault" {\n'
            '  # LOAD-BEARING — do NOT remove. Without this, the vault provider\n'
            '  # calls auth/token/create even when authenticating via\n'
            '  # auth_login_kubernetes, which 403s under the least-privilege\n'
            '  # `terraformer` OpenBao policy (that policy grants only the KV\n'
            '  # paths this runner touches, not token-management endpoints).\n'
            '  skip_child_token = true\n'
            '\n'
            '  auth_login_kubernetes {\n'
            f'    role  = {json.dumps(s.vault_k8s_auth_role)}\n'
            f'    mount = {json.dumps(s.vault_k8s_auth_mount)}\n'
            '    # file(), not a baked/interpolated value: re-read from disk on\n'
            '    # every plan/apply, so kubelet\'s automatic projected-token\n'
            '    # rotation is picked up for free — no restart, no re-issue.\n'
            '    jwt = file("/var/run/secrets/kubernetes.io/serviceaccount/token")\n'
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

    async def reconcile(self, inputs: TenantInputs, timeout: int | None = None) -> TerraformResult:
        effective_timeout = timeout or self._settings.apply_timeout_seconds
        async with self._lock_for(inputs.tenant_id):
            workdir = await self._ensure_workspace(inputs.tenant_id)
            await self._init(workdir, inputs.tenant_id)
            await self._tfvars_file(workdir, inputs)
            try:
                result = await self._spawn(
                    workdir,
                    ["apply", "-auto-approve", "-no-color", *self._module_var_files(workdir)],
                    timeout=effective_timeout,
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


_runner: TerraformRunner | None = None


def get_runner() -> TerraformRunner:
    global _runner
    if _runner is None:
        _runner = TerraformRunner()
    return _runner
