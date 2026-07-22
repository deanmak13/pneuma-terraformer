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
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from services.terraformer.src.settings import Settings, get_settings

_LOG = logging.getLogger("terraformer.terraform")

_TENANT_MODULE = "tenant"

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
    "openbao_admin_token",
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

    def _lock_for(self, tenant_id: str) -> asyncio.Lock:
        if tenant_id not in self._locks:
            self._locks[tenant_id] = asyncio.Lock()
        return self._locks[tenant_id]

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
            "openbao_admin_token": s.openbao_admin_token,
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
        _LOG.info(
            "tf spawn: workdir=%s cmd=%s",
            workdir,
            shlex.join([self._settings.terraform_binary, *safe_args]),
        )

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

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=str(workdir),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        try:
            stdout_b, stderr_b = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return TerraformResult(
                exit_code=124,
                stdout="",
                stderr=f"terraform {args[0]} timed out after {timeout}s",
                outputs={},
            )

        return TerraformResult(
            exit_code=proc.returncode or 0,
            stdout=stdout_b.decode("utf-8", errors="replace"),
            stderr=stderr_b.decode("utf-8", errors="replace"),
            outputs={},
        )

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
            "VAULT_TOKEN": s.openbao_admin_token,
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
