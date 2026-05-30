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
import shlex
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from services.terraformer.src.settings import Settings, get_settings

_LOG = logging.getLogger("terraformer.terraform")

_TENANT_MODULE = "tenant"


@dataclass(frozen=True)
class TenantInputs:
    tenant_id: str
    tenant_slug: str
    env: str
    compliance_profile: str
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
        s = self._settings
        return {
            "bucket": s.tf_state_backend_bucket,
            "key": f"tenants/{tenant_id}.tfstate",
            "region": s.tf_state_backend_region,
            "endpoint": s.tf_state_backend_endpoint,
            "access_key": s.tf_state_backend_access_key,
            "secret_key": s.tf_state_backend_secret_key,
            "force_path_style": "true",
            "skip_credentials_validation": "true",
            "skip_region_validation": "true",
            "skip_metadata_api_check": "true",
        }

    def _tf_vars(self, inputs: TenantInputs) -> dict[str, str]:
        s = self._settings
        return {
            "tenant_id": inputs.tenant_id,
            "tenant_slug": inputs.tenant_slug,
            "env": inputs.env,
            "compliance_profile": inputs.compliance_profile,
            "pooled_namespace": inputs.pooled_namespace,
            "hetzner_api_token": s.hetzner_api_token,
            "cloudflare_api_token": s.cloudflare_api_token,
            "postgres_superuser_password": s.postgres_superuser_password,
            "rabbitmq_admin_password": s.rabbitmq_admin_password,
            "minio_admin_password": s.minio_admin_password,
            "openbao_admin_token": s.openbao_admin_token,
        }

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

        import os

        env = os.environ.copy()
        env["TF_IN_AUTOMATION"] = "1"
        env["TF_INPUT"] = "0"
        env["AWS_ACCESS_KEY_ID"] = self._settings.tf_state_backend_access_key
        env["AWS_SECRET_ACCESS_KEY"] = self._settings.tf_state_backend_secret_key

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
        """Delete ``terraform.auto.tfvars.json`` between runs. The file
        embeds every admin token in plaintext; leaving it on disk past
        the apply means a future workspace read (or pod compromise)
        exfiltrates the full credential set. Idempotent — silent if the
        file is already gone."""
        path = workdir / "terraform.auto.tfvars.json"
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        except OSError as exc:
            _LOG.warning("could not wipe tfvars file %s: %s", path, exc)

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

    async def reconcile(self, inputs: TenantInputs) -> TerraformResult:
        async with self._lock_for(inputs.tenant_id):
            workdir = await self._ensure_workspace(inputs.tenant_id)
            await self._init(workdir, inputs.tenant_id)
            await self._tfvars_file(workdir, inputs)
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
                # Wipe the credential-laden tfvars file regardless of
                # success/failure — never leave admin tokens on disk
                # between runs.
                self._wipe_tfvars(workdir)

    async def destroy(self, inputs: TenantInputs) -> TerraformResult:
        async with self._lock_for(inputs.tenant_id):
            workdir = await self._ensure_workspace(inputs.tenant_id)
            await self._init(workdir, inputs.tenant_id)
            await self._tfvars_file(workdir, inputs)
            try:
                result = await self._spawn(
                    workdir,
                    ["destroy", "-auto-approve", "-no-color"],
                    timeout=self._settings.destroy_timeout_seconds,
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
        s = self._settings
        return {
            "bucket": s.tf_state_backend_bucket,
            "key": f"platform-secrets/{env}.tfstate",
            "region": s.tf_state_backend_region,
            "endpoint": s.tf_state_backend_endpoint,
            "access_key": s.tf_state_backend_access_key,
            "secret_key": s.tf_state_backend_secret_key,
            "force_path_style": "true",
            "skip_credentials_validation": "true",
            "skip_region_validation": "true",
            "skip_metadata_api_check": "true",
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


_runner: TerraformRunner | None = None


def get_runner() -> TerraformRunner:
    global _runner
    if _runner is None:
        _runner = TerraformRunner()
    return _runner
