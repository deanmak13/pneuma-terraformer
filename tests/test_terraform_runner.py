"""Unit tests for TerraformRunner — exercise the subprocess wrapper
without invoking the real terraform CLI.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from services.terraformer.src import terraform_runner as runner_mod
from services.terraformer.src.settings import Settings, get_settings
from services.terraformer.src.terraform_runner import (
    PlatformSecretsInputs,
    TenantInputs,
    TerraformError,
    TerraformResult,
    TerraformRunner,
)


def _stub_inputs() -> TenantInputs:
    return TenantInputs(
        tenant_id="t-001",
        tenant_slug="acme",
        env="tst",
        compliance_profile="standard",
        pooled_namespace="platform-tst",
    )


def _seed_module(modules_root: Path) -> None:
    src = modules_root / "tenant"
    src.mkdir(parents=True)
    (src / "main.tf").write_text('terraform {\n  required_version = ">= 1.5"\n}\n')


@pytest.mark.asyncio
async def test_tf_vars_carry_inputs_and_creds() -> None:
    settings = get_settings()
    _seed_module(settings.terraform_modules_root)
    runner = TerraformRunner(settings)
    inputs = _stub_inputs()

    vars_ = runner._tf_vars(inputs)
    assert vars_["tenant_id"] == "t-001"
    assert vars_["tenant_slug"] == "acme"
    assert vars_["pooled_namespace"] == "platform-tst"
    assert vars_["profile"] == "standard"
    assert "compliance_profile" not in vars_
    assert "hetzner_api_token" not in vars_
    assert "cloudflare_api_token" not in vars_
    assert vars_["postgres_superuser_password"] == "pg-pass-1234"


@pytest.mark.asyncio
async def test_tf_vars_include_provider_tokens_when_hetzner_enabled(
    tmp_path: Path,
) -> None:
    settings = Settings(
        tenant_infra_provider="hetzner",
        hetzner_api_token="h" * 40,
        cloudflare_api_token="c" * 40,
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/bin/true",
    )
    _seed_module(settings.terraform_modules_root)
    runner = TerraformRunner(settings)

    vars_ = runner._tf_vars(_stub_inputs())

    assert vars_["hetzner_api_token"] == "h" * 40
    assert vars_["cloudflare_api_token"] == "c" * 40


@pytest.mark.asyncio
async def test_backend_config_keys_per_tenant() -> None:
    settings = get_settings()
    _seed_module(settings.terraform_modules_root)
    runner = TerraformRunner(settings)

    cfg_a = runner._backend_config("tenant-a")
    cfg_b = runner._backend_config("tenant-b")
    assert cfg_a["key"] == "tenants/tenant-a.tfstate"
    assert cfg_b["key"] == "tenants/tenant-b.tfstate"
    assert cfg_a["bucket"] == "pneuma-tf-state"


@pytest.mark.asyncio
async def test_workspace_copied_from_modules_root() -> None:
    settings = get_settings()
    _seed_module(settings.terraform_modules_root)
    runner = TerraformRunner(settings)

    workdir = await runner._ensure_workspace("t-001")
    assert workdir.exists()
    assert (workdir / "main.tf").exists()


@pytest.mark.asyncio
async def test_reconcile_happy_path() -> None:
    settings = get_settings()
    _seed_module(settings.terraform_modules_root)
    runner = TerraformRunner(settings)

    fake_init = TerraformResult(exit_code=0, stdout="initialized", stderr="", outputs={})
    fake_apply = TerraformResult(exit_code=0, stdout="apply complete", stderr="", outputs={})
    fake_output = TerraformResult(
        exit_code=0,
        stdout=json.dumps({"vhost": {"value": "/acme-tst"}, "bucket": {"value": "acme-tst-media"}}),
        stderr="",
        outputs={},
    )
    spawn_results = iter([fake_init, fake_apply, fake_output])

    async def _fake_spawn(workdir, args, timeout):  # noqa: ARG001
        return next(spawn_results)

    with patch.object(runner, "_spawn", AsyncMock(side_effect=_fake_spawn)):
        result = await runner.reconcile(_stub_inputs())

    assert result.exit_code == 0
    assert result.outputs["vhost"] == "/acme-tst"
    assert result.outputs["bucket"] == "acme-tst-media"


@pytest.mark.asyncio
async def test_reconcile_propagates_apply_failure() -> None:
    settings = get_settings()
    _seed_module(settings.terraform_modules_root)
    runner = TerraformRunner(settings)

    fake_init = TerraformResult(exit_code=0, stdout="", stderr="", outputs={})
    fake_apply = TerraformResult(
        exit_code=1,
        stdout="",
        stderr="Error: Hetzner API rate-limited",
        outputs={},
    )
    spawn_results = iter([fake_init, fake_apply])

    async def _fake_spawn(workdir, args, timeout):  # noqa: ARG001
        return next(spawn_results)

    with patch.object(runner, "_spawn", AsyncMock(side_effect=_fake_spawn)):
        with pytest.raises(TerraformError) as exc_info:
            await runner.reconcile(_stub_inputs())

    assert exc_info.value.command == "apply"
    assert "rate-limited" in exc_info.value.result.stderr


@pytest.mark.asyncio
async def test_destroy_removes_workspace() -> None:
    settings = get_settings()
    _seed_module(settings.terraform_modules_root)
    runner = TerraformRunner(settings)

    fake_init = TerraformResult(exit_code=0, stdout="", stderr="", outputs={})
    fake_destroy = TerraformResult(exit_code=0, stdout="destroy complete", stderr="", outputs={})
    spawn_results = iter([fake_init, fake_destroy])

    async def _fake_spawn(workdir, args, timeout):  # noqa: ARG001
        return next(spawn_results)

    with patch.object(runner, "_spawn", AsyncMock(side_effect=_fake_spawn)):
        await runner.destroy(_stub_inputs())

    assert not runner._workspace_dir("t-001").exists()


@pytest.mark.asyncio
async def test_state_when_workspace_absent() -> None:
    settings = get_settings()
    _seed_module(settings.terraform_modules_root)
    runner = TerraformRunner(settings)

    payload = await runner.state("never-created")
    assert payload == {"exists": False, "outputs": {}}


@pytest.mark.asyncio
async def test_per_tenant_lock_serialises_same_tenant() -> None:
    settings = get_settings()
    _seed_module(settings.terraform_modules_root)
    runner = TerraformRunner(settings)

    lock_a = runner._lock_for("t-001")
    lock_b = runner._lock_for("t-001")
    lock_c = runner._lock_for("t-002")
    assert lock_a is lock_b
    assert lock_a is not lock_c


# ---------------------------------------------------------------------------
# Security-hardening regression guards (#805 review fix)
# ---------------------------------------------------------------------------


def test_workspace_dir_blocks_path_traversal() -> None:
    """`_workspace_dir` must refuse a tenant_id that resolves outside
    the configured workdir root — defence-in-depth on top of the
    route-level Pydantic pattern guard."""
    settings = get_settings()
    _seed_module(settings.terraform_modules_root)
    runner = TerraformRunner(settings)

    with pytest.raises(ValueError, match="escapes workdir root"):
        runner._workspace_dir("../../etc")
    with pytest.raises(ValueError, match="escapes workdir root"):
        runner._workspace_dir("../escape")


def test_workspace_dir_accepts_valid_tenant_id() -> None:
    """Sanity: a normal tenant_id resolves inside the workdir root."""
    settings = get_settings()
    _seed_module(settings.terraform_modules_root)
    runner = TerraformRunner(settings)

    path = runner._workspace_dir("t-001")
    assert str(path).startswith(str(settings.terraform_workdir_root.resolve()))


def test_scrub_credentials_redacts_known_secrets() -> None:
    """Every known credential value in stdout/stderr must be replaced
    with <REDACTED> before crossing the HTTP boundary."""
    from services.terraformer.src.terraform_runner import scrub_credentials

    settings = get_settings()
    secret = settings.openbao_admin_token
    leaky = f"Error: failed to provision: invalid token={secret} response=403"
    assert secret not in scrub_credentials(leaky, settings)
    assert "<REDACTED>" in scrub_credentials(leaky, settings)


def test_scrub_credentials_idempotent_and_safe_on_empty() -> None:
    """Idempotent + handles empty/short strings without IndexError."""
    from services.terraformer.src.terraform_runner import scrub_credentials

    settings = get_settings()
    assert scrub_credentials("", settings) == ""
    once = scrub_credentials("blob with secret", settings)
    assert scrub_credentials(once, settings) == once


def test_wipe_tfvars_removes_credential_file(tmp_path: Path) -> None:
    """`_wipe_tfvars` deletes the credential-laden tfvars file. Called
    in the `finally` block of reconcile/destroy so admin tokens never
    persist between runs."""
    settings = get_settings()
    runner = TerraformRunner(settings)
    workdir = tmp_path / "ws"
    workdir.mkdir()
    tfvars = workdir / "terraform.auto.tfvars.json"
    tfvars.write_text('{"hetzner_api_token": "REAL-TOKEN"}')

    runner._wipe_tfvars(workdir)
    assert not tfvars.exists()


def test_wipe_tfvars_idempotent(tmp_path: Path) -> None:
    """No error if the file is already gone."""
    settings = get_settings()
    runner = TerraformRunner(settings)
    workdir = tmp_path / "ws"
    workdir.mkdir()
    runner._wipe_tfvars(workdir)  # absent
    runner._wipe_tfvars(workdir)  # still absent — no raise


@pytest.mark.asyncio
async def test_state_returns_unbootstrapped_sentinel_without_init(
    tmp_path: Path,
) -> None:
    """`state()` must NOT call _init() on an un-bootstrapped workspace.
    Read-path leaking admin tokens through a provider-error response
    was the pre-fix CRITICAL — guard against regression."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/usr/local/bin/terraform",
    )
    settings.terraform_workdir_root.mkdir(parents=True)
    (settings.terraform_workdir_root / "t-state-001").mkdir()

    runner = TerraformRunner(settings)
    # Patch _init to raise — proves state() didn't call it.
    with patch.object(runner, "_init", side_effect=AssertionError("init must NOT run")):
        result = await runner.state("t-state-001")

    assert result == {"exists": True, "outputs": {}, "bootstrapped": False}


@pytest.mark.asyncio
async def test_state_returns_not_exists_for_missing_workspace(
    tmp_path: Path,
) -> None:
    """If the workspace dir doesn't exist at all, state() returns the
    not-exists sentinel without trying to init/output."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/usr/local/bin/terraform",
    )
    settings.terraform_workdir_root.mkdir(parents=True)
    runner = TerraformRunner(settings)

    result = await runner.state("never-bootstrapped")
    assert result == {"exists": False, "outputs": {}}


@pytest.mark.asyncio
async def test_spawn_redacts_backend_config_secrets_in_log(
    tmp_path: Path, caplog,
) -> None:
    """`_spawn` MUST redact ``-backend-config secret_key=<v>`` /
    ``access_key=<v>`` pairs from its INFO log line. Pre-fix the log
    leaked MinIO admin creds on every init."""
    import logging

    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/bin/true",
        tf_state_backend_access_key="LEAKY-ACCESS-KEY-VALUE",
        tf_state_backend_secret_key="LEAKY-SECRET-KEY-VALUE",
    )
    settings.terraform_workdir_root.mkdir(parents=True)
    workdir = settings.terraform_workdir_root / "ws"
    workdir.mkdir()
    runner = TerraformRunner(settings)
    caplog.set_level(logging.INFO, logger="terraformer.terraform")
    await runner._spawn(
        workdir,
        ["init", "-backend-config", "secret_key=LEAKY-SECRET-KEY-VALUE",
                 "-backend-config", "access_key=LEAKY-ACCESS-KEY-VALUE",
                 "-backend-config", "bucket=pneuma-tf-state"],
        timeout=5,
    )
    log_text = " ".join(r.getMessage() for r in caplog.records)
    assert "LEAKY-SECRET-KEY-VALUE" not in log_text
    assert "LEAKY-ACCESS-KEY-VALUE" not in log_text
    assert "<REDACTED>" in log_text
    # Non-secret backend-config still visible
    assert "bucket=pneuma-tf-state" in log_text


@pytest.mark.asyncio
async def test_init_raises_terraform_error_on_nonzero(tmp_path: Path) -> None:
    """`_init` raises TerraformError when the spawned terraform exits
    non-zero — keeps reconcile/destroy on the failure path."""
    settings = get_settings()
    runner = TerraformRunner(settings)
    workdir = tmp_path / "ws"
    workdir.mkdir()

    bad = TerraformResult(exit_code=1, stdout="", stderr="boom", outputs={})
    with patch.object(runner, "_spawn", AsyncMock(return_value=bad)):
        with pytest.raises(TerraformError) as exc_info:
            await runner._init(workdir, "t-1")
    assert exc_info.value.command == "init"
    assert exc_info.value.result.exit_code == 1


@pytest.mark.asyncio
async def test_reconcile_wipes_tfvars_on_success(tmp_path: Path) -> None:
    """`reconcile` MUST wipe terraform.auto.tfvars.json in the finally
    block so admin tokens don't persist on disk between runs."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/bin/true",
    )
    settings.terraform_modules_root.mkdir(parents=True)
    _seed_module(settings.terraform_modules_root)
    settings.terraform_workdir_root.mkdir(parents=True)

    runner = TerraformRunner(settings)
    ok = TerraformResult(exit_code=0, stdout="ok", stderr="", outputs={})

    with patch.object(runner, "_spawn", AsyncMock(return_value=ok)):
        with patch.object(runner, "_output_json", AsyncMock(return_value={})):
            await runner.reconcile(_stub_inputs())

    tfvars = settings.terraform_workdir_root / "t-001" / "terraform.auto.tfvars.json"
    assert not tfvars.exists(), "tfvars file must be wiped after reconcile"


@pytest.mark.asyncio
async def test_reconcile_wipes_tfvars_on_apply_failure(tmp_path: Path) -> None:
    """Even when ``apply`` fails the tfvars file is still wiped. Pre-fix
    a failed apply would leave admin tokens on disk indefinitely."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/bin/true",
    )
    settings.terraform_modules_root.mkdir(parents=True)
    _seed_module(settings.terraform_modules_root)
    settings.terraform_workdir_root.mkdir(parents=True)

    runner = TerraformRunner(settings)
    init_ok = TerraformResult(exit_code=0, stdout="init-ok", stderr="", outputs={})
    apply_fail = TerraformResult(exit_code=1, stdout="", stderr="apply boom", outputs={})

    spawn_results = [init_ok, apply_fail]
    async def fake_spawn(*args, **kwargs):
        return spawn_results.pop(0)

    with patch.object(runner, "_spawn", side_effect=fake_spawn):
        with pytest.raises(TerraformError):
            await runner.reconcile(_stub_inputs())

    tfvars = settings.terraform_workdir_root / "t-001" / "terraform.auto.tfvars.json"
    assert not tfvars.exists(), "tfvars must be wiped even on apply failure"


@pytest.mark.asyncio
async def test_output_json_parses_outputs(tmp_path: Path) -> None:
    """`_output_json` extracts the value key from each output. Smoke
    test for the happy path."""
    settings = get_settings()
    runner = TerraformRunner(settings)
    workdir = tmp_path / "ws"
    workdir.mkdir()

    raw = json.dumps({
        "tenant_db_url": {"value": "postgres://...", "type": "string"},
        "tenant_vhost": {"value": "/tenant-001", "type": "string"},
    })
    result = TerraformResult(exit_code=0, stdout=raw, stderr="", outputs={})
    with patch.object(runner, "_spawn", AsyncMock(return_value=result)):
        out = await runner._output_json(workdir)
    assert out == {"tenant_db_url": "postgres://...", "tenant_vhost": "/tenant-001"}


# ---------------------------------------------------------------------------
# P3 — module bake + provider mirror + S3 backend + var-files + provider env
# ---------------------------------------------------------------------------


def test_backend_config_uses_verified_s3_backend_keys_not_deprecated() -> None:
    """gate-5 finding (verified locally against hashicorp/terraform:1.9):
    the S3 backend deprecates top-level endpoint/force_path_style in
    favour of endpoints.s3/use_path_style, and separately requires
    skip_requesting_account_id=true against a non-AWS S3-compatible
    endpoint (MinIO) or init fails outright with 'AWS account ID not
    previously found' regardless of endpoint key naming."""
    settings = get_settings()
    runner = TerraformRunner(settings)

    cfg = runner._backend_config("t-001")
    assert "endpoint" not in cfg
    assert "force_path_style" not in cfg
    assert cfg["endpoints"] == f'{{s3="{settings.tf_state_backend_endpoint}"}}'
    assert cfg["use_path_style"] == "true"
    assert cfg["skip_requesting_account_id"] == "true"


def test_platform_secrets_backend_config_uses_same_verified_s3_keys() -> None:
    """The platform-secrets workspace's backend config must not drift
    onto a different (untested) key shape than the tenant workspace."""
    settings = get_settings()
    runner = TerraformRunner(settings)

    cfg = runner._platform_secrets_backend_config("tst")
    assert "endpoint" not in cfg
    assert "force_path_style" not in cfg
    assert cfg["endpoints"] == f'{{s3="{settings.tf_state_backend_endpoint}"}}'
    assert cfg["use_path_style"] == "true"
    assert cfg["skip_requesting_account_id"] == "true"


@pytest.mark.asyncio
async def test_ensure_workspace_writes_backend_tf() -> None:
    """The tenant module ships with no backend block (reusable-module
    convention) — _ensure_workspace must stub one so -backend-config args
    passed at init time have something to bind to."""
    settings = get_settings()
    _seed_module(settings.terraform_modules_root)
    runner = TerraformRunner(settings)

    workdir = await runner._ensure_workspace("t-010")
    backend_tf = workdir / "backend.tf"
    assert backend_tf.exists()
    assert 'backend "s3"' in backend_tf.read_text()


def test_module_var_files_absent_returns_empty_list(tmp_path: Path) -> None:
    settings = get_settings()
    runner = TerraformRunner(settings)
    workdir = tmp_path / "ws-no-generated"
    workdir.mkdir()

    assert runner._module_var_files(workdir) == []


def test_module_var_files_present_returns_sorted_var_file_args(tmp_path: Path) -> None:
    settings = get_settings()
    runner = TerraformRunner(settings)
    workdir = tmp_path / "ws-generated"
    generated = workdir / "_generated"
    generated.mkdir(parents=True)
    (generated / "b_bus_topology.auto.tfvars.json").write_text("{}")
    (generated / "a_bus_topology.auto.tfvars.json").write_text("{}")

    args = runner._module_var_files(workdir)
    assert args == [
        "-var-file", "_generated/a_bus_topology.auto.tfvars.json",
        "-var-file", "_generated/b_bus_topology.auto.tfvars.json",
    ]


@pytest.mark.asyncio
async def test_reconcile_apply_argv_includes_module_var_files() -> None:
    settings = get_settings()
    _seed_module(settings.terraform_modules_root)
    runner = TerraformRunner(settings)

    workdir = await runner._ensure_workspace("t-011")
    generated = workdir / "_generated"
    generated.mkdir(parents=True)
    (generated / "bus_topology.auto.tfvars.json").write_text("{}")

    captured_args: list[list[str]] = []
    fake_init = TerraformResult(exit_code=0, stdout="", stderr="", outputs={})
    fake_apply = TerraformResult(exit_code=0, stdout="", stderr="", outputs={})
    fake_output = TerraformResult(exit_code=0, stdout="{}", stderr="", outputs={})
    spawn_results = iter([fake_init, fake_apply, fake_output])

    async def _fake_spawn(wd, args, timeout):  # noqa: ARG001
        captured_args.append(args)
        return next(spawn_results)

    with patch.object(runner, "_ensure_workspace", AsyncMock(return_value=workdir)):
        with patch.object(runner, "_spawn", AsyncMock(side_effect=_fake_spawn)):
            await runner.reconcile(_stub_inputs())

    apply_args = captured_args[1]
    assert "-var-file" in apply_args
    assert "_generated/bus_topology.auto.tfvars.json" in apply_args


@pytest.mark.asyncio
async def test_destroy_argv_includes_module_var_files() -> None:
    settings = get_settings()
    _seed_module(settings.terraform_modules_root)
    runner = TerraformRunner(settings)

    workdir = await runner._ensure_workspace("t-011b")
    generated = workdir / "_generated"
    generated.mkdir(parents=True)
    (generated / "bus_topology.auto.tfvars.json").write_text("{}")

    captured_args: list[list[str]] = []
    fake_init = TerraformResult(exit_code=0, stdout="", stderr="", outputs={})
    fake_destroy = TerraformResult(exit_code=0, stdout="destroy complete", stderr="", outputs={})
    spawn_results = iter([fake_init, fake_destroy])

    async def _fake_spawn(wd, args, timeout):  # noqa: ARG001
        captured_args.append(args)
        return next(spawn_results)

    with patch.object(runner, "_ensure_workspace", AsyncMock(return_value=workdir)):
        with patch.object(runner, "_spawn", AsyncMock(side_effect=_fake_spawn)):
            await runner.destroy(_stub_inputs())

    destroy_args = captured_args[1]
    assert "-var-file" in destroy_args
    assert "_generated/bus_topology.auto.tfvars.json" in destroy_args


def test_wipe_tfvars_logs_warning_on_os_error(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-FileNotFoundError OSError (e.g. permission denied) during
    unlink must be logged as a warning, not raised — _wipe_tfvars runs in
    a `finally` block and must never itself fail the reconcile/destroy
    call it's cleaning up after."""
    import logging

    settings = get_settings()
    runner = TerraformRunner(settings)
    workdir = tmp_path / "ws"
    workdir.mkdir()
    (workdir / "terraform.auto.tfvars.json").write_text("{}")

    def _raise_permission_error(self):  # noqa: ANN001
        raise PermissionError("permission denied")

    monkeypatch.setattr(Path, "unlink", _raise_permission_error)
    caplog.set_level(logging.WARNING, logger="terraformer.terraform")

    runner._wipe_tfvars(workdir)  # must not raise

    assert any("could not wipe tfvars file" in r.getMessage() for r in caplog.records)


def test_wipe_tfvars_removes_root_files_but_not_generated(tmp_path: Path) -> None:
    settings = get_settings()
    runner = TerraformRunner(settings)
    workdir = tmp_path / "ws"
    workdir.mkdir()
    (workdir / "terraform.tfvars.json").write_text("{}")
    (workdir / "terraform.auto.tfvars.json").write_text("{}")
    (workdir / "extra.auto.tfvars.json").write_text("{}")
    generated = workdir / "_generated"
    generated.mkdir()
    (generated / "bus_topology.auto.tfvars.json").write_text("{}")

    runner._wipe_tfvars(workdir)

    assert not (workdir / "terraform.tfvars.json").exists()
    assert not (workdir / "terraform.auto.tfvars.json").exists()
    assert not (workdir / "extra.auto.tfvars.json").exists()
    assert (generated / "bus_topology.auto.tfvars.json").exists(), (
        "_generated/ topology data must survive _wipe_tfvars"
    )


@pytest.mark.asyncio
async def test_reconcile_platform_secrets_wipes_terraform_tfvars_json(tmp_path: Path) -> None:
    """Regression (8a): the platform-secrets path writes
    terraform.tfvars.json (not the .auto. variant the tenant path uses)
    — _wipe_tfvars must cover both filenames so admin tokens don't
    persist on disk between reconciles."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_standalone_root=tmp_path / "standalone",
        terraform_binary="/bin/true",
    )
    standalone_src = settings.terraform_standalone_root / "platform-secrets-apply"
    standalone_src.mkdir(parents=True)
    (standalone_src / "main.tf").write_text("# stub\n")
    settings.terraform_workdir_root.mkdir(parents=True)

    runner = TerraformRunner(settings)
    ok = TerraformResult(exit_code=0, stdout="ok", stderr="", outputs={})

    with patch.object(runner, "_spawn", AsyncMock(return_value=ok)):
        with patch.object(runner, "_output_json", AsyncMock(return_value={})):
            await runner.reconcile_platform_secrets(PlatformSecretsInputs(env="tst"))

    tfvars = settings.terraform_workdir_root / "_platform" / "tst" / "terraform.tfvars.json"
    assert not tfvars.exists(), "terraform.tfvars.json must be wiped after platform-secrets reconcile"


def test_provider_env_includes_five_credential_vars() -> None:
    settings = get_settings()
    runner = TerraformRunner(settings)

    env = runner._provider_env()
    assert env["PGPASSWORD"] == settings.postgres_superuser_password
    assert env["RABBITMQ_PASSWORD"] == settings.rabbitmq_admin_password
    assert env["MINIO_USER"] == settings.tf_state_backend_access_key
    assert env["MINIO_PASSWORD"] == settings.minio_admin_password
    assert env["VAULT_TOKEN"] == settings.openbao_admin_token


def test_provider_env_excludes_kube_vars_when_sa_files_absent(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setattr(runner_mod, "_KUBE_SA_TOKEN_PATH", tmp_path / "absent-token")
    monkeypatch.setattr(runner_mod, "_KUBE_SA_CA_CERT_PATH", tmp_path / "absent-ca.crt")

    settings = get_settings()
    runner = TerraformRunner(settings)
    env = runner._provider_env()

    assert "KUBE_HOST" not in env
    assert "KUBE_TOKEN" not in env
    assert "KUBE_CLUSTER_CA_CERT_DATA" not in env


def test_provider_env_includes_kube_vars_only_when_sa_files_present(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    token_path = tmp_path / "sa-token"
    ca_path = tmp_path / "sa-ca.crt"
    token_path.write_text("sa-jwt-token\n")
    ca_path.write_text("-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----\n")
    monkeypatch.setattr(runner_mod, "_KUBE_SA_TOKEN_PATH", token_path)
    monkeypatch.setattr(runner_mod, "_KUBE_SA_CA_CERT_PATH", ca_path)
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    monkeypatch.setenv("KUBERNETES_SERVICE_PORT_HTTPS", "443")

    settings = get_settings()
    runner = TerraformRunner(settings)
    env = runner._provider_env()

    assert env["KUBE_HOST"] == "https://10.0.0.1:443"
    assert env["KUBE_TOKEN"] == "sa-jwt-token"
    assert "BEGIN CERTIFICATE" in env["KUBE_CLUSTER_CA_CERT_DATA"]


@pytest.mark.asyncio
async def test_spawn_sets_tf_cli_config_file_and_provider_env(tmp_path: Path) -> None:
    """`_spawn` must set TF_CLI_CONFIG_FILE (wiring the baked provider
    mirror) and merge _provider_env() onto the subprocess environment —
    proves the wiring end-to-end without invoking real terraform."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/usr/bin/env",
        tf_cli_config_file=str(tmp_path / "custom-cli.tfrc"),
    )
    settings.terraform_workdir_root.mkdir(parents=True)
    workdir = settings.terraform_workdir_root / "ws"
    workdir.mkdir()
    runner = TerraformRunner(settings)

    captured_env: dict[str, str] = {}
    orig_create_subprocess_exec = __import__("asyncio").create_subprocess_exec

    async def _capturing_exec(*args, **kwargs):
        captured_env.update(kwargs.get("env") or {})
        return await orig_create_subprocess_exec(*args, **kwargs)

    with patch("asyncio.create_subprocess_exec", side_effect=_capturing_exec):
        await runner._spawn(workdir, ["--version"], timeout=5)

    assert captured_env["TF_CLI_CONFIG_FILE"] == str(tmp_path / "custom-cli.tfrc")
    assert captured_env["PGPASSWORD"] == settings.postgres_superuser_password
    assert captured_env["VAULT_TOKEN"] == settings.openbao_admin_token


@pytest.mark.asyncio
async def test_reconcile_uses_explicit_timeout_over_settings_default() -> None:
    settings = get_settings()
    _seed_module(settings.terraform_modules_root)
    runner = TerraformRunner(settings)

    captured_timeouts: list[int] = []
    fake = TerraformResult(exit_code=0, stdout="{}", stderr="", outputs={})

    async def _fake_spawn(workdir, args, timeout):  # noqa: ARG001
        captured_timeouts.append(timeout)
        return fake

    with patch.object(runner, "_spawn", AsyncMock(side_effect=_fake_spawn)):
        with patch.object(runner, "_output_json", AsyncMock(return_value={})):
            await runner.reconcile(_stub_inputs(), timeout=42)

    assert 42 in captured_timeouts


@pytest.mark.asyncio
async def test_reconcile_falls_back_to_settings_apply_timeout_when_none() -> None:
    settings = get_settings()
    _seed_module(settings.terraform_modules_root)
    runner = TerraformRunner(settings)

    captured_timeouts: list[int] = []
    fake = TerraformResult(exit_code=0, stdout="{}", stderr="", outputs={})

    async def _fake_spawn(workdir, args, timeout):  # noqa: ARG001
        captured_timeouts.append(timeout)
        return fake

    with patch.object(runner, "_spawn", AsyncMock(side_effect=_fake_spawn)):
        with patch.object(runner, "_output_json", AsyncMock(return_value={})):
            await runner.reconcile(_stub_inputs())

    assert settings.apply_timeout_seconds in captured_timeouts


@pytest.mark.asyncio
async def test_destroy_uses_explicit_timeout_over_settings_default() -> None:
    settings = get_settings()
    _seed_module(settings.terraform_modules_root)
    runner = TerraformRunner(settings)

    captured_timeouts: list[int] = []
    fake = TerraformResult(exit_code=0, stdout="destroy complete", stderr="", outputs={})

    async def _fake_spawn(workdir, args, timeout):  # noqa: ARG001
        captured_timeouts.append(timeout)
        return fake

    with patch.object(runner, "_spawn", AsyncMock(side_effect=_fake_spawn)):
        await runner.destroy(_stub_inputs(), timeout=17)

    assert 17 in captured_timeouts
