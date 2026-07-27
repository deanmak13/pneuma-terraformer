"""Unit tests for TerraformRunner — exercise the subprocess wrapper
without invoking the real terraform CLI.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from services.terraformer.src import terraform_runner as runner_mod
from services.terraformer.src.settings import Settings, get_settings
from services.terraformer.src.terraform_runner import (
    PlatformBusTopologyInputs,
    PlatformSecretsInputs,
    TenantInputs,
    TerraformError,
    TerraformResult,
    TerraformRunner,
)


def _stub_inputs(compliance_profile: str | None = "gdpr-special-uk") -> TenantInputs:
    return TenantInputs(
        tenant_id="t-001",
        tenant_slug="acme",
        env="tst",
        compliance_profile=compliance_profile,
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
    assert vars_["profile"] == "gdpr-special-uk"
    assert "compliance_profile" not in vars_
    assert "hetzner_api_token" not in vars_
    assert "cloudflare_api_token" not in vars_
    assert vars_["postgres_superuser_password"] == "pg-pass-1234"


@pytest.mark.asyncio
@pytest.mark.parametrize("raw_profile", [None, "", "standard", "STANDARD", "  standard  "])
async def test_tf_vars_normalizes_non_regulated_sentinels_to_null_profile(
    raw_profile: str | None,
) -> None:
    """Terraform's tenant module (infrastructure/terraform/modules/tenant/
    variables.tf) validates `var.profile == null || contains(["gdpr-
    special-uk", "fca-uk"], var.profile)` — there is no "standard" value.
    None/""/"standard" (any case, whitespace) must all collapse to a real
    `null` in the var-file, never the literal string "standard" — this is
    the regression guard for the tenant-provisioning-fails-on-standard-
    tenants incident (canary blocker #3)."""
    settings = get_settings()
    _seed_module(settings.terraform_modules_root)
    runner = TerraformRunner(settings)
    inputs = _stub_inputs(compliance_profile=raw_profile)

    vars_ = runner._tf_vars(inputs)

    assert vars_["profile"] is None


@pytest.mark.asyncio
async def test_tf_vars_preserves_regulated_profile_value() -> None:
    settings = get_settings()
    _seed_module(settings.terraform_modules_root)
    runner = TerraformRunner(settings)
    inputs = _stub_inputs(compliance_profile="fca-uk")

    vars_ = runner._tf_vars(inputs)

    assert vars_["profile"] == "fca-uk"


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


def test_backend_config_is_flat_primitives_only_no_secrets_no_endpoints() -> None:
    """Review findings 1+2 on the P3 PR: CLI -backend-config values are
    literal strings (cty.StringVal), never HCL — the S3 backend's
    object-typed `endpoints` attribute CANNOT be set via CLI (an inline
    HCL literal fails type conversion at init; hashicorp/terraform#34616,
    #36911), and access_key/secret_key in the dict would land in argv
    (/proc/<pid>/cmdline-visible) while being strictly redundant with the
    AWS_* env vars _spawn already exports. Both travel env-only now
    (AWS_ENDPOINT_URL_S3 / AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY).
    The dict keeps only flat scalars — verified against terraform 1.9:
    use_path_style (force_path_style is deprecated) and
    skip_requesting_account_id=true (required against MinIO or the AWS
    account-ID lookup 403s init outright)."""
    settings = get_settings()
    runner = TerraformRunner(settings)

    cfg = runner._backend_config("t-001")
    assert "endpoints" not in cfg
    assert "endpoint" not in cfg
    assert "access_key" not in cfg
    assert "secret_key" not in cfg
    assert "force_path_style" not in cfg
    assert cfg["bucket"] == settings.tf_state_backend_bucket
    assert cfg["key"] == "tenants/t-001.tfstate"
    assert cfg["region"] == settings.tf_state_backend_region
    assert cfg["use_path_style"] == "true"
    assert cfg["skip_credentials_validation"] == "true"
    assert cfg["skip_requesting_account_id"] == "true"


def test_platform_secrets_backend_config_matches_tenant_key_shape() -> None:
    """The platform-secrets workspace's backend config must not drift
    onto a different (untested) key shape than the tenant workspace —
    same flat-primitives-only rule, same no-secrets/no-endpoints rule."""
    settings = get_settings()
    runner = TerraformRunner(settings)

    cfg = runner._platform_secrets_backend_config("tst")
    assert "endpoints" not in cfg
    assert "endpoint" not in cfg
    assert "access_key" not in cfg
    assert "secret_key" not in cfg
    assert "force_path_style" not in cfg
    assert cfg["key"] == "platform-secrets/tst.tfstate"
    assert cfg["use_path_style"] == "true"
    assert cfg["skip_requesting_account_id"] == "true"


@pytest.mark.asyncio
async def test_init_argv_carries_no_secret_values(tmp_path: Path) -> None:
    """Recurrence guard for review finding 2: no element of the argv
    _init/_init_platform assemble (the -backend-config pairs) may contain
    the state-backend secret key or access key — those travel env-only
    via _spawn. If a future edit re-adds credentials to either backend
    dict, this fails."""
    settings = get_settings()
    runner = TerraformRunner(settings)
    workdir = tmp_path / "ws"
    workdir.mkdir()

    captured_args: list[list[str]] = []
    ok = TerraformResult(exit_code=0, stdout="", stderr="", outputs={})

    async def _fake_spawn(wd, args, timeout):  # noqa: ARG001
        captured_args.append(args)
        return ok

    with patch.object(runner, "_spawn", AsyncMock(side_effect=_fake_spawn)):
        await runner._init(workdir, "t-001")
        await runner._init_platform(workdir, "tst")

    assert len(captured_args) == 2, "expected _init AND _init_platform to spawn terraform"
    for args in captured_args:
        for arg in args:
            assert settings.tf_state_backend_secret_key not in arg
            assert settings.tf_state_backend_access_key not in arg


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
    mirror), the S3 state-backend env trio (AWS_ENDPOINT_URL_S3 — the
    only CLI-settable channel for the backend's object-typed
    `endpoints.s3` attribute — plus the AWS credential pair), and merge
    _provider_env() onto the subprocess environment — proves the wiring
    end-to-end without invoking real terraform."""
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
    assert captured_env["AWS_ENDPOINT_URL_S3"] == settings.tf_state_backend_endpoint
    assert captured_env["AWS_ACCESS_KEY_ID"] == settings.tf_state_backend_access_key
    assert captured_env["AWS_SECRET_ACCESS_KEY"] == settings.tf_state_backend_secret_key
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


# ---------------------------------------------------------------------------
# P5.2 — platform-bus-topology reconcile (provisioning.apply_platform_bus_topology)
# ---------------------------------------------------------------------------


def _seed_bus_topology_standalone(standalone_root: Path) -> None:
    src = standalone_root / "platform-bus-topology-apply"
    src.mkdir(parents=True)
    (src / "main.tf").write_text('terraform {\n  required_version = ">= 1.6"\n}\n')


def test_platform_bus_topology_backend_config_matches_state_key_convention() -> None:
    """Key must be exactly `platform/bus-topology/<env>.tfstate` — the
    convention documented in pneuma-deployments'
    modules/platform-bus-topology/README.md 'State-key convention'.
    Same flat-primitives-only shape as every other backend-config dict on
    this runner (no secrets, no nested `endpoints`)."""
    settings = get_settings()
    runner = TerraformRunner(settings)

    cfg = runner._platform_bus_topology_backend_config("tst")
    assert cfg["key"] == "platform/bus-topology/tst.tfstate"
    assert cfg["bucket"] == settings.tf_state_backend_bucket
    assert cfg["use_path_style"] == "true"
    assert cfg["skip_requesting_account_id"] == "true"
    assert "endpoints" not in cfg
    assert "endpoint" not in cfg
    assert "access_key" not in cfg
    assert "secret_key" not in cfg
    assert "force_path_style" not in cfg


def test_platform_bus_topology_state_key_disjoint_from_tenant_and_platform_secrets_keys() -> None:
    """Explicit collision guard: the platform-bus-topology state key must
    NEVER match a per-tenant `tenants/<tenant_id>.tfstate` key or the
    platform-secrets `platform-secrets/<env>.tfstate` key — for every env
    AND even for adversarial tenant_ids that echo the bus-topology
    naming (e.g. a tenant literally called 'bus-topology' or
    'platform-bus-topology' must still land under `tenants/`, never
    `platform/`)."""
    settings = get_settings()
    runner = TerraformRunner(settings)

    for env in ("dev", "tst", "prod"):
        bus_topology_key = runner._platform_bus_topology_backend_config(env)["key"]
        platform_secrets_key = runner._platform_secrets_backend_config(env)["key"]

        assert bus_topology_key == f"platform/bus-topology/{env}.tfstate"
        assert bus_topology_key != platform_secrets_key
        assert not bus_topology_key.startswith("tenants/")
        assert not platform_secrets_key.startswith("platform/bus-topology/")

    adversarial_tenant_ids = ("bus-topology", "platform-bus-topology", "platform-secrets", "prod")
    for tenant_id in adversarial_tenant_ids:
        tenant_key = runner._backend_config(tenant_id)["key"]
        assert tenant_key.startswith("tenants/")
        assert tenant_key != "platform/bus-topology/tst.tfstate"
        assert tenant_key not in {
            runner._platform_bus_topology_backend_config(e)["key"] for e in ("dev", "tst", "prod")
        }


def test_platform_bus_topology_tfvars_only_carries_env() -> None:
    """The standalone harness's only required root variable is `env` —
    `platform_vhost` defaults to null and must NOT be set here (no
    caller-supplied vhost-override surface)."""
    settings = get_settings()
    runner = TerraformRunner(settings)

    vars_ = runner._platform_bus_topology_tfvars(PlatformBusTopologyInputs(env="tst"))
    assert vars_ == {"env": "tst"}


def test_platform_bus_topology_workdir_rejects_invalid_env() -> None:
    settings = get_settings()
    runner = TerraformRunner(settings)

    with pytest.raises(ValueError, match="invalid env"):
        runner._platform_bus_topology_workdir("staging")


def test_platform_bus_topology_workdir_isolated_from_platform_secrets_workdir() -> None:
    """The two platform-tier workspaces must resolve to different
    filesystem paths even for the same env — a shared directory would
    let a stale tfvars/backend file from one reconcile bleed into the
    other's `terraform init`."""
    settings = get_settings()
    runner = TerraformRunner(settings)

    bus_topology_dir = runner._platform_bus_topology_workdir("tst")
    platform_secrets_dir = runner._platform_secrets_workdir("tst")
    assert bus_topology_dir != platform_secrets_dir
    assert str(bus_topology_dir).startswith(str(settings.terraform_workdir_root.resolve()))


@pytest.mark.asyncio
async def test_ensure_platform_bus_topology_workspace_copies_source_files(
    tmp_path: Path,
) -> None:
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_standalone_root=tmp_path / "standalone",
        terraform_binary="/bin/true",
    )
    _seed_bus_topology_standalone(settings.terraform_standalone_root)
    settings.terraform_workdir_root.mkdir(parents=True)
    runner = TerraformRunner(settings)

    workdir = await runner._ensure_platform_bus_topology_workspace("tst")
    assert (workdir / "main.tf").exists()


@pytest.mark.asyncio
async def test_ensure_platform_bus_topology_workspace_missing_source_raises() -> None:
    """Mirrors _ensure_platform_workspace's missing-harness guard — a
    baked-image regression (harness dir absent) must surface as a
    TerraformError, not a raw FileNotFoundError from shutil/iterdir."""
    settings = get_settings()
    runner = TerraformRunner(settings)

    with pytest.raises(TerraformError) as exc_info:
        await runner._ensure_platform_bus_topology_workspace("tst")
    assert exc_info.value.command == "init"
    assert "platform-bus-topology-apply" in exc_info.value.result.stderr


@pytest.mark.asyncio
async def test_reconcile_platform_bus_topology_happy_path(tmp_path: Path) -> None:
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_standalone_root=tmp_path / "standalone",
        terraform_binary="/bin/true",
    )
    _seed_bus_topology_standalone(settings.terraform_standalone_root)
    settings.terraform_workdir_root.mkdir(parents=True)
    runner = TerraformRunner(settings)

    fake_init = TerraformResult(exit_code=0, stdout="initialized", stderr="", outputs={})
    fake_apply = TerraformResult(
        exit_code=0,
        stdout="Apply complete! Resources: 12 added, 0 changed, 0 destroyed.",
        stderr="",
        outputs={},
    )
    fake_output = TerraformResult(
        exit_code=0,
        stdout=json.dumps({"vhost": {"value": "/pneuma-tst"}}),
        stderr="",
        outputs={},
    )
    spawn_results = iter([fake_init, fake_apply, fake_output])

    async def _fake_spawn(workdir, args, timeout):  # noqa: ARG001
        return next(spawn_results)

    with patch.object(runner, "_spawn", AsyncMock(side_effect=_fake_spawn)):
        result = await runner.reconcile_platform_bus_topology(
            PlatformBusTopologyInputs(env="tst")
        )

    assert result.exit_code == 0
    assert result.outputs["vhost"] == "/pneuma-tst"
    assert "Apply complete" in result.stdout


@pytest.mark.asyncio
async def test_reconcile_platform_bus_topology_propagates_apply_failure(
    tmp_path: Path,
) -> None:
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_standalone_root=tmp_path / "standalone",
        terraform_binary="/bin/true",
    )
    _seed_bus_topology_standalone(settings.terraform_standalone_root)
    settings.terraform_workdir_root.mkdir(parents=True)
    runner = TerraformRunner(settings)

    fake_init = TerraformResult(exit_code=0, stdout="", stderr="", outputs={})
    fake_apply = TerraformResult(
        exit_code=1, stdout="", stderr="Error: vhost already managed by Helm", outputs={},
    )
    spawn_results = iter([fake_init, fake_apply])

    async def _fake_spawn(workdir, args, timeout):  # noqa: ARG001
        return next(spawn_results)

    with patch.object(runner, "_spawn", AsyncMock(side_effect=_fake_spawn)):
        with pytest.raises(TerraformError) as exc_info:
            await runner.reconcile_platform_bus_topology(
                PlatformBusTopologyInputs(env="tst")
            )

    assert exc_info.value.command == "apply"
    assert "already managed by Helm" in exc_info.value.result.stderr


@pytest.mark.asyncio
async def test_reconcile_platform_bus_topology_wipes_terraform_tfvars_json(
    tmp_path: Path,
) -> None:
    """Mirrors test_reconcile_platform_secrets_wipes_terraform_tfvars_json
    — the tfvars file (carrying no secrets here, but the same wipe
    discipline applies uniformly across every reconcile path) must not
    persist on disk after a run."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_standalone_root=tmp_path / "standalone",
        terraform_binary="/bin/true",
    )
    _seed_bus_topology_standalone(settings.terraform_standalone_root)
    settings.terraform_workdir_root.mkdir(parents=True)
    runner = TerraformRunner(settings)
    ok = TerraformResult(exit_code=0, stdout="ok", stderr="", outputs={})

    with patch.object(runner, "_spawn", AsyncMock(return_value=ok)):
        with patch.object(runner, "_output_json", AsyncMock(return_value={})):
            await runner.reconcile_platform_bus_topology(PlatformBusTopologyInputs(env="tst"))

    tfvars = settings.terraform_workdir_root / "_platform_bus_topology" / "tst" / "terraform.tfvars.json"
    assert not tfvars.exists(), "terraform.tfvars.json must be wiped after platform-bus-topology reconcile"


@pytest.mark.asyncio
async def test_reconcile_platform_bus_topology_wipes_tfvars_on_apply_failure(
    tmp_path: Path,
) -> None:
    """Even when apply fails the tfvars file is still wiped — mirrors
    test_reconcile_wipes_tfvars_on_apply_failure for the tenant path."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_standalone_root=tmp_path / "standalone",
        terraform_binary="/bin/true",
    )
    _seed_bus_topology_standalone(settings.terraform_standalone_root)
    settings.terraform_workdir_root.mkdir(parents=True)
    runner = TerraformRunner(settings)

    init_ok = TerraformResult(exit_code=0, stdout="init-ok", stderr="", outputs={})
    apply_fail = TerraformResult(exit_code=1, stdout="", stderr="apply boom", outputs={})
    spawn_results = [init_ok, apply_fail]

    async def _fake_spawn(*args, **kwargs):
        return spawn_results.pop(0)

    with patch.object(runner, "_spawn", side_effect=_fake_spawn):
        with pytest.raises(TerraformError):
            await runner.reconcile_platform_bus_topology(PlatformBusTopologyInputs(env="tst"))

    tfvars = settings.terraform_workdir_root / "_platform_bus_topology" / "tst" / "terraform.tfvars.json"
    assert not tfvars.exists(), "tfvars must be wiped even on apply failure"


@pytest.mark.asyncio
async def test_init_platform_bus_topology_raises_terraform_error_on_nonzero(
    tmp_path: Path,
) -> None:
    """Mirrors test_init_raises_terraform_error_on_nonzero for the
    bus-topology init path — keeps reconcile_platform_bus_topology on the
    failure path when the backend-config init itself fails (e.g. a state
    bucket permission issue), distinct from an apply failure."""
    settings = get_settings()
    runner = TerraformRunner(settings)
    workdir = tmp_path / "ws"
    workdir.mkdir()

    bad = TerraformResult(exit_code=1, stdout="", stderr="backend init boom", outputs={})
    with patch.object(runner, "_spawn", AsyncMock(return_value=bad)):
        with pytest.raises(TerraformError) as exc_info:
            await runner._init_platform_bus_topology(workdir, "tst")
    assert exc_info.value.command == "init"
    assert exc_info.value.result.exit_code == 1


@pytest.mark.asyncio
async def test_init_platform_bus_topology_argv_carries_no_secret_values(
    tmp_path: Path,
) -> None:
    """Recurrence guard mirroring test_init_argv_carries_no_secret_values
    — the bus-topology backend config carries no secrets by construction
    (same flat-primitives dict shape), but this locks the invariant at
    the _init call site too."""
    settings = get_settings()
    runner = TerraformRunner(settings)
    workdir = tmp_path / "ws"
    workdir.mkdir()

    captured_args: list[list[str]] = []
    ok = TerraformResult(exit_code=0, stdout="", stderr="", outputs={})

    async def _fake_spawn(wd, args, timeout):  # noqa: ARG001
        captured_args.append(args)
        return ok

    with patch.object(runner, "_spawn", AsyncMock(side_effect=_fake_spawn)):
        await runner._init_platform_bus_topology(workdir, "tst")

    assert len(captured_args) == 1
    for arg in captured_args[0]:
        assert settings.tf_state_backend_secret_key not in arg
        assert settings.tf_state_backend_access_key not in arg


# ---------------------------------------------------------------------------
# Concurrency guard — _spawn bounds concurrent terraform subprocesses.
# Regression coverage for the 2026-07-27 OOMKill: six concurrent applies
# spawned within 13s, 618Mi against a 1Gi pod limit, exit 137 twice.
# ---------------------------------------------------------------------------


class _FakeProc:
    """Stand-in for the object asyncio.create_subprocess_exec returns.
    communicate() tracks live/peak concurrency via ``state`` and holds the
    semaphore for ``hold`` seconds so overlapping _spawn calls are
    observable."""

    returncode = 0

    def __init__(self, state: dict[str, int], hold: float = 0.05) -> None:
        self._state = state
        self._hold = hold

    async def communicate(self) -> tuple[bytes, bytes]:
        self._state["current"] += 1
        self._state["peak"] = max(self._state["peak"], self._state["current"])
        await asyncio.sleep(self._hold)
        self._state["current"] -= 1
        return b"", b""

    def kill(self) -> None:  # pragma: no cover - not exercised here
        pass

    async def wait(self) -> None:  # pragma: no cover - not exercised here
        return None


@pytest.mark.asyncio
async def test_spawn_respects_concurrency_limit(tmp_path: Path) -> None:
    """5 concurrent _spawn calls against a 2-slot limit must never observe
    more than 2 running at once, and all 5 must still complete."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/bin/true",
        max_concurrent_terraform_runs=2,
    )
    settings.terraform_workdir_root.mkdir(parents=True)
    workdir = settings.terraform_workdir_root / "ws"
    workdir.mkdir()
    runner = TerraformRunner(settings)

    state = {"current": 0, "peak": 0}

    async def _fake_exec(*args, **kwargs):  # noqa: ARG001
        return _FakeProc(state)

    with patch("asyncio.create_subprocess_exec", side_effect=_fake_exec):
        results = await asyncio.gather(
            *[runner._spawn(workdir, ["apply"], timeout=5) for _ in range(5)]
        )

    assert len(results) == 5
    assert all(r.exit_code == 0 for r in results)
    assert state["peak"] <= 2, f"observed peak concurrency {state['peak']} exceeded limit 2"


@pytest.mark.asyncio
async def test_spawn_releases_semaphore_on_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The timeout/kill branch must not leak the semaphore permit — a
    subsequent _spawn must still acquire (bounded wait, not a hang).
    Uses timeout=1.2 (just above the 1s refuse-to-start floor — see
    _MIN_RUN_SECONDS_AFTER_QUEUE) rather than a near-zero value: an
    operation timeout below that floor now gets intercepted by the
    refuse-to-start guard before ever spawning a process, which would
    stop this test from exercising the actual kill()-on-timeout path it
    means to cover. The real ~1.2s wait_for expiry below is a deliberate,
    small, real-time cost to keep that coverage genuine."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/bin/true",
        max_concurrent_terraform_runs=1,
    )
    settings.terraform_workdir_root.mkdir(parents=True)
    workdir = settings.terraform_workdir_root / "ws"
    workdir.mkdir()
    runner = TerraformRunner(settings)

    class _HangingProc:
        returncode = None

        async def communicate(self) -> tuple[bytes, bytes]:
            await asyncio.sleep(10)  # never resolves before the timeout below
            return b"", b""

        def kill(self) -> None:
            pass

        async def wait(self) -> None:
            return None

    async def _hanging_exec(*args, **kwargs):  # noqa: ARG001
        return _HangingProc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _hanging_exec)
    result = await runner._spawn(workdir, ["apply"], timeout=1.2)
    assert result.exit_code == 124

    class _FastProc:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"ok", b""

        def kill(self) -> None:
            pass

        async def wait(self) -> None:
            return None

    async def _fast_exec(*args, **kwargs):  # noqa: ARG001
        return _FastProc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fast_exec)
    # If the timeout path leaked the permit, this would hang forever with
    # max_concurrent_terraform_runs=1 — the outer wait_for turns a leak
    # into a clean test failure instead of a stuck suite.
    second = await asyncio.wait_for(runner._spawn(workdir, ["apply"], timeout=5), timeout=1)
    assert second.exit_code == 0


@pytest.mark.asyncio
async def test_spawn_releases_semaphore_on_exception(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An exception raised while the permit is held (e.g. subprocess spawn
    failure) must not leak it — a subsequent _spawn must still acquire."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/bin/true",
        max_concurrent_terraform_runs=1,
    )
    settings.terraform_workdir_root.mkdir(parents=True)
    workdir = settings.terraform_workdir_root / "ws"
    workdir.mkdir()
    runner = TerraformRunner(settings)

    async def _raising_exec(*args, **kwargs):  # noqa: ARG001
        raise RuntimeError("boom: subprocess spawn failed")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _raising_exec)
    with pytest.raises(RuntimeError, match="boom"):
        await runner._spawn(workdir, ["apply"], timeout=5)

    class _FastProc:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"ok", b""

        def kill(self) -> None:
            pass

        async def wait(self) -> None:
            return None

    async def _fast_exec(*args, **kwargs):  # noqa: ARG001
        return _FastProc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fast_exec)
    second = await asyncio.wait_for(runner._spawn(workdir, ["apply"], timeout=5), timeout=1)
    assert second.exit_code == 0


@pytest.mark.asyncio
async def test_spawn_logs_when_queued(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    """When a call actually waits behind the concurrency limit, _spawn logs
    it — a silent wait looks indistinguishable from a hang. Drives the
    monotonic clock directly so the test doesn't need a real >1s wait."""
    import logging

    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/bin/true",
        max_concurrent_terraform_runs=3,
    )
    settings.terraform_workdir_root.mkdir(parents=True)
    workdir = settings.terraform_workdir_root / "ws"
    workdir.mkdir()
    runner = TerraformRunner(settings)

    class _FastProc:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"ok", b""

        def kill(self) -> None:
            pass

        async def wait(self) -> None:
            return None

    async def _fast_exec(*args, **kwargs):  # noqa: ARG001
        return _FastProc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fast_exec)

    # Drive only the two time.monotonic() reads _spawn itself makes (_t0,
    # then the post-acquire read) to fake a 2.5s queue wait without a real
    # sleep. Anything beyond those two calls falls back to the real clock
    # — asyncio's own internals (loop.time(), wait_for's deadline tracking)
    # also call time.monotonic() and must keep advancing normally.
    real_monotonic = runner_mod.time.monotonic
    clock = iter([0.0, 2.5])

    def _fake_monotonic() -> float:
        try:
            return next(clock)
        except StopIteration:
            return real_monotonic()

    monkeypatch.setattr(runner_mod.time, "monotonic", _fake_monotonic)

    caplog.set_level(logging.INFO, logger="terraformer.terraform")
    await runner._spawn(workdir, ["apply"], timeout=5)

    assert any("tf spawn queued" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_spawn_reaps_subprocess_on_cancellation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CancelledError (e.g. the gRPC caller's ~60s deadline firing —
    grpc.aio cancels the server handler on expiry) must still reap the
    subprocess before the permit is released. CancelledError is a
    BaseException: pre-fix, only `except asyncio.TimeoutError` ran, so
    cancellation left the process (and its ~5 provider children) alive
    and unreaped while the freed permit let a queued run start — live
    processes then silently exceeded the concurrency limit. Without the
    `finally` this test's kill_calls stays empty."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/bin/true",
        max_concurrent_terraform_runs=1,
    )
    settings.terraform_workdir_root.mkdir(parents=True)
    workdir = settings.terraform_workdir_root / "ws"
    workdir.mkdir()
    runner = TerraformRunner(settings)

    kill_calls: list[bool] = []

    class _NeverEndingProc:
        returncode = None

        async def communicate(self) -> tuple[bytes, bytes]:
            await asyncio.sleep(10)  # cancelled long before this fires
            return b"", b""

        def kill(self) -> None:
            kill_calls.append(True)
            self.returncode = -9

        async def wait(self) -> None:
            return None

    async def _never_ending_exec(*args, **kwargs):  # noqa: ARG001
        return _NeverEndingProc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _never_ending_exec)

    task = asyncio.ensure_future(runner._spawn(workdir, ["apply"], timeout=600))
    await asyncio.sleep(0.05)  # let _spawn acquire the permit and enter communicate()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert kill_calls, "cancelled _spawn must kill the still-running subprocess"

    class _FastProc:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"ok", b""

        def kill(self) -> None:
            pass

        async def wait(self) -> None:
            return None

    async def _fast_exec(*args, **kwargs):  # noqa: ARG001
        return _FastProc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fast_exec)
    # If cancellation leaked the permit, this hangs forever (limit=1) —
    # the outer wait_for turns that into a clean test failure.
    second = await asyncio.wait_for(runner._spawn(workdir, ["apply"], timeout=5), timeout=1)
    assert second.exit_code == 0


@pytest.mark.asyncio
async def test_spawn_acquire_times_out_when_saturated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dispatch that cannot acquire a concurrency slot within its
    (timeout-scaled) queue budget must fail fast with exit_code=124
    rather than hang — regression guard for finding B4: a read-only
    `terraform output -json` (own timeout 30s) queuing behind a 600s
    apply could otherwise hang for ~20 minutes. spawn_queue_timeout_fraction
    is driven near-zero so the 30s-timeout read's budget floors at 1s,
    keeping this test fast without changing what it exercises."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/bin/true",
        max_concurrent_terraform_runs=1,
        spawn_queue_timeout_fraction=0.01,
    )
    settings.terraform_workdir_root.mkdir(parents=True)
    workdir = settings.terraform_workdir_root / "ws"
    workdir.mkdir()
    runner = TerraformRunner(settings)

    class _SlowProc:
        returncode = None

        async def communicate(self) -> tuple[bytes, bytes]:
            await asyncio.sleep(5)
            self.returncode = 0
            return b"", b""

        def kill(self) -> None:
            self.returncode = -9

        async def wait(self) -> None:
            return None

    async def _slow_exec(*args, **kwargs):  # noqa: ARG001
        return _SlowProc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _slow_exec)

    busy_task = asyncio.ensure_future(runner._spawn(workdir, ["apply"], timeout=30))
    await asyncio.sleep(0.05)  # let the busy call acquire the sole permit

    # A second dispatch must fail fast (queue budget=1s) instead of
    # waiting behind the still-running "apply" above.
    saturated = await asyncio.wait_for(
        runner._spawn(workdir, ["output", "-json"], timeout=30), timeout=3
    )
    assert saturated.exit_code == 124
    assert "queued too long" in saturated.stderr

    busy_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await busy_task

    # No permit leaked by the timed-out acquire attempt.
    class _FastProc:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"ok", b""

        def kill(self) -> None:
            pass

        async def wait(self) -> None:
            return None

    async def _fast_exec(*args, **kwargs):  # noqa: ARG001
        return _FastProc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fast_exec)
    third = await asyncio.wait_for(runner._spawn(workdir, ["apply"], timeout=5), timeout=1)
    assert third.exit_code == 0


def test_spawn_queue_budget_scales_with_operation_timeout(tmp_path: Path) -> None:
    """The queue budget must scale with the operation's OWN timeout, not
    sit at a single flat value regardless of what kind of run it is —
    assert the computed numbers directly, not just end-to-end behavior.
    Default spawn_queue_timeout_fraction=0.5: a 30s read gets a 15s
    budget, a 600s apply gets a 300s budget."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/bin/true",
    )
    runner = TerraformRunner(settings)
    assert runner._spawn_queue_budget(30) == 15
    assert runner._spawn_queue_budget(600) == 300
    # Monotonic: a longer operation timeout never yields a smaller budget.
    assert runner._spawn_queue_budget(600) > runner._spawn_queue_budget(30)


def test_spawn_queue_budget_floors_at_one_second(tmp_path: Path) -> None:
    """A very short operation timeout, or a near-zero fraction override,
    must still get a real (if tiny) queue attempt rather than a 0s
    budget that fails before ever trying to acquire."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/bin/true",
        spawn_queue_timeout_fraction=0.01,
    )
    runner = TerraformRunner(settings)
    assert runner._spawn_queue_budget(1) == 1
    assert runner._spawn_queue_budget(30) == 1


@pytest.mark.asyncio
async def test_spawn_long_timeout_not_aborted_at_old_flat_45s(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression guard for the actual shipped defect: the queue budget
    for a 600s-timeout apply used to be a flat 45s, calibrated against
    the WRONG assumption that every caller's gRPC deadline was ~60s —
    the real provisioning caller configures 600s (onboarding_cycles.py
    sets both the runner arg and the step timeout to 600, explicitly to
    stop Temporal killing a legitimately-running apply early). A run
    that merely queued behind others was aborted at 45s despite having
    600s of caller budget. This test fails if the budget formula ever
    reverts to that flat 45s — both at the formula level and by proving
    `_spawn` actually hands the scaled budget (not a stale 45) to the
    real acquire wait."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/bin/true",
    )
    settings.terraform_workdir_root.mkdir(parents=True)
    workdir = settings.terraform_workdir_root / "ws"
    workdir.mkdir()
    runner = TerraformRunner(settings)

    budget_600 = runner._spawn_queue_budget(600)
    assert budget_600 != 45
    assert budget_600 > 45

    class _FastProc:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"ok", b""

        def kill(self) -> None:
            pass

        async def wait(self) -> None:
            return None

    async def _fast_exec(*args, **kwargs):  # noqa: ARG001
        return _FastProc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fast_exec)

    captured_timeouts: list[float] = []
    real_wait_for = asyncio.wait_for

    async def _spy_wait_for(fut, timeout=None, *a, **kw):
        captured_timeouts.append(timeout)
        return await real_wait_for(fut, timeout, *a, **kw)

    monkeypatch.setattr(asyncio, "wait_for", _spy_wait_for)

    result = await runner._spawn(workdir, ["apply"], timeout=600)
    assert result.exit_code == 0
    # First wait_for call inside _spawn is the semaphore-acquire budget —
    # must be the scaled 300s, never the old flat 45s.
    assert captured_timeouts[0] == budget_600 == 300
    assert captured_timeouts[0] != 45


def test_effective_run_timeout_deducts_queue_wait(tmp_path: Path) -> None:
    """The caller's Temporal step timeout covers the WHOLE RPC — queue
    wait included (onboarding_cycles.py deliberately aligns the step
    timeout with the `timeout` value the runner receives) — so the run
    must get what's LEFT of `timeout` after queuing, not `timeout` again
    on top of it. queue_wait + run must never exceed the original
    `timeout`."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/bin/true",
    )
    runner = TerraformRunner(settings)
    assert runner._effective_run_timeout(600, 40.0) == 560
    assert runner._effective_run_timeout(600, 0.0) == 600
    # queue_wait + run == the original timeout, by construction.
    queued = 40.0
    assert queued + runner._effective_run_timeout(600, queued) == 600


@pytest.mark.asyncio
async def test_spawn_run_timeout_is_reduced_by_actual_queue_wait(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Integration proof that _spawn actually wires _effective_run_timeout
    into the real subprocess wait: a call that spent 40s queuing on a
    600s-timeout op must hand `proc.communicate()` a 560s budget, not the
    full 600s again — otherwise queue_wait + run (640s) would exceed the
    600s the caller is actually willing to wait, and Temporal would kill
    the activity mid-apply. Drives the monotonic clock directly (like
    test_spawn_logs_when_queued) so this doesn't need a real 40s sleep."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/bin/true",
    )
    settings.terraform_workdir_root.mkdir(parents=True)
    workdir = settings.terraform_workdir_root / "ws"
    workdir.mkdir()
    runner = TerraformRunner(settings)

    class _FastProc:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"ok", b""

        def kill(self) -> None:
            pass

        async def wait(self) -> None:
            return None

    async def _fast_exec(*args, **kwargs):  # noqa: ARG001
        return _FastProc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fast_exec)

    # Fake time.monotonic() to simulate a 40s queue wait for a 600s-timeout
    # call without a real sleep: the FIRST call (_spawn's `_t0` read)
    # returns 0.0, every call after that returns 40.0. Clamping (rather
    # than a finite iterator falling back to the real clock) matters
    # because asyncio.wait_for's own deadline bookkeeping calls
    # loop.time() -> time.monotonic() too, at least once, between our
    # `_t0` read and our `_queued_s` read — a strict 2-value iterator
    # would be consumed by that hidden call and hand our own
    # `_queued_s` read a huge real-clock fallback value instead of 40.0.
    real_monotonic = runner_mod.time.monotonic
    _calls = {"n": 0}

    def _fake_monotonic() -> float:
        _calls["n"] += 1
        return 0.0 if _calls["n"] == 1 else 40.0

    monkeypatch.setattr(runner_mod.time, "monotonic", _fake_monotonic)

    captured_timeouts: list[float] = []
    real_wait_for = asyncio.wait_for

    async def _spy_wait_for(fut, timeout=None, *a, **kw):
        captured_timeouts.append(timeout)
        return await real_wait_for(fut, timeout, *a, **kw)

    monkeypatch.setattr(asyncio, "wait_for", _spy_wait_for)

    result = await runner._spawn(workdir, ["apply"], timeout=600)
    assert result.exit_code == 0
    # captured_timeouts[0] = semaphore-acquire budget (unaffected by the
    # simulated queue wait — the real acquire didn't actually block).
    # captured_timeouts[1] = the run timeout handed to proc.communicate(),
    # which must be timeout(600) - queued(40) = 560, never 600 again.
    assert captured_timeouts[1] == 560
    # queue_wait + run never exceeds the caller's original budget.
    assert 40.0 + captured_timeouts[1] == 600


@pytest.mark.asyncio
async def test_spawn_refuses_to_start_when_queue_wait_consumes_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the queue wait alone consumes (almost) the whole timeout
    budget, _spawn must refuse to start the subprocess at all: starting
    a long apply with only a few seconds left is strictly worse than not
    starting it — it does real, partial, stateful work against the
    tfstate and then gets killed mid-run anyway. Must return 124 WITHOUT
    ever calling create_subprocess_exec."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/bin/true",
    )
    settings.terraform_workdir_root.mkdir(parents=True)
    workdir = settings.terraform_workdir_root / "ws"
    workdir.mkdir()
    runner = TerraformRunner(settings)

    spawn_calls: list[tuple] = []

    async def _must_not_be_called(*args, **kwargs):
        spawn_calls.append((args, kwargs))
        raise AssertionError("terraform subprocess must not be spawned")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _must_not_be_called)

    # timeout=30, simulate a 29.5s queue wait -> remaining=0.5s, below the
    # 1s floor -> must refuse without ever reaching create_subprocess_exec.
    # Clamp (rather than a finite iterator) past the first call: asyncio's
    # own wait_for deadline bookkeeping also calls time.monotonic() at
    # least once between our `_t0` and `_queued_s` reads, and a strict
    # 2-value iterator would hand that hidden call the real clock instead.
    _calls = {"n": 0}

    def _fake_monotonic() -> float:
        _calls["n"] += 1
        return 0.0 if _calls["n"] == 1 else 29.5

    monkeypatch.setattr(runner_mod.time, "monotonic", _fake_monotonic)

    result = await runner._spawn(workdir, ["apply"], timeout=30)

    assert result.exit_code == 124
    assert "consumed" in result.stderr and "budget" in result.stderr
    assert not spawn_calls, "must refuse before ever spawning the subprocess"

    # No permit leaked by the refuse-to-start branch either.
    class _FastProc:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"ok", b""

        def kill(self) -> None:
            pass

        async def wait(self) -> None:
            return None

    async def _fast_exec(*args, **kwargs):  # noqa: ARG001
        return _FastProc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fast_exec)
    second = await asyncio.wait_for(runner._spawn(workdir, ["apply"], timeout=5), timeout=1)
    assert second.exit_code == 0


@pytest.mark.asyncio
async def test_spawn_log_emitted_after_acquisition_not_before(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    """The 'tf spawn: ...' line must mean 'a process actually started' —
    regression guard for finding B3. The log signature that identified the
    2026-07-27 OOMKill incident was six 'tf spawn:' lines in 13s; logging
    before the acquire would keep printing one line per dispatch even once
    concurrency is bounded, misleading the next investigator. With the
    limit saturated by a still-running call, a second, queued dispatch
    must NOT log 'tf spawn:' until the first call frees the slot — and the
    contended wait must log its own 'queued (waiting...)' line instead."""
    import logging

    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/bin/true",
        max_concurrent_terraform_runs=1,
    )
    settings.terraform_workdir_root.mkdir(parents=True)
    workdir = settings.terraform_workdir_root / "ws"
    workdir.mkdir()
    runner = TerraformRunner(settings)

    release_first = asyncio.Event()

    class _BlockingProc:
        returncode = None

        async def communicate(self) -> tuple[bytes, bytes]:
            await release_first.wait()
            self.returncode = 0
            return b"", b""

        def kill(self) -> None:
            pass

        async def wait(self) -> None:
            return None

    async def _blocking_exec(*args, **kwargs):  # noqa: ARG001
        return _BlockingProc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _blocking_exec)
    caplog.set_level(logging.INFO, logger="terraformer.terraform")

    first_task = asyncio.ensure_future(runner._spawn(workdir, ["apply"], timeout=30))
    await asyncio.sleep(0.05)  # first call acquires the sole permit and logs "tf spawn:"

    second_task = asyncio.ensure_future(runner._spawn(workdir, ["output", "-json"], timeout=30))
    await asyncio.sleep(0.05)  # second call is now queued behind the first

    spawn_lines_while_queued = [
        r for r in caplog.records if r.getMessage().startswith("tf spawn: ")
    ]
    assert len(spawn_lines_while_queued) == 1, (
        "only the running process may have logged 'tf spawn:' while the "
        "second dispatch is still queued"
    )
    queued_lines = [r for r in caplog.records if "tf spawn queued (waiting" in r.getMessage()]
    assert queued_lines, "a contended acquire must log the queued-waiting line"

    release_first.set()
    await asyncio.wait_for(first_task, timeout=1)
    await asyncio.wait_for(second_task, timeout=1)

    spawn_lines_final = [r for r in caplog.records if r.getMessage().startswith("tf spawn: ")]
    assert len(spawn_lines_final) == 2
