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
