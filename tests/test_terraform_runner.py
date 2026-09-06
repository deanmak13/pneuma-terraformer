"""Unit tests for TerraformRunner — exercise the subprocess wrapper
without invoking the real terraform CLI.
"""

from __future__ import annotations

import asyncio
import json
import shutil
import subprocess
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from services.terraformer.src import terraform_runner as runner_mod
from services.terraformer.src.settings import Settings, get_settings
from services.terraformer.src.terraform_runner import (
    PlatformBusTopologyInputs,
    PlatformResourcesInputs,
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


@pytest.mark.asyncio
async def test_platform_auth_workdir_blocks_symlink_escape() -> None:
    """Unlike _workspace_dir, _platform_auth_workdir takes no caller
    input — the `_platform_auth` subdirectory name is a fixed literal, so
    the only realistic way it can ever resolve outside
    terraform_workdir_root is a symlink planted at that path (e.g. a
    misconfigured shared PV mount). The same defence-in-depth guard must
    still catch that."""
    settings = get_settings()
    root = settings.terraform_workdir_root
    root.mkdir(parents=True, exist_ok=True)
    outside = root.parent / "outside-workdir-root"
    outside.mkdir(parents=True, exist_ok=True)
    (root / "_platform_auth").symlink_to(outside, target_is_directory=True)
    runner = TerraformRunner(settings)

    with pytest.raises(ValueError, match="platform-auth workdir escapes workdir root"):
        runner._platform_auth_workdir()


def test_scrub_credentials_redacts_known_secrets() -> None:
    """Every known credential value in stdout/stderr must be replaced
    with <REDACTED> before crossing the HTTP boundary."""
    from services.terraformer.src.terraform_runner import scrub_credentials

    settings = get_settings()
    secret = settings.rabbitmq_admin_password
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


# ---------------------------------------------------------------------------
# Generated vault provider block (feat/openbao-k8s-auth) — replaces the
# static VAULT_TOKEN. skip_child_token=true is LOAD-BEARING (without it the
# provider calls auth/token/create even under the kubernetes auth method,
# 403s against the least-privilege `terraformer` OpenBao policy); role/mount
# must come from Settings, never a hardcoded literal (design-for-N — a
# second cluster/role needs no code change).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ensure_workspace_writes_vault_provider_file() -> None:
    settings = get_settings()
    _seed_module(settings.terraform_modules_root)
    runner = TerraformRunner(settings)

    workdir = await runner._ensure_workspace("t-vault-001")
    provider_tf = workdir / "provider_vault.tf"
    assert provider_tf.exists()
    hcl = provider_tf.read_text()

    assert "skip_child_token = true" in hcl
    # The vault provider has NO `auth_login_kubernetes` block. Asserting on
    # that invented name is exactly what let the bug ship: the test only ever
    # compared the generator's output to itself, so it stayed green while
    # every real apply died with "Blocks of type auth_login_kubernetes are
    # not expected here" (2026-07-28). Pin the GENERIC block instead.
    assert "auth_login_kubernetes {" not in hcl
    assert "  auth_login {" in hcl
    assert f'path = "auth/{settings.vault_k8s_auth_mount}/login"' in hcl
    assert "parameters = {" in hcl
    assert f'role = "{settings.vault_k8s_auth_role}"' in hcl
    assert 'jwt = file("/var/run/secrets/kubernetes.io/serviceaccount/token")' in hcl


@pytest.mark.asyncio
async def test_vault_provider_role_and_mount_come_from_settings_not_literals(
    tmp_path: Path,
) -> None:
    """Changing the setting must change the rendered HCL — proves role/
    mount are threaded through, not baked-in literals."""
    settings_a = Settings(
        terraform_workdir_root=tmp_path / "wd-a",
        terraform_modules_root=tmp_path / "modules-a",
        terraform_binary="/bin/true",
        vault_k8s_auth_role="terraformer",
        vault_k8s_auth_mount="kubernetes",
    )
    _seed_module(settings_a.terraform_modules_root)
    hcl_a = TerraformRunner(settings_a)._vault_provider_hcl()

    settings_b = Settings(
        terraform_workdir_root=tmp_path / "wd-b",
        terraform_modules_root=tmp_path / "modules-b",
        terraform_binary="/bin/true",
        vault_k8s_auth_role="a-second-cluster-role",
        vault_k8s_auth_mount="kubernetes-secondary",
    )
    _seed_module(settings_b.terraform_modules_root)
    hcl_b = TerraformRunner(settings_b)._vault_provider_hcl()

    assert hcl_a != hcl_b
    assert 'role = "a-second-cluster-role"' in hcl_b
    # The mount is threaded into the generic auth_login path, not a `mount`
    # attribute — a second cluster/mount still needs config only, no code.
    assert 'path = "auth/kubernetes-secondary/login"' in hcl_b
    assert 'path = "auth/kubernetes/login"' in hcl_a
    assert "a-second-cluster-role" not in hcl_a


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("kubernetes", "kubernetes"), ("/kubernetes/", "kubernetes"), (" k8s ", "k8s")],
)
def test_auth_mount_is_slash_normalised(tmp_path: Path, raw: str, expected: str) -> None:
    """The mount is interpolated into `auth/<mount>/login`, so stray slashes
    would yield a malformed endpoint. Normalisation happens in Settings so
    the generator never has to re-derive it."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/bin/true",
        vault_k8s_auth_mount=raw,
    )
    assert settings.vault_k8s_auth_mount == expected
    _seed_module(settings.terraform_modules_root)
    assert (
        f'path = "auth/{expected}/login"'
        in TerraformRunner(settings)._vault_provider_hcl()
    )


@pytest.mark.skipif(
    shutil.which("terraform") is None,
    reason="terraform not on PATH; CI installs it (see .github/workflows/pr-checks.yml)",
)
def test_generated_vault_provider_hcl_passes_real_terraform_validate(
    tmp_path: Path,
) -> None:
    """The ONE assertion here that the provider, not we, get to make.

    Every other test in this file string-matches the generator's output
    against itself — they can confirm what we wrote, never that the
    provider accepts it. That gap shipped a `provider "vault"` block named
    `auth_login_kubernetes`, which does not exist: the suite stayed green
    while 100% of tenant provisioning runs died at provider-parse time and
    tenants sat in lifecycle_state=provisioning until the caller timed out
    (2026-07-28). Hand the rendered file to the real toolchain and let it
    reject an invented block name or argument.

    No backend, no state, no credentials — schema validation only.
    """
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/bin/true",
    )
    _seed_module(settings.terraform_modules_root)

    work = tmp_path / "hclcheck"
    work.mkdir()
    (work / "provider_vault.tf").write_text(
        TerraformRunner(settings)._vault_provider_hcl()
    )
    # Kept in lockstep with the baked tenant module's versions.tf — if the
    # two drift this guard stops reflecting the provider that actually runs.
    (work / "versions.tf").write_text(
        "terraform {\n"
        "  required_providers {\n"
        "    vault = {\n"
        '      source  = "hashicorp/vault"\n'
        '      version = "~> 4.2"\n'
        "    }\n"
        "  }\n"
        "}\n"
    )

    for args, timeout in (
        (["init", "-backend=false", "-input=false", "-no-color"], 300),
        (["validate", "-no-color"], 120),
    ):
        proc = subprocess.run(
            ["terraform", *args],
            cwd=work,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        assert proc.returncode == 0, (
            f"`terraform {args[0]}` rejected the generated provider block:\n"
            f"{proc.stdout}\n{proc.stderr}"
        )


@pytest.mark.parametrize("raw", ["/", "//", "   "])
def test_auth_mount_rejects_slash_only_values(tmp_path: Path, raw: str) -> None:
    """`min_length=1` passes "/" — it strips to empty and would render
    `auth//login`. Reject at config load instead of shipping a broken path."""
    with pytest.raises(ValueError, match="must name a mount path"):
        Settings(
            terraform_workdir_root=tmp_path / "wd",
            terraform_modules_root=tmp_path / "modules",
            terraform_binary="/bin/true",
            vault_k8s_auth_mount=raw,
        )


@pytest.mark.asyncio
async def test_ensure_platform_secrets_workspace_writes_vault_provider_file(
    tmp_path: Path,
) -> None:
    """platform-secrets-apply provisions vault_kv_secret_v2 resources via
    the same OpenBao connection — it must get the identical generated
    provider file as the tenant workspace (see terraform_runner.py's
    _ensure_platform_workspace comment on why this requires the
    companion pneuma-deployments harness to drop its own hardcoded
    `provider "vault" {}` block from main.tf)."""
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

    workdir = await runner._ensure_platform_workspace("tst")

    provider_tf = workdir / "provider_vault.tf"
    assert provider_tf.exists()
    assert "skip_child_token = true" in provider_tf.read_text()


@pytest.mark.asyncio
async def test_platform_workspaces_get_modules_symlink(tmp_path: Path) -> None:
    """Every standalone harness references `source = "../../modules/<name>"`,
    which from `<workdir_root>/_<harness>/<env>/` resolves to
    `<workdir_root>/modules` — the runner must materialize that as a symlink
    to the baked-in modules tree or every platform reconcile dies at init
    with "Unreadable module directory" (live incident 2026-08-19)."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_standalone_root=tmp_path / "standalone",
        terraform_binary="/bin/true",
    )
    settings.terraform_modules_root.mkdir(parents=True)
    standalone_src = settings.terraform_standalone_root / "platform-secrets-apply"
    standalone_src.mkdir(parents=True)
    (standalone_src / "main.tf").write_text("# stub\n")
    runner = TerraformRunner(settings)

    workdir = await runner._ensure_platform_workspace("tst")

    link = settings.terraform_workdir_root / "modules"
    assert link.is_symlink()
    assert link.resolve() == settings.terraform_modules_root.resolve()
    # the harness's relative source now resolves from inside the workspace
    assert (workdir / ".." / ".." / "modules").resolve() == (
        settings.terraform_modules_root.resolve()
    )
    # idempotent — second ensure leaves a valid link in place
    await runner._ensure_platform_workspace("tst")
    assert link.is_symlink()


@pytest.mark.asyncio
async def test_ensure_platform_bus_topology_workspace_has_no_vault_provider_file(
    tmp_path: Path,
) -> None:
    """platform-bus-topology-apply's only provider is rabbitmq (confirmed
    against pneuma-deployments' standalone harness — no vault provider
    block at all) — it must NOT get a generated provider_vault.tf; that
    file would declare a provider the harness's required_providers never
    lists, which Terraform rejects."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_standalone_root=tmp_path / "standalone",
        terraform_binary="/bin/true",
    )
    standalone_src = settings.terraform_standalone_root / "platform-bus-topology-apply"
    standalone_src.mkdir(parents=True)
    (standalone_src / "main.tf").write_text("# stub\n")
    settings.terraform_workdir_root.mkdir(parents=True)
    runner = TerraformRunner(settings)

    workdir = await runner._ensure_platform_bus_topology_workspace("tst")

    assert not (workdir / "provider_vault.tf").exists()


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


def test_provider_env_includes_four_credential_vars() -> None:
    settings = get_settings()
    runner = TerraformRunner(settings)

    env = runner._provider_env()
    assert env["PGPASSWORD"] == settings.postgres_superuser_password
    assert env["RABBITMQ_PASSWORD"] == settings.rabbitmq_admin_password
    assert env["MINIO_USER"] == settings.tf_state_backend_access_key
    assert env["MINIO_PASSWORD"] == settings.minio_admin_password


def test_provider_env_no_longer_exports_vault_token() -> None:
    """OpenBao auth moved to kubernetes auth via the generic auth_login block (generated
    provider_vault.tf) — the runner must never source a static
    VAULT_TOKEN from Settings again. A silent fallback to a stored
    token is exactly the failure class this fix removes."""
    settings = get_settings()
    runner = TerraformRunner(settings)

    env = runner._provider_env()
    assert "VAULT_TOKEN" not in env
    assert not hasattr(settings, "openbao_admin_token")


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


async def _spawn_capturing_env(runner: TerraformRunner, workdir: Path) -> dict[str, str]:
    captured_env: dict[str, str] = {}
    orig_create_subprocess_exec = __import__("asyncio").create_subprocess_exec

    async def _capturing_exec(*args, **kwargs):
        captured_env.update(kwargs.get("env") or {})
        return await orig_create_subprocess_exec(*args, **kwargs)

    with patch("asyncio.create_subprocess_exec", side_effect=_capturing_exec):
        await runner._spawn(workdir, ["--version"], timeout=5)
    return captured_env


@pytest.mark.asyncio
async def test_spawn_sets_and_creates_shared_plugin_cache_dir(tmp_path: Path) -> None:
    """Regression for the 2026-08-19 → 2026-09-06 TST eviction series (104
    `Usage of EmptyDir volume "workspace" exceeds the limit "2Gi"`): every
    subprocess must carry TF_PLUGIN_CACHE_DIR so provider packages are
    unpacked once per pod and symlinked into each workspace, and the dir
    must exist before terraform runs (terraform refuses a missing cache
    dir) — created by `_spawn` itself, because the reconcile CronJob runs
    this path from a fresh container with no lifespan hook."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/usr/bin/env",
    )
    settings.terraform_workdir_root.mkdir(parents=True)
    workdir = settings.terraform_workdir_root / "ws"
    workdir.mkdir()
    expected = settings.terraform_workdir_root / ".plugin-cache"
    assert not expected.exists()

    captured_env = await _spawn_capturing_env(TerraformRunner(settings), workdir)

    assert captured_env["TF_PLUGIN_CACHE_DIR"] == str(expected)
    assert expected.is_dir()


@pytest.mark.asyncio
async def test_spawn_honours_explicit_plugin_cache_dir(tmp_path: Path) -> None:
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/usr/bin/env",
        terraform_plugin_cache_dir=tmp_path / "elsewhere" / "cache",
    )
    settings.terraform_workdir_root.mkdir(parents=True)
    workdir = settings.terraform_workdir_root / "ws"
    workdir.mkdir()

    captured_env = await _spawn_capturing_env(TerraformRunner(settings), workdir)

    assert captured_env["TF_PLUGIN_CACHE_DIR"] == str(tmp_path / "elsewhere" / "cache")
    assert (tmp_path / "elsewhere" / "cache").is_dir()


def test_baked_cli_config_lets_the_cache_serve_fresh_workspaces() -> None:
    """Since Terraform 1.4 a cached provider is only reused when the
    workspace's .terraform.lock.hcl already vouches for it. Every tenant
    workspace here is generated fresh (no lock file), so without this
    setting TF_PLUGIN_CACHE_DIR would be inert and every init would still
    unpack the full provider set. Asserts on the effective (non-comment)
    lines of the baked config, not on its prose."""
    tfrc = Path(__file__).resolve().parents[1] / "tf" / "cli.tfrc"
    effective = [
        line.strip()
        for line in tfrc.read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    assert "plugin_cache_may_break_dependency_lock_file = true" in effective


@pytest.mark.asyncio
async def test_spawn_env_keeps_vault_addr_but_drops_vault_token(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """VAULT_ADDR is env-invariant cluster config (configmap-sourced, not
    a secret) — it must keep reaching the subprocess via the ambient
    `os.environ.copy()` in _spawn even after VAULT_TOKEN retirement.
    VAULT_TOKEN must NEVER appear, from Settings or anywhere else — a
    static token is the failure class this fix removes; there is no
    fallback path that could reintroduce it."""
    monkeypatch.setenv("VAULT_ADDR", "http://openbao.platform-tst.svc.cluster.local:8200")
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/usr/bin/env",
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

    assert captured_env["VAULT_ADDR"] == "http://openbao.platform-tst.svc.cluster.local:8200"
    assert "VAULT_TOKEN" not in captured_env


@pytest.mark.asyncio
async def test_spawn_extra_env_merges_onto_subprocess_env(tmp_path: Path) -> None:
    """`extra_env` (apply_platform_auth's one caller today, injecting a
    transient break-glass VAULT_TOKEN) must actually land in the real
    subprocess environment ON TOP OF everything _spawn already sets —
    exercised against the real `_spawn`, not a mocked stand-in, so the
    `env.update(extra_env)` merge itself is proven, not just that some
    dict got threaded through call args."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/usr/bin/env",
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
        await runner._spawn(
            workdir,
            ["--version"],
            timeout=5,
            extra_env={"VAULT_TOKEN": "s.break-glass-xyz"},
        )

    assert captured_env["VAULT_TOKEN"] == "s.break-glass-xyz"
    # extra_env is additive, not a replacement — everything _spawn already
    # sets must still be present alongside it.
    assert captured_env["TF_CLI_CONFIG_FILE"] == settings.tf_cli_config_file
    assert captured_env["AWS_ACCESS_KEY_ID"] == settings.tf_state_backend_access_key


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
    # call without a real sleep: the FIRST TWO calls return 0.0, every call
    # after that returns 40.0. Two (not one) because `_spawn` (the bounded-
    # retry wrapper — see fix/transient-conflict-retry) reads its own
    # `overall_start` before ever delegating to `_spawn_once`, whose `_t0`
    # read is now the SECOND call, not the first; both are effectively
    # simultaneous (negligible real overhead between them) so clamping both
    # to 0.0 is faithful, not a loosened assertion. Clamping (rather than a
    # finite iterator falling back to the real clock) matters because
    # asyncio.wait_for's own deadline bookkeeping calls loop.time() ->
    # time.monotonic() too, at least once, between our `_t0` read and our
    # `_queued_s` read — a strict iterator would be consumed by that hidden
    # call and hand our own `_queued_s` read a huge real-clock fallback
    # value instead of 40.0.
    _calls = {"n": 0}

    def _fake_monotonic() -> float:
        _calls["n"] += 1
        return 0.0 if _calls["n"] <= 2 else 40.0

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
    # First TWO calls return 0.0 (not one): `_spawn`'s own `overall_start`
    # read now precedes `_spawn_once`'s `_t0` read (bounded-retry wrapper —
    # see fix/transient-conflict-retry); both are effectively simultaneous,
    # so clamping both to 0.0 is faithful. Clamp past those two (rather
    # than a finite iterator) because asyncio's own wait_for deadline
    # bookkeeping also calls time.monotonic() at least once between our
    # `_t0` and `_queued_s` reads, and a strict iterator would hand that
    # hidden call the real clock instead.
    _calls = {"n": 0}

    def _fake_monotonic() -> float:
        _calls["n"] += 1
        return 0.0 if _calls["n"] <= 2 else 29.5

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


# ---------------------------------------------------------------------------
# Platform-auth bootstrap (feat/openbao-k8s-auth) — apply_platform_auth is
# the break-glass apply target for openbao_bootstrap.ensure_platform_auth.
# Uses a transient root token via _spawn's extra_env, never the generated
# k8s-auth provider_vault.tf (that role doesn't exist yet on the only
# occasion this harness runs).
# ---------------------------------------------------------------------------


def _seed_platform_auth_standalone(standalone_root: Path) -> None:
    src = standalone_root / "platform-auth-bootstrap"
    src.mkdir(parents=True)
    (src / "main.tf").write_text('terraform {\n  required_version = ">= 1.6"\n}\n')


@pytest.mark.asyncio
async def test_ensure_platform_auth_workspace_has_backend_stub_and_no_vault_provider_file(
    tmp_path: Path,
) -> None:
    """This harness must get a backend.tf stub (same reusable-root
    convention as _ensure_workspace) but NEVER the generated
    provider_vault.tf: that file authenticates via the generic auth_login block
    against the very role this harness's job is to create — circular on
    the only occasion this harness ever runs (a cold start)."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_standalone_root=tmp_path / "standalone",
        terraform_binary="/bin/true",
    )
    _seed_platform_auth_standalone(settings.terraform_standalone_root)
    settings.terraform_workdir_root.mkdir(parents=True)
    runner = TerraformRunner(settings)

    workdir = await runner._ensure_platform_auth_workspace()

    assert (workdir / "main.tf").exists()
    backend_tf = workdir / "backend.tf"
    assert backend_tf.exists()
    assert 'backend "s3"' in backend_tf.read_text()
    assert not (workdir / "provider_vault.tf").exists()


@pytest.mark.asyncio
async def test_ensure_platform_auth_workspace_missing_source_raises(tmp_path: Path) -> None:
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_standalone_root=tmp_path / "standalone",
        terraform_binary="/bin/true",
    )
    settings.terraform_workdir_root.mkdir(parents=True)
    runner = TerraformRunner(settings)

    with pytest.raises(TerraformError) as exc_info:
        await runner._ensure_platform_auth_workspace()
    assert exc_info.value.command == "init"
    assert "platform-auth-bootstrap" in exc_info.value.result.stderr


def test_platform_auth_backend_config_state_key_disjoint_from_every_other_workspace() -> None:
    settings = get_settings()
    runner = TerraformRunner(settings)

    key = runner._platform_auth_backend_config()["key"]
    assert key == f"platform/auth-bootstrap/{settings.env}.tfstate"
    assert key != runner._platform_secrets_backend_config(settings.env)["key"]
    assert key != runner._platform_bus_topology_backend_config(settings.env)["key"]
    assert not key.startswith("tenants/")


def test_platform_auth_tfvars_carries_role_mount_namespace_and_sa_name() -> None:
    settings = get_settings()
    runner = TerraformRunner(settings)

    vars_ = runner._platform_auth_tfvars()
    assert vars_ == {
        "pooled_namespace": settings.pneuma_namespace,
        "vault_k8s_auth_mount": settings.vault_k8s_auth_mount,
        "vault_k8s_auth_role": settings.vault_k8s_auth_role,
        "terraformer_service_account_name": settings.terraformer_service_account_name,
    }


@pytest.mark.asyncio
async def test_apply_platform_auth_injects_vault_token_for_apply_only(
    tmp_path: Path,
) -> None:
    """The break-glass root token must reach ONLY the apply subprocess's
    env (via _spawn's extra_env) — never the init call, and never folded
    into _provider_env() where every other invocation would pick it up."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_standalone_root=tmp_path / "standalone",
        terraform_binary="/bin/true",
    )
    _seed_platform_auth_standalone(settings.terraform_standalone_root)
    settings.terraform_workdir_root.mkdir(parents=True)
    runner = TerraformRunner(settings)

    captured_extra_env: list[dict[str, str] | None] = []
    ok = TerraformResult(exit_code=0, stdout="applied", stderr="", outputs={})

    async def _fake_spawn(wd, args, timeout, extra_env=None):  # noqa: ARG001
        captured_extra_env.append(extra_env)
        return ok

    with patch.object(runner, "_spawn", AsyncMock(side_effect=_fake_spawn)):
        result = await runner.apply_platform_auth("s.break-glass-root-token")

    assert result.exit_code == 0
    assert len(captured_extra_env) == 2, "expected init then apply"
    init_env, apply_env = captured_extra_env
    assert init_env is None
    assert apply_env == {"VAULT_TOKEN": "s.break-glass-root-token"}
    assert "VAULT_TOKEN" not in runner._provider_env()


@pytest.mark.asyncio
async def test_apply_platform_auth_wipes_tfvars_on_success_and_failure(
    tmp_path: Path,
) -> None:
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_standalone_root=tmp_path / "standalone",
        terraform_binary="/bin/true",
    )
    _seed_platform_auth_standalone(settings.terraform_standalone_root)
    settings.terraform_workdir_root.mkdir(parents=True)
    runner = TerraformRunner(settings)
    tfvars_path = settings.terraform_workdir_root / "_platform_auth" / "terraform.tfvars.json"

    ok = TerraformResult(exit_code=0, stdout="applied", stderr="", outputs={})
    with patch.object(runner, "_spawn", AsyncMock(return_value=ok)):
        await runner.apply_platform_auth("s.root-token")
    assert not tfvars_path.exists()

    apply_fail = TerraformResult(exit_code=1, stdout="", stderr="denied", outputs={})
    spawn_results = [ok, apply_fail]  # init ok, apply fails

    async def _fake_spawn(*args, **kwargs):
        return spawn_results.pop(0)

    with patch.object(runner, "_spawn", side_effect=_fake_spawn):
        with pytest.raises(TerraformError):
            await runner.apply_platform_auth("s.root-token")
    assert not tfvars_path.exists()


@pytest.mark.asyncio
async def test_apply_platform_auth_propagates_apply_failure(tmp_path: Path) -> None:
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_standalone_root=tmp_path / "standalone",
        terraform_binary="/bin/true",
    )
    _seed_platform_auth_standalone(settings.terraform_standalone_root)
    settings.terraform_workdir_root.mkdir(parents=True)
    runner = TerraformRunner(settings)

    init_ok = TerraformResult(exit_code=0, stdout="", stderr="", outputs={})
    apply_fail = TerraformResult(exit_code=1, stdout="", stderr="permission denied", outputs={})
    spawn_results = [init_ok, apply_fail]

    async def _fake_spawn(*args, **kwargs):
        return spawn_results.pop(0)

    with patch.object(runner, "_spawn", side_effect=_fake_spawn):
        with pytest.raises(TerraformError) as exc_info:
            await runner.apply_platform_auth("s.root-token")
    assert exc_info.value.command == "apply"
    assert "permission denied" in exc_info.value.result.stderr


@pytest.mark.asyncio
async def test_apply_platform_auth_propagates_init_failure(tmp_path: Path) -> None:
    """`_init_platform_auth`'s own exit-code check — distinct from
    `_ensure_platform_auth_workspace`'s missing-source-dir guard above —
    must raise TerraformError("init", ...) and never reach the apply
    step (no root token should ever be spent against a workspace that
    didn't even init)."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_standalone_root=tmp_path / "standalone",
        terraform_binary="/bin/true",
    )
    _seed_platform_auth_standalone(settings.terraform_standalone_root)
    settings.terraform_workdir_root.mkdir(parents=True)
    runner = TerraformRunner(settings)

    init_fail = TerraformResult(
        exit_code=1, stdout="", stderr="Error: failed to get existing workspaces: S3 bucket does not exist", outputs={},
    )
    spawn_calls: list[list[str]] = []

    async def _fake_spawn(wd, args, timeout, extra_env=None):  # noqa: ARG001
        spawn_calls.append(args)
        return init_fail

    with patch.object(runner, "_spawn", side_effect=_fake_spawn):
        with pytest.raises(TerraformError) as exc_info:
            await runner.apply_platform_auth("s.root-token")

    assert exc_info.value.command == "init"
    assert "S3 bucket does not exist" in exc_info.value.result.stderr
    assert len(spawn_calls) == 1, "must not attempt apply after init fails"


# ---------------------------------------------------------------------------
# Transient infrastructure-provider conflict retry (fix/transient-conflict-
# retry) — bounded retry for the live 2026-07-29 failure: two concurrent
# tenant applies (max_concurrent_terraform_runs stays > 1 on purpose) both
# issue CREATE ROLE + GRANT, contending on Postgres's *shared* catalogs
# (pg_authid/pg_shdepend) even though each tenant owns a distinct role and
# schema:
#
#   Error: could not execute revoke query: pq: tuple concurrently updated (XX000)
#     with postgresql_grant.tenant_admin_schema_all
#
# _spawn wraps _spawn_once in a bounded retry loop that only fires when a
# failure's combined stdout+stderr matches a registered
# _TransientConflictSignature (see _TRANSIENT_CONFLICT_SIGNATURES) — never
# for a clean failure — and never past the caller's own timeout budget.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("stdout", "stderr", "expected_name"),
    [
        ("", "Error: pq: tuple concurrently updated (XX000)", "postgres_tuple_concurrently_updated"),
        ("", "ERROR:  could not serialize access (SQLSTATE 40001)", "postgres_serialization_failure"),
        ("", "pq: deadlock_detected", "postgres_deadlock_detected"),
        ("", "pq: DEADLOCK_DETECTED", "postgres_deadlock_detected"),
        ("some stdout mentioning 40P01 inline", "", "postgres_deadlock_detected"),
        (
            "",
            "F ev_epoll1_linux.cc:1121 Check failed: next_worker->state == KICKED",
            "terraform_provider_grpc_epoll1_abort",
        ),
        ("", 'Error: Unsupported argument "bogus_attr" in resource block', None),
    ],
)
def test_match_transient_conflict_recognises_every_seeded_signature(
    stdout: str, stderr: str, expected_name: str | None,
) -> None:
    """Each seeded registry row (Postgres XX000 / 40001 / 40P01, plus the
    gRPC epoll1 fork-safety abort) must be recognised case-insensitively
    from either stdout or stderr — matched by either the raw
    SQLSTATE/vendor code or the condition's human name — and a genuinely
    unrelated failure (bad HCL) must match nothing."""
    signature = runner_mod._match_transient_conflict(stdout, stderr)
    assert (signature.name if signature else None) == expected_name


@pytest.mark.asyncio
async def test_spawn_retries_transient_conflict_and_succeeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    """A `terraform apply` that fails with a matched transient-conflict
    signature must be retried, and a subsequent clean run must succeed —
    the actual live 2026-07-29 failure mode (tenant b6c10c08, concurrent
    CREATE ROLE/GRANT against Postgres's shared catalogs)."""
    import logging

    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/bin/true",
    )
    settings.terraform_workdir_root.mkdir(parents=True)
    workdir = settings.terraform_workdir_root / "ws"
    workdir.mkdir()
    runner = TerraformRunner(settings)

    call_count = {"n": 0}

    class _FlakyThenOkProc:
        def __init__(self, is_success: bool) -> None:
            self.returncode = 0 if is_success else 1
            self._is_success = is_success

        async def communicate(self) -> tuple[bytes, bytes]:
            if self._is_success:
                return b"Apply complete!", b""
            return b"", (
                b"Error: could not execute revoke query: pq: tuple "
                b"concurrently updated (XX000)\n"
                b"  with postgresql_grant.tenant_admin_schema_all"
            )

        def kill(self) -> None:
            pass

        async def wait(self) -> None:
            return None

    async def _fake_exec(*args, **kwargs):  # noqa: ARG001
        call_count["n"] += 1
        return _FlakyThenOkProc(is_success=call_count["n"] > 1)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_exec)

    sleep_calls: list[float] = []

    async def _fast_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", _fast_sleep)

    caplog.set_level(logging.INFO, logger="terraformer.terraform")
    result = await runner._spawn(workdir, ["apply"], timeout=30)

    assert result.exit_code == 0
    assert call_count["n"] == 2, "must retry exactly once after the matched failure"
    assert sleep_calls == [1.0], "backoff before the single retry must be the base 1.0s"
    retry_logs = [
        r for r in caplog.records if "retrying after transient conflict" in r.getMessage()
    ]
    assert len(retry_logs) == 1
    assert "postgres_tuple_concurrently_updated" in retry_logs[0].getMessage()
    assert "attempt=2/3" in retry_logs[0].getMessage()


@pytest.mark.asyncio
async def test_spawn_does_not_retry_clean_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failure whose combined stdout+stderr matches NO registered
    transient-conflict signature (a genuine HCL/schema error) must never
    be retried — turning a fast, permanent failure into 3x the wall-clock
    for nothing would be strictly worse than failing once."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/bin/true",
    )
    settings.terraform_workdir_root.mkdir(parents=True)
    workdir = settings.terraform_workdir_root / "ws"
    workdir.mkdir()
    runner = TerraformRunner(settings)

    call_count = {"n": 0}

    class _CleanFailureProc:
        returncode = 1

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"", b'Error: Unsupported argument "bogus_attr" in resource block'

        def kill(self) -> None:
            pass

        async def wait(self) -> None:
            return None

    async def _fake_exec(*args, **kwargs):  # noqa: ARG001
        call_count["n"] += 1
        return _CleanFailureProc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_exec)

    sleep_calls: list[float] = []

    async def _fast_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", _fast_sleep)

    result = await runner._spawn(workdir, ["apply"], timeout=30)

    assert result.exit_code == 1
    assert call_count["n"] == 1, "a clean (non-matching) failure must not be retried"
    assert sleep_calls == [], "no backoff sleep for a non-retryable failure"


@pytest.mark.asyncio
async def test_spawn_stops_retrying_when_remaining_budget_cannot_fit_another_attempt(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """If the first attempt's own run time already consumed nearly the
    whole caller `timeout`, the retry loop must give up rather than push
    past the caller's budget — retries live INSIDE the same total budget
    queue-wait+run already respects (_effective_run_timeout /
    _spawn_queue_budget), never past it. Uses real (short, ~1.6s) timing
    rather than a monkeypatched clock: the retry wrapper reads
    time.monotonic() both above AND below the nested `_spawn_once` call,
    so clamping the clock for one level would starve the other's own
    internal bookkeeping (see the queue-wait tests' clamp-pattern
    comments above for why that trick doesn't compose across two
    nested budgets)."""
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

    call_count = {"n": 0}

    class _SlowMatchedFailureProc:
        returncode = 1

        async def communicate(self) -> tuple[bytes, bytes]:
            # Consumes most of the 2.0s total `timeout` budget below,
            # leaving too little remaining for backoff (1.0s) + the
            # refuse-to-start floor (1s) a retry attempt would need.
            await asyncio.sleep(1.6)
            return b"", b"pq: tuple concurrently updated (XX000)"

        def kill(self) -> None:
            pass

        async def wait(self) -> None:
            return None

    async def _fake_exec(*args, **kwargs):  # noqa: ARG001
        call_count["n"] += 1
        return _SlowMatchedFailureProc()

    with patch("asyncio.create_subprocess_exec", side_effect=_fake_exec):
        caplog.set_level(logging.INFO, logger="terraformer.terraform")
        result = await runner._spawn(workdir, ["apply"], timeout=2.0)

    assert result.exit_code == 1
    assert call_count["n"] == 1, "must not start a second attempt with insufficient budget left"
    assert any("NOT retrying" in r.getMessage() for r in caplog.records), (
        "must log why it gave up rather than retry silently"
    )


@pytest.mark.asyncio
async def test_spawn_bounds_attempts_at_the_configured_cap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failure that matches a transient-conflict signature on EVERY
    attempt must still stop at _MAX_TRANSIENT_CONFLICT_ATTEMPTS total
    tries, never loop forever. A generous timeout budget isolates the
    attempt cap from the budget-exhaustion guard covered above. Also
    pins the doubling backoff (1s, then 2s) between the 3 attempts."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/bin/true",
    )
    settings.terraform_workdir_root.mkdir(parents=True)
    workdir = settings.terraform_workdir_root / "ws"
    workdir.mkdir()
    runner = TerraformRunner(settings)

    call_count = {"n": 0}

    class _AlwaysMatchedFailureProc:
        returncode = 1

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"", b"pq: tuple concurrently updated (XX000)"

        def kill(self) -> None:
            pass

        async def wait(self) -> None:
            return None

    async def _fake_exec(*args, **kwargs):  # noqa: ARG001
        call_count["n"] += 1
        return _AlwaysMatchedFailureProc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_exec)

    sleep_calls: list[float] = []

    async def _fast_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", _fast_sleep)

    result = await runner._spawn(workdir, ["apply"], timeout=600)

    assert result.exit_code == 1
    assert call_count["n"] == runner_mod._MAX_TRANSIENT_CONFLICT_ATTEMPTS == 3
    assert sleep_calls == [1.0, 2.0], "backoff must double per retry (1s then 2s)"


# ---------------------------------------------------------------------------
# Defect 1 — silent failures: a failed terraform run must log its
# stdout/stderr tail and propagate a meaningful error (2026-08-15, tenant
# 831acdc5: apply failed repeatedly with no visible terraform output
# anywhere in terraformer's logs, and RunTenantReconcile returned no
# detail).

def test_tail_truncates_to_last_n_lines() -> None:
    text = "\n".join(f"line{i}" for i in range(100))
    tail = runner_mod._tail(text, n=5)
    assert tail.splitlines() == [f"line{i}" for i in range(95, 100)]


def test_tail_empty_input_returns_empty() -> None:
    assert runner_mod._tail("") == ""


def test_scrub_secret_shaped_redacts_key_value_pairs() -> None:
    raw = 'token=abcdEFGH12345678 other=short api_key: "sk-1234567890abcd"'
    scrubbed = runner_mod._scrub_secret_shaped(raw)
    assert "abcdEFGH12345678" not in scrubbed
    assert "sk-1234567890abcd" not in scrubbed
    assert "<REDACTED>" in scrubbed
    # "other=short" is below the 8-char minimum and must survive untouched.
    assert "other=short" in scrubbed


@pytest.mark.asyncio
async def test_spawn_once_logs_tail_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/bin/true",
    )
    settings.terraform_workdir_root.mkdir(parents=True)
    workdir = settings.terraform_workdir_root / "ws"
    workdir.mkdir()
    runner = TerraformRunner(settings)

    class _FailingProc:
        returncode = 1

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"", b"Error: bucket already exists! with minio_s3_bucket.tenant_media"

        def kill(self) -> None:
            pass

        async def wait(self) -> None:
            return None

    async def _fake_exec(*args, **kwargs):  # noqa: ARG001
        return _FailingProc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_exec)

    with caplog.at_level("WARNING", logger="terraformer.terraform"):
        result = await runner._spawn_once(workdir, ["apply"], timeout=30)

    assert result.exit_code == 1
    assert any(
        "tf spawn FAILED" in record.message and "bucket already exists" in record.message
        for record in caplog.records
    ), "the terraform stderr tail must land in terraformer's own logs on failure"


def test_terraform_error_message_carries_stderr_tail_and_is_scrubbed() -> None:
    result = TerraformResult(
        exit_code=1,
        stdout="",
        stderr="Error: connection refused password=supersecret1234",
        outputs={},
    )
    exc = TerraformError("apply", result)
    assert "connection refused" in str(exc)
    assert "supersecret1234" not in str(exc)


@pytest.mark.asyncio
async def test_reconcile_persists_last_apply_log_on_failure() -> None:
    settings = get_settings()
    _seed_module(settings.terraform_modules_root)
    runner = TerraformRunner(settings)

    fake_init = TerraformResult(exit_code=0, stdout="", stderr="", outputs={})
    fake_apply = TerraformResult(
        exit_code=1, stdout="", stderr="Error: bucket already exists!", outputs={},
    )
    spawn_results = iter([fake_init, fake_apply])

    async def _fake_spawn(workdir, args, timeout):  # noqa: ARG001
        return next(spawn_results)

    with patch.object(runner, "_spawn", AsyncMock(side_effect=_fake_spawn)):
        with pytest.raises(TerraformError):
            await runner.reconcile(_stub_inputs())

    log_path = runner._workspace_dir("t-001") / "last_apply.log"
    assert log_path.exists()
    assert "bucket already exists" in log_path.read_text()


# ---------------------------------------------------------------------------
# Defect 2 — re-apply drift: a re-apply over a half-provisioned tenant must
# CONVERGE, never fatal (2026-08-15, tenant 831acdc5: `[FATAL] bucket
# already exists! (...-media): with minio_s3_bucket.tenant_media`).

def test_tenant_media_bucket_id_matches_module_naming_convention() -> None:
    inputs = _stub_inputs()
    assert runner_mod._tenant_media_bucket_id(inputs) == "acme-tst-media"


@pytest.mark.asyncio
async def test_import_preexisting_resources_skips_when_already_in_state(
    tmp_path: Path,
) -> None:
    settings = get_settings()
    runner = TerraformRunner(settings)
    workdir = tmp_path / "ws"
    workdir.mkdir()

    calls: list[list[str]] = []
    all_addresses = "\n".join(e.resource_address for e in runner_mod._IMPORT_ON_EXISTS_RESOURCES)

    async def _fake_spawn_once(workdir_, args, timeout, **kwargs):  # noqa: ARG001
        calls.append(args)
        if args == ["state", "list"]:
            return TerraformResult(exit_code=0, stdout=all_addresses + "\n", stderr="", outputs={})
        raise AssertionError("import must not run when every resource is already in state")

    with patch.object(runner, "_spawn_once", AsyncMock(side_effect=_fake_spawn_once)):
        await runner._import_preexisting_resources(workdir, _stub_inputs())

    # ONE bulk `state list` call, no per-resource address filter — the fix
    # this test now guards (2026-08-15 19:01-19:11 incident: 6 separate
    # per-resource probes/tenant queued on the heavy semaphore).
    assert calls == [["state", "list"]]


@pytest.mark.asyncio
async def test_import_preexisting_resources_adopts_pre_existing_bucket(
    tmp_path: Path,
) -> None:
    """The re-apply-drift fix: when the bucket is NOT in state but exists
    against the real provider (the half-applied-first-run scenario),
    `terraform import` must be attempted with the exact bucket ID the
    module itself would compute."""
    settings = get_settings()
    runner = TerraformRunner(settings)
    workdir = tmp_path / "ws"
    workdir.mkdir()

    bucket_import_args: list[list[str]] = []

    async def _fake_spawn_once(workdir_, args, timeout, **kwargs):  # noqa: ARG001
        if args[:2] == ["state", "list"]:
            return TerraformResult(exit_code=1, stdout="", stderr="No instances", outputs={})
        assert args[0] == "import"
        if args[-2] == "minio_s3_bucket.tenant_media":
            bucket_import_args.append(args)
        return TerraformResult(exit_code=0, stdout="Import successful!", stderr="", outputs={})

    with patch.object(runner, "_spawn_once", AsyncMock(side_effect=_fake_spawn_once)):
        await runner._import_preexisting_resources(workdir, _stub_inputs())

    assert bucket_import_args == [
        ["import", "-input=false", "minio_s3_bucket.tenant_media", "acme-tst-media"],
    ]


@pytest.mark.asyncio
async def test_import_preexisting_resources_swallows_not_found_and_lets_apply_create(
    tmp_path: Path,
) -> None:
    """A fresh tenant (bucket genuinely doesn't exist yet): the import
    attempt fails and must NOT raise — the subsequent `apply` creates the
    resource exactly as it did before this fix."""
    settings = get_settings()
    runner = TerraformRunner(settings)
    workdir = tmp_path / "ws"
    workdir.mkdir()

    async def _fake_spawn_once(workdir_, args, timeout, **kwargs):  # noqa: ARG001
        if args[:2] == ["state", "list"]:
            return TerraformResult(exit_code=1, stdout="", stderr="", outputs={})
        return TerraformResult(
            exit_code=1, stdout="", stderr="Cannot import non-existent object", outputs={},
        )

    with patch.object(runner, "_spawn_once", AsyncMock(side_effect=_fake_spawn_once)):
        # Must not raise.
        await runner._import_preexisting_resources(workdir, _stub_inputs())


@pytest.mark.asyncio
async def test_reconcile_attempts_import_before_apply() -> None:
    """Wiring check: reconcile() must run the import-on-exists step before
    dispatching `apply`, so a half-provisioned tenant converges instead of
    hitting the provider's fatal duplicate-resource error."""
    settings = get_settings()
    _seed_module(settings.terraform_modules_root)
    runner = TerraformRunner(settings)

    called = {"import": False}

    async def _fake_import(workdir, inputs):  # noqa: ARG001
        called["import"] = True

    fake_init = TerraformResult(exit_code=0, stdout="", stderr="", outputs={})
    fake_apply = TerraformResult(exit_code=0, stdout="{}", stderr="", outputs={})
    fake_output = TerraformResult(exit_code=0, stdout="{}", stderr="", outputs={})
    spawn_results = iter([fake_init, fake_apply, fake_output])

    async def _fake_spawn(workdir, args, timeout):  # noqa: ARG001
        return next(spawn_results)

    with patch.object(runner, "_import_preexisting_resources", AsyncMock(side_effect=_fake_import)), \
         patch.object(runner, "_spawn", AsyncMock(side_effect=_fake_spawn)):
        await runner.reconcile(_stub_inputs())

    assert called["import"] is True


# ---------------------------------------------------------------------------
# Follow-up sweep (2026-08-15, 17:45 TST): bucket import worked but apply
# then died on kubernetes_service_account.tenant_reader and both
# postgresql_role.* resources — the registry was too narrow. Also: the
# failure-tail log line was empty when grepped live even though
# last_apply.log had the full stderr — an embedded-newline log message
# gets split across separate container-log records.

@pytest.mark.parametrize(
    "resource_address,expected_id",
    [
        ("kubernetes_service_account.tenant_reader", "platform-tst/tenant-acme-reader"),
        ("postgresql_role.tenant_app", "tenant_acme_app"),
        ("postgresql_role.tenant_admin", "tenant_acme_admin"),
        ("rabbitmq_vhost.tenant", "/acme-tst"),
        ("rabbitmq_user.tenant", "tenant_acme"),
        ("minio_s3_bucket.tenant_media", "acme-tst-media"),
    ],
)
def test_import_registry_ids_match_module_naming_convention(
    resource_address: str, expected_id: str,
) -> None:
    entry = next(
        e for e in runner_mod._IMPORT_ON_EXISTS_RESOURCES if e.resource_address == resource_address
    )
    assert entry.resource_id(_stub_inputs()) == expected_id


@pytest.mark.asyncio
async def test_import_preexisting_resources_covers_service_account_and_both_roles(
    tmp_path: Path,
) -> None:
    """The three resources the live 17:45 sweep actually hit after the
    bucket import succeeded — must all be attempted."""
    settings = get_settings()
    runner = TerraformRunner(settings)
    workdir = tmp_path / "ws"
    workdir.mkdir()

    imported: list[str] = []

    async def _fake_spawn_once(workdir_, args, timeout, **kwargs):  # noqa: ARG001
        if args[:2] == ["state", "list"]:
            return TerraformResult(exit_code=1, stdout="", stderr="", outputs={})
        assert args[0] == "import"
        imported.append(args[-2])
        return TerraformResult(exit_code=0, stdout="Import successful!", stderr="", outputs={})

    with patch.object(runner, "_spawn_once", AsyncMock(side_effect=_fake_spawn_once)):
        await runner._import_preexisting_resources(workdir, _stub_inputs())

    assert "kubernetes_service_account.tenant_reader" in imported
    assert "postgresql_role.tenant_app" in imported
    assert "postgresql_role.tenant_admin" in imported


@pytest.mark.asyncio
async def test_expected_import_probe_failures_log_at_debug_not_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    """A fresh tenant's "not found yet" state-list/import probes must NOT
    fire the WARNING "tf spawn FAILED" line — that line is for a genuine
    apply/destroy failure, and every registry entry now probes on every
    apply, so treating expected probe misses as WARNING would bury the
    real signal in per-tenant noise."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/bin/true",
    )
    settings.terraform_workdir_root.mkdir(parents=True)
    workdir = settings.terraform_workdir_root / "ws"
    workdir.mkdir()
    runner = TerraformRunner(settings)

    class _NotFoundProc:
        returncode = 1

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"", b"No instances of the given resource address"

        def kill(self) -> None:
            pass

        async def wait(self) -> None:
            return None

    async def _fake_exec(*args, **kwargs):  # noqa: ARG001
        return _NotFoundProc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_exec)

    with caplog.at_level("DEBUG", logger="terraformer.terraform"):
        await runner._spawn_once(
            workdir, ["state", "list", "postgresql_role.tenant_app"],
            timeout=30, failure_expected=True,
        )

    assert not any(r.levelname == "WARNING" for r in caplog.records)
    assert any(r.levelname == "DEBUG" and "failed (expected)" in r.message for r in caplog.records)


def test_flatten_for_log_collapses_multiline_tail_to_one_line() -> None:
    multiline = "Error: role already exists\n\n  with postgresql_role.tenant_app,\n  on postgres.tf line 20:\n"
    flattened = runner_mod._flatten_for_log(multiline)
    assert "\n" not in flattened
    assert "Error: role already exists" in flattened
    assert "postgresql_role.tenant_app" in flattened


@pytest.mark.asyncio
async def test_spawn_once_failure_log_is_single_line_and_carries_stderr_only_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    """Regression for the 17:45 sweep: `tail=` logged empty when grepped
    live, even though last_apply.log had the full stderr — because the
    old log message embedded raw newlines, which the container log driver
    splits into separate, unattributed records. The formatted log record
    itself must now be a single line, and a STDERR-ONLY failure (no
    stdout at all — the realistic apply-error shape) must still populate
    a non-empty scrubbed tail."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/bin/true",
    )
    settings.terraform_workdir_root.mkdir(parents=True)
    workdir = settings.terraform_workdir_root / "ws"
    workdir.mkdir()
    runner = TerraformRunner(settings)

    class _StderrOnlyFailure:
        returncode = 1

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"", (
                b"Error: creating ServiceAccount: serviceaccounts "
                b"\"tenant-acme-reader\" already exists\n\n"
                b"  with kubernetes_service_account.tenant_reader,\n"
                b"  on eso.tf line 17:\n"
            )

        def kill(self) -> None:
            pass

        async def wait(self) -> None:
            return None

    async def _fake_exec(*args, **kwargs):  # noqa: ARG001
        return _StderrOnlyFailure()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_exec)

    with caplog.at_level("WARNING", logger="terraformer.terraform"):
        result = await runner._spawn_once(workdir, ["apply"], timeout=30)

    assert result.exit_code == 1
    assert result.stdout == ""
    assert "already exists" in result.stderr
    failure_records = [r for r in caplog.records if "tf spawn FAILED" in r.message]
    assert len(failure_records) == 1
    record = failure_records[0]
    assert "\n" not in record.message, "the formatted log message must be a single line"
    assert "already exists" in record.message
    assert "tenant-acme-reader" in record.message
    assert record.message.rstrip().endswith(("already exists", "17:")) or "17:" in record.message


def test_terraform_error_carries_stderr_only_tail() -> None:
    """gRPC-propagated error string (grpc_server.py forwards str(exc)) must
    carry the real cause even when stdout is completely empty — the
    common apply-failure shape."""
    result = TerraformResult(
        exit_code=1,
        stdout="",
        stderr="Error: role \"tenant_acme_app\" already exists (SQLSTATE 42710)",
        outputs={},
    )
    exc = TerraformError("apply", result)
    assert "tenant_acme_app" in str(exc)
    assert "42710" in str(exc)


# ---------------------------------------------------------------------------
# Follow-up sweep (2026-08-15 19:00 TST): imports worked (SA + both postgres
# roles adopted), but every apply now costs up to 6 state-list + import
# spawns/tenant, ALL queued behind the heavy max_concurrent_terraform_runs=2
# semaphore alongside every apply itself. 4 concurrent tenants' probes
# queued 19:01→19:11 — the whole ~590s RunTenantReconcile budget burned on
# queueing, apply never started (no last_apply.log), and each stall
# consumed a reconcile attempt (7/10 by the time this was caught).

@pytest.mark.asyncio
async def test_import_preexisting_resources_issues_exactly_one_bulk_state_list(
    tmp_path: Path,
) -> None:
    """Root fix: ONE `terraform state list` (no address filter) replaces
    one per-registry-entry probe — collapses ~6 spawns/tenant to 1 +
    (missing count), and to just 1 once a tenant has converged."""
    settings = get_settings()
    runner = TerraformRunner(settings)
    workdir = tmp_path / "ws"
    workdir.mkdir()

    state_list_calls = 0

    async def _fake_spawn_once(workdir_, args, timeout, **kwargs):  # noqa: ARG001
        nonlocal state_list_calls
        if args == ["state", "list"]:
            state_list_calls += 1
            all_addresses = "\n".join(
                e.resource_address for e in runner_mod._IMPORT_ON_EXISTS_RESOURCES
            )
            return TerraformResult(exit_code=0, stdout=all_addresses, stderr="", outputs={})
        raise AssertionError("no import expected — every registry entry reported in state")

    with patch.object(runner, "_spawn_once", AsyncMock(side_effect=_fake_spawn_once)):
        await runner._import_preexisting_resources(workdir, _stub_inputs())

    assert state_list_calls == 1


@pytest.mark.asyncio
async def test_state_addresses_probe_uses_light_semaphore() -> None:
    """The bulk state-list probe must route through `light=True` — the
    wide read-only semaphore — not the heavy apply/import one, or 4
    concurrent tenants' probes queue behind the same limit=2 slots as
    every apply (the exact 19:01-19:11 stall)."""
    settings = get_settings()
    runner = TerraformRunner(settings)
    workdir = Path("/tmp/nonexistent-ws")

    captured_kwargs: dict = {}

    async def _fake_spawn_once(workdir_, args, timeout, **kwargs):  # noqa: ARG001
        captured_kwargs.update(kwargs)
        return TerraformResult(exit_code=0, stdout="", stderr="", outputs={})

    with patch.object(runner, "_spawn_once", AsyncMock(side_effect=_fake_spawn_once)):
        await runner._state_addresses(workdir)

    assert captured_kwargs.get("light") is True


def test_read_semaphore_is_wider_than_and_independent_of_heavy_semaphore() -> None:
    settings = Settings(
        terraform_workdir_root=Path("/tmp/wd"),
        terraform_modules_root=Path("/tmp/modules"),
        max_concurrent_terraform_runs=2,
    )
    runner = TerraformRunner(settings)
    heavy = runner._spawn_semaphore()
    light = runner._read_spawn_semaphore()
    assert heavy is not light
    assert light._value == 2 * runner_mod.TerraformRunner._READ_SEMAPHORE_MULTIPLIER
    assert light._value > heavy._value


@pytest.mark.asyncio
async def test_light_spawn_bypasses_heavy_semaphore_when_heavy_is_fully_held(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A light (read-only) spawn must complete even while every heavy slot
    is held by concurrent applies — proves the semaphores are genuinely
    independent, not just differently sized."""
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

    class _FastProc:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"", b""

        def kill(self) -> None:
            pass

        async def wait(self) -> None:
            return None

    async def _fast_exec(*args, **kwargs):  # noqa: ARG001
        return _FastProc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fast_exec)

    # Hold the ONE heavy slot for the duration of this test.
    heavy_sem = runner._spawn_semaphore()
    await heavy_sem.acquire()
    try:
        result = await asyncio.wait_for(
            runner._spawn_once(workdir, ["state", "list"], timeout=5, light=True),
            timeout=1,
        )
    finally:
        heavy_sem.release()

    assert result.exit_code == 0


# ---------------------------------------------------------------------------
# gRPC epoll1 fork-safety abort — crash-signature classification, process-
# group kill, and per-tenant cross-process single-flight (live 2026-08-18,
# tenant 72f36de4: `terraform init`/`apply` intermittently aborts with
#   F ev_epoll1_linux.cc:1121 Check failed: next_worker->state == KICKED
# — exit=-6/SIGABRT). True crash source is THIS process's own live
# `grpc.aio.server()` (grpc_server.py) sharing an OS process with every
# `asyncio.create_subprocess_exec` fork in terraform_runner.py — see
# _TRANSIENT_CONFLICT_SIGNATURES' `terraform_provider_grpc_epoll1_abort`
# row and _kill_process_group's docstring for the full incident writeup.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_spawn_retries_epoll1_abort_signature_with_its_own_attempts_and_backoff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The epoll1-abort signature gets its OWN retry shape (5 attempts,
    10/20/30/30s backoff — capped at 30s) — NOT the Postgres rows' shared
    3-attempts/1-2s shape — because a fork-timing race clears on a
    different timescale than a sub-second catalog lock. Fails 4 times
    with the exact live stderr signature, succeeds on the 5th."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/bin/true",
    )
    settings.terraform_workdir_root.mkdir(parents=True)
    workdir = settings.terraform_workdir_root / "ws"
    workdir.mkdir()
    runner = TerraformRunner(settings)

    call_count = {"n": 0}

    class _EpollAbortThenOkProc:
        def __init__(self, is_success: bool) -> None:
            self.returncode = 0 if is_success else -6
            self._is_success = is_success

        async def communicate(self) -> tuple[bytes, bytes]:
            if self._is_success:
                return b"Apply complete!", b""
            return (
                b"",
                b"F ev_epoll1_linux.cc:1121 Check failed: next_worker->state == KICKED",
            )

        def kill(self) -> None:
            pass

        async def wait(self) -> None:
            return None

    async def _fake_exec(*args, **kwargs):  # noqa: ARG001
        call_count["n"] += 1
        return _EpollAbortThenOkProc(is_success=call_count["n"] > 4)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_exec)

    sleep_calls: list[float] = []

    async def _fast_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", _fast_sleep)

    result = await runner._spawn(workdir, ["init"], timeout=600)

    assert result.exit_code == 0
    assert call_count["n"] == 5, "must retry 4 times (5 attempts total) before succeeding"
    assert sleep_calls == [10.0, 20.0, 30.0, 30.0], "backoff must double then cap at 30s"


@pytest.mark.asyncio
async def test_spawn_stops_retrying_epoll1_abort_after_5_attempts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A run that NEVER clears must stop at the epoll1 row's own 5-attempt
    cap and surface the last failure — never retry forever."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/bin/true",
    )
    settings.terraform_workdir_root.mkdir(parents=True)
    workdir = settings.terraform_workdir_root / "ws"
    workdir.mkdir()
    runner = TerraformRunner(settings)

    call_count = {"n": 0}

    class _AlwaysAbortsProc:
        returncode = -6

        async def communicate(self) -> tuple[bytes, bytes]:
            return (
                b"",
                b"F ev_epoll1_linux.cc:1121 Check failed: next_worker->state == KICKED",
            )

        def kill(self) -> None:
            pass

        async def wait(self) -> None:
            return None

    async def _fake_exec(*args, **kwargs):  # noqa: ARG001
        call_count["n"] += 1
        return _AlwaysAbortsProc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_exec)
    monkeypatch.setattr(asyncio, "sleep", AsyncMock())

    result = await runner._spawn(workdir, ["init"], timeout=600)

    assert result.exit_code == -6
    assert call_count["n"] == 5, "must stop after exactly 5 attempts, never retry forever"


def test_kill_process_group_kills_the_whole_group_when_pid_is_real(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With a real PID, `_kill_process_group` must `os.killpg` the WHOLE
    group (SIGKILL) — NOT just `proc.kill()` the immediate PID — so a
    `terraform` provider-plugin grandchild (postgresql/rabbitmq/minio/
    vault/kubernetes, forked over HashiCorp's go-plugin protocol) can
    never survive as an orphan past a cancelled/timed-out apply. This is
    the actual live 2026-08-18 same-tenant collision fix (tenant
    72f36de4) — see the function's own docstring."""
    import signal as signal_mod

    killpg_calls: list[tuple[int, int]] = []
    kill_calls: list[bool] = []

    class _RealPidProc:
        pid = 4321

        def kill(self) -> None:
            kill_calls.append(True)

    monkeypatch.setattr(runner_mod.os, "getpgid", lambda pid: pid)
    monkeypatch.setattr(
        runner_mod.os, "killpg", lambda pgid, sig: killpg_calls.append((pgid, sig))
    )

    runner_mod._kill_process_group(_RealPidProc())

    assert killpg_calls == [(4321, signal_mod.SIGKILL)]
    assert kill_calls == [], "must not ALSO fall back to proc.kill() when the group-kill succeeded"


def test_kill_process_group_falls_back_to_proc_kill_without_a_real_pid() -> None:
    """A `proc` with no real `.pid` (every existing fake-process test
    double in this file) must fall straight back to the always-correct
    `proc.kill()` — the group-kill enhancement must never regress the
    guaranteed base behaviour."""
    kill_calls: list[bool] = []

    class _NoPidProc:
        def kill(self) -> None:
            kill_calls.append(True)

    runner_mod._kill_process_group(_NoPidProc())

    assert kill_calls == [True]


def test_tenant_lease_cm_is_noop_without_incluster_service_account(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No projected ServiceAccount token (every unit test's environment,
    and a local dev shell) — `_tenant_lease_cm` must return the no-op CM,
    so `reconcile()`/`destroy()` fall back to `_lock_for`'s process-local
    lock alone, exactly today's behaviour."""
    missing = tmp_path / "does-not-exist"
    monkeypatch.setattr(runner_mod, "_KUBE_SA_TOKEN_PATH", missing)
    monkeypatch.setattr(runner_mod, "_KUBE_SA_CA_CERT_PATH", missing)

    runner = TerraformRunner(get_settings())
    cm = runner._tenant_lease_cm("t-001", 600)

    assert cm.__class__.__name__ == "_AsyncGeneratorContextManager"


def test_tenant_lease_cm_returns_kube_lease_mutex_when_incluster(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A projected ServiceAccount token present (the in-cluster case) —
    `_tenant_lease_cm` must hand back a real per-tenant `KubeLeaseMutex`,
    named so a DIFFERENT tenant can never collide on the same Lease. The
    WAITER budget covers the whole plan/apply cycle (not just the timed
    apply step — see _TENANT_LEASE_MARGIN_SECONDS's comment); the lease
    itself is short and renewed, so a holder that dies mid-apply frees
    the tenant within seconds (TST 2026-09-06 — see the
    _TENANT_LEASE_DURATION_SECONDS comment)."""
    from services.terraformer.src.kube_lease_mutex import KubeLeaseMutex

    token = tmp_path / "token"
    ca = tmp_path / "ca.crt"
    token.write_text("fake-token")
    ca.write_text("fake-ca")
    monkeypatch.setattr(runner_mod, "_KUBE_SA_TOKEN_PATH", token)
    monkeypatch.setattr(runner_mod, "_KUBE_SA_CA_CERT_PATH", ca)
    monkeypatch.setenv("PNEUMA_NAMESPACE", "platform-tst")

    runner = TerraformRunner(get_settings())
    cm = runner._tenant_lease_cm("t-001", 600)

    assert isinstance(cm, KubeLeaseMutex)
    assert cm.name == "tf-tenant-t-001"
    assert cm.acquire_timeout_seconds == 600 + runner_mod._TENANT_LEASE_MARGIN_SECONDS
    assert cm.lease_duration_seconds == runner_mod._TENANT_LEASE_DURATION_SECONDS
    assert cm.renew_interval_seconds == runner_mod._TENANT_LEASE_RENEW_SECONDS
    # A dead holder must free the tenant well inside the 60s workspace
    # SLO, and a live one must get several renewal attempts per duration
    # so one transient API failure cannot cost it the lease.
    assert cm.lease_duration_seconds <= 60
    assert cm.lease_duration_seconds >= 4 * cm.renew_interval_seconds
    assert cm.lease_duration_seconds < cm.acquire_timeout_seconds


@pytest.mark.asyncio
async def test_reconcile_serializes_two_runner_instances_via_tenant_lease(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cross-process single-flight regression guard (live 2026-08-18,
    tenant 72f36de4 — see _kill_process_group's docstring): TWO SEPARATE
    `TerraformRunner` instances — each with its OWN process-local
    `_locks` dict, simulating two workers/pod-restarts that share no
    in-memory state — must still serialise a `reconcile()` for the SAME
    tenant_id via the Kubernetes Lease `_tenant_lease_cm` acquires. The
    second instance's apply must NOT start while the first still holds
    the lease, and must proceed once the first releases it.

    Exercises the REAL `KubeLeaseMutex` acquire/steal/release HTTP calls
    against an in-memory fake `coordination.k8s.io/v1` Leases API (only
    GET/POST/PUT on one named Lease are needed) rather than mocking
    `KubeLeaseMutex` away — this is also the first test coverage
    `kube_lease_mutex.py` has ever had."""
    import httpx

    from services.terraformer.src.kube_lease_mutex import KubeLeaseMutex

    token = tmp_path / "token"
    ca = tmp_path / "ca.crt"
    token.write_text("fake-token")
    ca.write_text("fake-ca")
    monkeypatch.setattr(runner_mod, "_KUBE_SA_TOKEN_PATH", token)
    monkeypatch.setattr(runner_mod, "_KUBE_SA_CA_CERT_PATH", ca)
    monkeypatch.setenv("PNEUMA_NAMESPACE", "platform-tst")
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.43.0.1")

    leases: dict[str, dict] = {}

    def _handler(request: httpx.Request) -> httpx.Response:
        name = request.url.path.rsplit("/", 1)[-1]
        if request.method == "GET":
            if name in leases:
                return httpx.Response(200, json=leases[name])
            return httpx.Response(404, json={})
        if request.method == "POST":
            body = json.loads(request.content)
            leases[body["metadata"]["name"]] = body
            return httpx.Response(201, json=body)
        if request.method == "PUT":
            body = json.loads(request.content)
            leases[name] = body
            return httpx.Response(200, json=body)
        raise AssertionError(f"unexpected method {request.method}")  # pragma: no cover

    def _fake_client(self: KubeLeaseMutex) -> httpx.AsyncClient:  # noqa: ARG001
        return httpx.AsyncClient(transport=httpx.MockTransport(_handler))

    monkeypatch.setattr(KubeLeaseMutex, "_client", _fake_client)

    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_binary="/bin/true",
    )
    settings.terraform_workdir_root.mkdir(parents=True)
    _seed_module(settings.terraform_modules_root)

    runner_a = TerraformRunner(settings)
    runner_b = TerraformRunner(settings)

    order: list[str] = []
    first_started = asyncio.Event()
    release_first = asyncio.Event()

    async def _fake_spawn_a(workdir, args, timeout):  # noqa: ARG001
        order.append("a-start")
        first_started.set()
        await release_first.wait()
        order.append("a-end")
        return TerraformResult(exit_code=0, stdout="{}", stderr="", outputs={})

    async def _fake_spawn_b(workdir, args, timeout):  # noqa: ARG001
        order.append("b-start")
        return TerraformResult(exit_code=0, stdout="{}", stderr="", outputs={})

    with patch.object(runner_a, "_spawn", AsyncMock(side_effect=_fake_spawn_a)), \
         patch.object(runner_b, "_spawn", AsyncMock(side_effect=_fake_spawn_b)):
        task_a = asyncio.ensure_future(runner_a.reconcile(_stub_inputs()))
        await asyncio.wait_for(first_started.wait(), timeout=5)

        task_b = asyncio.ensure_future(runner_b.reconcile(_stub_inputs()))
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(task_b), timeout=0.5)
        assert "b-start" not in order, (
            "the second runner instance must not start its apply while the "
            "first still holds the tenant Lease"
        )

        release_first.set()
        await asyncio.wait_for(task_a, timeout=5)
        await asyncio.wait_for(task_b, timeout=10)

    assert order[0] == "a-start"
    assert order.index("b-start") > order.index("a-end"), (
        "the second runner must only start AFTER the first released the lease"
    )


# ---------------------------------------------------------------------------
# Platform-resources import-on-exists (2026-08-19 defect): platform-
# resources/<env>.tfstate applies wedge permanently on
# `pq: role "activepieces_app" already exists (42710)` when a prior
# apply created the role but died before the state write — mirrors the
# tenant registry's design exactly (see _IMPORT_ON_EXISTS_RESOURCES /
# _import_preexisting_resources above), extended to this workspace's
# CREATE-ONLY resources (the AP role, the AP database, and every
# inter-service-HMAC pair KV secret).

def test_platform_resources_import_entries_cover_role_and_database() -> None:
    entries = runner_mod._platform_resources_import_entries("tst")
    by_address = {e.resource_address: e.resource_id for e in entries}
    assert by_address["postgresql_role.activepieces_app"] == "activepieces_app"
    assert by_address["postgresql_database.activepieces"] == "activepieces"


def test_platform_resources_import_entries_cover_every_hmac_pair() -> None:
    entries = runner_mod._platform_resources_import_entries("tst")
    addresses = {e.resource_address for e in entries}
    for pair in runner_mod._INTER_SERVICE_HMAC_PAIRS:
        assert f'vault_kv_secret_v2.inter_service_hmac["{pair}"]' in addresses
    # One row generates N addresses (design-for-N) — not a hardcoded
    # one-off for the single role the live incident hit.
    assert len(addresses) == 2 + len(runner_mod._INTER_SERVICE_HMAC_PAIRS)


def test_platform_resources_hmac_import_id_matches_vault_kv_convention() -> None:
    entries = runner_mod._platform_resources_import_entries("tst")
    entry = next(
        e for e in entries
        if e.resource_address == 'vault_kv_secret_v2.inter_service_hmac["brain-brain"]'
    )
    assert entry.resource_id == "pneuma/infra/inter-service-hmac/brain-brain"


@pytest.mark.asyncio
async def test_import_preexisting_platform_resources_skips_when_already_in_state(
    tmp_path: Path,
) -> None:
    settings = get_settings()
    runner = TerraformRunner(settings)
    workdir = tmp_path / "ws"
    workdir.mkdir()

    all_addresses = "\n".join(
        e.resource_address for e in runner_mod._platform_resources_import_entries("tst")
    )
    calls: list[list[str]] = []

    async def _fake_spawn_once(workdir_, args, timeout, **kwargs):  # noqa: ARG001
        calls.append(args)
        if args == ["state", "list"]:
            return TerraformResult(exit_code=0, stdout=all_addresses + "\n", stderr="", outputs={})
        raise AssertionError("import must not run when every resource is already in state")

    with patch.object(runner, "_spawn_once", AsyncMock(side_effect=_fake_spawn_once)), \
         patch.object(runner, "_platform_resources_extra_env", lambda: {}):
        await runner._import_preexisting_platform_resources(workdir, "tst")

    assert calls == [["state", "list"]]


@pytest.mark.asyncio
async def test_import_preexisting_platform_resources_adopts_pre_existing_role(
    tmp_path: Path,
) -> None:
    """The live defect: `activepieces_app` created by a prior partial
    apply but never recorded in state must be imported with the exact ID
    the module itself would compute (the role name)."""
    settings = get_settings()
    runner = TerraformRunner(settings)
    workdir = tmp_path / "ws"
    workdir.mkdir()

    imported: list[list[str]] = []

    async def _fake_spawn_once(workdir_, args, timeout, **kwargs):  # noqa: ARG001
        if args[:2] == ["state", "list"]:
            return TerraformResult(exit_code=1, stdout="", stderr="No instances", outputs={})
        assert args[0] == "import"
        # extra_env must be threaded through so the postgresql provider
        # can actually authenticate the import call.
        assert kwargs.get("extra_env") == {"TF_VAR_pg_host": "stub"}
        imported.append(args)
        return TerraformResult(exit_code=0, stdout="Import successful!", stderr="", outputs={})

    with patch.object(runner, "_spawn_once", AsyncMock(side_effect=_fake_spawn_once)), \
         patch.object(
             runner, "_platform_resources_extra_env", lambda: {"TF_VAR_pg_host": "stub"}
         ):
        await runner._import_preexisting_platform_resources(workdir, "tst")

    role_imports = [a for a in imported if a[-2] == "postgresql_role.activepieces_app"]
    assert role_imports == [
        ["import", "-input=false", "postgresql_role.activepieces_app", "activepieces_app"],
    ]


@pytest.mark.asyncio
async def test_import_preexisting_platform_resources_swallows_not_found(
    tmp_path: Path,
) -> None:
    """A fresh cluster (nothing pre-exists): every import attempt fails
    and must NOT raise — the subsequent `apply` creates everything fresh,
    exactly as before this fix."""
    settings = get_settings()
    runner = TerraformRunner(settings)
    workdir = tmp_path / "ws"
    workdir.mkdir()

    async def _fake_spawn_once(workdir_, args, timeout, **kwargs):  # noqa: ARG001
        if args[:2] == ["state", "list"]:
            return TerraformResult(exit_code=1, stdout="", stderr="", outputs={})
        return TerraformResult(
            exit_code=1, stdout="", stderr="Cannot import non-existent object", outputs={},
        )

    with patch.object(runner, "_spawn_once", AsyncMock(side_effect=_fake_spawn_once)), \
         patch.object(runner, "_platform_resources_extra_env", lambda: {}):
        # Must not raise.
        await runner._import_preexisting_platform_resources(workdir, "tst")


@pytest.mark.asyncio
async def test_reconcile_platform_resources_attempts_import_before_apply(
    tmp_path: Path,
) -> None:
    """Wiring check: reconcile_platform_resources() must run the
    import-on-exists step before dispatching `apply`, mirroring
    test_reconcile_attempts_import_before_apply for the tenant path."""
    settings = Settings(
        terraform_workdir_root=tmp_path / "wd",
        terraform_modules_root=tmp_path / "modules",
        terraform_standalone_root=tmp_path / "standalone",
        terraform_binary="/bin/true",
    )
    standalone_src = settings.terraform_standalone_root / "platform-resources-apply"
    standalone_src.mkdir(parents=True)
    (standalone_src / "main.tf").write_text("# stub\n")
    settings.terraform_workdir_root.mkdir(parents=True)
    runner = TerraformRunner(settings)

    called = {"import": False}

    async def _fake_import(workdir, env):  # noqa: ARG001
        called["import"] = True

    ok = TerraformResult(exit_code=0, stdout="{}", stderr="", outputs={})

    with patch.object(
        runner, "_import_preexisting_platform_resources", AsyncMock(side_effect=_fake_import)
    ), patch.object(runner, "_spawn", AsyncMock(return_value=ok)), \
       patch.object(runner, "_output_json", AsyncMock(return_value={})), \
       patch.object(runner, "_platform_resources_extra_env", lambda: {}):
        await runner.reconcile_platform_resources(PlatformResourcesInputs(env="tst"))

    assert called["import"] is True
