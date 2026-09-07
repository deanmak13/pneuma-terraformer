"""Tests for openbao_bootstrap.ensure_platform_auth — the converge flow
that replaces the expired static OPENBAO_ADMIN_TOKEN AND proves the
kubernetes-auth role's POLICY (not just its existence) is current with
git on every boot (see the module docstring, INCIDENT 2, for the 20-day
inert-policy defect this replaces). All OpenBao/Kubernetes HTTP calls are
mocked via respx — never live; `plan_platform_auth`/`apply_platform_auth`
are mocked on the runner — never a real terraform subprocess.
"""

from __future__ import annotations

import base64
import logging
from unittest.mock import AsyncMock, patch

import httpx
import pytest
import respx

from services.terraformer.src import k8s_api, openbao_bootstrap
from services.terraformer.src.openbao_bootstrap import (
    PlatformAuthBootstrapError,
    ensure_platform_auth,
)
from services.terraformer.src.settings import Settings
from services.terraformer.src.terraform_runner import (
    TerraformError,
    TerraformResult,
    TerraformRunner,
)
from tests.conftest import DUMMY_CA_PEM as _DUMMY_CA_PEM

_VAULT_ADDR = "http://openbao.openbao.test:8200"
_LOGIN_URL = f"{_VAULT_ADDR}/v1/auth/kubernetes/login"
_ATTEMPT_URL = f"{_VAULT_ADDR}/v1/sys/generate-root/attempt"
_UPDATE_URL = f"{_VAULT_ADDR}/v1/sys/generate-root/update"
_DECODE_URL = f"{_VAULT_ADDR}/v1/sys/decode-token"
_REVOKE_URL = f"{_VAULT_ADDR}/v1/auth/token/revoke-self"

# The client_token a successful kubernetes-auth login mints — distinct
# from the break-glass root token ("s.root-token-xyz") so assertions can
# tell which token traveled with which call.
_K8S_TOKEN = "s.k8s-minted"


def _settings(**overrides) -> Settings:
    return Settings(vault_addr=_VAULT_ADDR, **overrides)


def _login_ok(client_token: str = _K8S_TOKEN) -> httpx.Response:
    return httpx.Response(200, json={"auth": {"client_token": client_token}})


def _fake_jwt(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    token_path = tmp_path / "sa-token"
    token_path.write_text("fake-sa-jwt")
    monkeypatch.setattr(k8s_api, "SA_TOKEN_PATH", token_path)


def _fake_unseal_secret(monkeypatch: pytest.MonkeyPatch, count: int = 3) -> None:
    shares = {f"UNSEAL_KEY_{i}": f"share-{i}" for i in range(1, count + 1)}
    monkeypatch.setattr(
        openbao_bootstrap.k8s_api,
        "read_namespaced_secret",
        AsyncMock(return_value=shares),
    )


# ---------------------------------------------------------------------------
# Steady state — login succeeds AND the currency plan is clean: no-op, no
# root token ever minted.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_noop_only_when_login_ok_and_plan_clean(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    _fake_jwt(monkeypatch, tmp_path)
    settings = _settings()
    runner = TerraformRunner(settings)

    login_route = respx.post(_LOGIN_URL).mock(return_value=_login_ok())
    revoke_route = respx.post(_REVOKE_URL).mock(return_value=httpx.Response(204))
    # No generate-root/decode route registered at all — respx raises if
    # any of them is hit, proving no root token is ever minted on this path.

    with (
        patch.object(runner, "plan_platform_auth", AsyncMock(return_value=0)) as plan_mock,
        patch.object(runner, "apply_platform_auth", AsyncMock()) as apply_mock,
    ):
        action = await ensure_platform_auth(settings, runner)

    assert action == "noop_role_and_policy_current"
    assert login_route.call_count == 1
    plan_mock.assert_awaited_once_with(_K8S_TOKEN)
    apply_mock.assert_not_called()
    assert revoke_route.call_count == 1
    assert revoke_route.calls.last.request.headers["X-Vault-Token"] == _K8S_TOKEN


# ---------------------------------------------------------------------------
# Login succeeds but the plan proves drift (exit 2) or cannot even run
# (any other non-zero) — both must break glass, apply, and re-prove.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_login_ok_but_plan_drift_must_break_glass(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    _fake_jwt(monkeypatch, tmp_path)
    _fake_unseal_secret(monkeypatch)
    settings = _settings()
    runner = TerraformRunner(settings)

    login_route = respx.post(_LOGIN_URL).mock(return_value=_login_ok())
    respx.post(_ATTEMPT_URL).mock(
        return_value=httpx.Response(200, json={"nonce": "n-1", "otp": "otp-value"})
    )
    respx.post(_UPDATE_URL).mock(
        return_value=httpx.Response(200, json={"complete": True, "encoded_token": "encoded-abc"})
    )
    respx.post(_DECODE_URL).mock(
        return_value=httpx.Response(200, json={"data": {"token": "s.root-token-xyz"}})
    )
    respx.post(_REVOKE_URL).mock(return_value=httpx.Response(204))

    apply_result = TerraformResult(exit_code=0, stdout="applied", stderr="", outputs={})
    with (
        patch.object(runner, "plan_platform_auth", AsyncMock(side_effect=[2, 0])) as plan_mock,
        patch.object(
            runner, "apply_platform_auth", AsyncMock(return_value=apply_result)
        ) as apply_mock,
    ):
        action = await ensure_platform_auth(settings, runner)

    assert action == "break_glass_applied"
    assert login_route.call_count == 2
    apply_mock.assert_awaited_once_with("s.root-token-xyz")
    assert plan_mock.await_count == 2


@pytest.mark.asyncio
@respx.mock
async def test_login_ok_but_plan_error_must_break_glass(
    monkeypatch: pytest.MonkeyPatch, tmp_path, caplog: pytest.LogCaptureFixture,
) -> None:
    _fake_jwt(monkeypatch, tmp_path)
    _fake_unseal_secret(monkeypatch)
    settings = _settings()
    runner = TerraformRunner(settings)

    respx.post(_LOGIN_URL).mock(return_value=_login_ok())
    respx.post(_ATTEMPT_URL).mock(
        return_value=httpx.Response(200, json={"nonce": "n-1", "otp": "otp-value"})
    )
    respx.post(_UPDATE_URL).mock(
        return_value=httpx.Response(200, json={"complete": True, "encoded_token": "encoded-abc"})
    )
    respx.post(_DECODE_URL).mock(
        return_value=httpx.Response(200, json={"data": {"token": "s.root-token-xyz"}})
    )
    respx.post(_REVOKE_URL).mock(return_value=httpx.Response(204))

    apply_result = TerraformResult(exit_code=0, stdout="applied", stderr="", outputs={})
    with (
        patch.object(runner, "plan_platform_auth", AsyncMock(side_effect=[1, 0])),
        patch.object(runner, "apply_platform_auth", AsyncMock(return_value=apply_result)),
        caplog.at_level(logging.WARNING, logger="terraformer.openbao_bootstrap"),
    ):
        action = await ensure_platform_auth(settings, runner)

    assert action == "break_glass_applied"
    assert any("cannot be proven" in r.message.lower() for r in caplog.records)


@pytest.mark.asyncio
@respx.mock
async def test_reproof_runs_plan_again_and_raises_when_still_drifted(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    """The post-apply re-proof must run its OWN plan (never assume the
    apply fixed things) and, if that second plan still reports drift,
    raise rather than loop — bounded by construction to one apply."""
    _fake_jwt(monkeypatch, tmp_path)
    _fake_unseal_secret(monkeypatch)
    settings = _settings()
    runner = TerraformRunner(settings)

    respx.post(_LOGIN_URL).mock(return_value=_login_ok())
    respx.post(_ATTEMPT_URL).mock(
        return_value=httpx.Response(200, json={"nonce": "n-1", "otp": "otp-value"})
    )
    respx.post(_UPDATE_URL).mock(
        return_value=httpx.Response(200, json={"complete": True, "encoded_token": "encoded-abc"})
    )
    respx.post(_DECODE_URL).mock(
        return_value=httpx.Response(200, json={"data": {"token": "s.root-token-xyz"}})
    )
    respx.post(_REVOKE_URL).mock(return_value=httpx.Response(204))

    apply_result = TerraformResult(exit_code=0, stdout="applied", stderr="", outputs={})
    with (
        patch.object(runner, "plan_platform_auth", AsyncMock(side_effect=[2, 2])) as plan_mock,
        patch.object(
            runner, "apply_platform_auth", AsyncMock(return_value=apply_result)
        ) as apply_mock,
    ):
        with pytest.raises(PlatformAuthBootstrapError, match="still reports"):
            await ensure_platform_auth(settings, runner)

    assert plan_mock.await_count == 2
    assert apply_mock.await_count == 1


# ---------------------------------------------------------------------------
# The k8s-auth-minted token must be revoked on every path that mints one —
# clean-plan no-op AND break-glass re-proof — and never appear in a log.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_minted_k8s_token_is_revoked_on_every_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path, caplog: pytest.LogCaptureFixture,
) -> None:
    _fake_jwt(monkeypatch, tmp_path)
    settings = _settings()
    runner = TerraformRunner(settings)

    login_route = respx.post(_LOGIN_URL).mock(return_value=_login_ok())
    revoke_route = respx.post(_REVOKE_URL).mock(return_value=httpx.Response(204))

    with (
        caplog.at_level(logging.DEBUG),
        patch.object(runner, "plan_platform_auth", AsyncMock(return_value=0)),
        patch.object(runner, "apply_platform_auth", AsyncMock()),
    ):
        action = await ensure_platform_auth(settings, runner)

    assert action == "noop_role_and_policy_current"
    assert login_route.call_count == 1
    assert revoke_route.call_count == 1
    assert revoke_route.calls.last.request.headers["X-Vault-Token"] == _K8S_TOKEN
    assert not any(_K8S_TOKEN in r.message for r in caplog.records)

    respx.reset()
    _fake_unseal_secret(monkeypatch)
    login_route = respx.post(_LOGIN_URL).mock(return_value=_login_ok())
    respx.post(_ATTEMPT_URL).mock(
        return_value=httpx.Response(200, json={"nonce": "n-1", "otp": "otp-value"})
    )
    respx.post(_UPDATE_URL).mock(
        return_value=httpx.Response(200, json={"complete": True, "encoded_token": "encoded-abc"})
    )
    respx.post(_DECODE_URL).mock(
        return_value=httpx.Response(200, json={"data": {"token": "s.root-token-xyz"}})
    )
    revoke_route = respx.post(_REVOKE_URL).mock(return_value=httpx.Response(204))
    apply_result = TerraformResult(exit_code=0, stdout="applied", stderr="", outputs={})

    caplog.clear()
    with (
        caplog.at_level(logging.DEBUG),
        patch.object(runner, "plan_platform_auth", AsyncMock(side_effect=[2, 0])),
        patch.object(runner, "apply_platform_auth", AsyncMock(return_value=apply_result)),
    ):
        action = await ensure_platform_auth(settings, runner)

    assert action == "break_glass_applied"
    # One revoke for the k8s-auth token (drift plan), one for the root
    # token (apply), one for the k8s-auth token again (re-proof plan).
    assert revoke_route.call_count == 3
    revoked_tokens = {c.request.headers["X-Vault-Token"] for c in revoke_route.calls}
    assert revoked_tokens == {_K8S_TOKEN, "s.root-token-xyz"}
    assert not any("root-token-xyz" in r.message for r in caplog.records)
    assert not any(_K8S_TOKEN in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# A 200 login carrying no usable client_token is NOT success — unknown is
# never benign on this gate.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_login_200_without_client_token_breaks_glass(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    _fake_jwt(monkeypatch, tmp_path)
    _fake_unseal_secret(monkeypatch)
    settings = _settings()
    runner = TerraformRunner(settings)

    # First probe: 200 but no auth.client_token (unusable) — must NOT
    # no-op. Second (post-apply) probe: a real client_token.
    login_route = respx.post(_LOGIN_URL).mock(
        side_effect=[httpx.Response(200, json={"auth": {}}), _login_ok()]
    )
    respx.post(_ATTEMPT_URL).mock(
        return_value=httpx.Response(200, json={"nonce": "n-1", "otp": "otp-value"})
    )
    respx.post(_UPDATE_URL).mock(
        return_value=httpx.Response(200, json={"complete": True, "encoded_token": "encoded-abc"})
    )
    respx.post(_DECODE_URL).mock(
        return_value=httpx.Response(200, json={"data": {"token": "s.root-token-xyz"}})
    )
    respx.post(_REVOKE_URL).mock(return_value=httpx.Response(204))

    apply_result = TerraformResult(exit_code=0, stdout="applied", stderr="", outputs={})
    with (
        patch.object(runner, "plan_platform_auth", AsyncMock(return_value=0)) as plan_mock,
        patch.object(
            runner, "apply_platform_auth", AsyncMock(return_value=apply_result)
        ) as apply_mock,
    ):
        action = await ensure_platform_auth(settings, runner)

    assert action == "break_glass_applied"
    assert login_route.call_count == 2
    apply_mock.assert_awaited_once()
    # plan_platform_auth must never be called with the unusable first
    # login's (nonexistent) token — only the second, real one.
    plan_mock.assert_awaited_once_with(_K8S_TOKEN)


# ---------------------------------------------------------------------------
# The login probe must use the CONFIGURED auth mount, never a hardcoded
# "kubernetes" literal.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_login_probe_uses_configured_auth_mount(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    _fake_jwt(monkeypatch, tmp_path)
    settings = _settings(vault_k8s_auth_mount="k8s-alt")
    runner = TerraformRunner(settings)

    alt_login_url = f"{_VAULT_ADDR}/v1/auth/k8s-alt/login"
    alt_route = respx.post(alt_login_url).mock(return_value=_login_ok())
    respx.post(_REVOKE_URL).mock(return_value=httpx.Response(204))
    # No route registered for the default "/v1/auth/kubernetes/login" —
    # respx raises if the code still hits the hardcoded default.

    with (
        patch.object(runner, "plan_platform_auth", AsyncMock(return_value=0)),
        patch.object(runner, "apply_platform_auth", AsyncMock()),
    ):
        action = await ensure_platform_auth(settings, runner)

    assert action == "noop_role_and_policy_current"
    assert alt_route.call_count == 1


# ---------------------------------------------------------------------------
# Cold start — login fails outright (role missing), module applied,
# revoke-self always fires for both tokens minted along the way.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_ensure_platform_auth_breaks_glass_on_cold_start(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    _fake_jwt(monkeypatch, tmp_path)
    _fake_unseal_secret(monkeypatch)
    settings = _settings()
    runner = TerraformRunner(settings)

    # First login probe fails outright (role missing); second (post-apply)
    # succeeds with a usable client_token.
    login_route = respx.post(_LOGIN_URL).mock(
        side_effect=[httpx.Response(400, json={"errors": ["role not found"]}), _login_ok()]
    )
    respx.post(_ATTEMPT_URL).mock(
        return_value=httpx.Response(200, json={"nonce": "n-1", "otp": "otp-value"})
    )
    respx.post(_UPDATE_URL).mock(
        return_value=httpx.Response(
            200, json={"complete": True, "encoded_token": "encoded-abc"}
        )
    )
    respx.post(_DECODE_URL).mock(
        return_value=httpx.Response(200, json={"data": {"token": "s.root-token-xyz"}})
    )
    revoke_route = respx.post(_REVOKE_URL).mock(return_value=httpx.Response(204))

    apply_result = TerraformResult(exit_code=0, stdout="applied", stderr="", outputs={})
    with (
        patch.object(runner, "plan_platform_auth", AsyncMock(return_value=0)) as plan_mock,
        patch.object(
            runner, "apply_platform_auth", AsyncMock(return_value=apply_result)
        ) as apply_mock,
    ):
        action = await ensure_platform_auth(settings, runner)

    assert action == "break_glass_applied"
    assert login_route.call_count == 2
    apply_mock.assert_awaited_once_with("s.root-token-xyz")
    plan_mock.assert_awaited_once_with(_K8S_TOKEN)
    # One revoke for the root token (apply), one for the k8s-auth token
    # minted by the post-apply re-proof login.
    assert revoke_route.call_count == 2
    revoked_tokens = {c.request.headers["X-Vault-Token"] for c in revoke_route.calls}
    assert revoked_tokens == {"s.root-token-xyz", _K8S_TOKEN}


@pytest.mark.asyncio
@respx.mock
async def test_ensure_platform_auth_revokes_even_if_apply_raises(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    """The `finally` around apply_platform_auth must revoke the break-glass
    root token regardless of outcome — and the original exception must
    still propagate to the caller (main.py's lifespan refuses to start)."""
    _fake_jwt(monkeypatch, tmp_path)
    _fake_unseal_secret(monkeypatch)
    settings = _settings()
    runner = TerraformRunner(settings)

    respx.post(_LOGIN_URL).mock(return_value=httpx.Response(400, json={}))
    respx.post(_ATTEMPT_URL).mock(
        return_value=httpx.Response(200, json={"nonce": "n-1", "otp": "otp-value"})
    )
    respx.post(_UPDATE_URL).mock(
        return_value=httpx.Response(200, json={"complete": True, "encoded_token": "encoded-abc"})
    )
    respx.post(_DECODE_URL).mock(
        return_value=httpx.Response(200, json={"data": {"token": "s.root-token-xyz"}})
    )
    revoke_route = respx.post(_REVOKE_URL).mock(return_value=httpx.Response(204))

    apply_err = TerraformError(
        "apply", TerraformResult(exit_code=1, stdout="", stderr="boom", outputs={})
    )
    with patch.object(runner, "apply_platform_auth", AsyncMock(side_effect=apply_err)):
        with pytest.raises(TerraformError, match="boom"):
            await ensure_platform_auth(settings, runner)

    assert revoke_route.call_count == 1, "revoke-self must fire even when apply raises"


# ---------------------------------------------------------------------------
# Post-apply re-verification failure — must raise, never silently return.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_ensure_platform_auth_raises_when_post_apply_verification_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    _fake_jwt(monkeypatch, tmp_path)
    _fake_unseal_secret(monkeypatch)
    settings = _settings()
    runner = TerraformRunner(settings)

    # Every login attempt fails outright — even after "applying" the module.
    respx.post(_LOGIN_URL).mock(return_value=httpx.Response(400, json={}))
    respx.post(_ATTEMPT_URL).mock(
        return_value=httpx.Response(200, json={"nonce": "n-1", "otp": "otp-value"})
    )
    respx.post(_UPDATE_URL).mock(
        return_value=httpx.Response(200, json={"complete": True, "encoded_token": "encoded-abc"})
    )
    respx.post(_DECODE_URL).mock(
        return_value=httpx.Response(200, json={"data": {"token": "s.root-token-xyz"}})
    )
    respx.post(_REVOKE_URL).mock(return_value=httpx.Response(204))

    apply_result = TerraformResult(exit_code=0, stdout="applied", stderr="", outputs={})
    with patch.object(runner, "apply_platform_auth", AsyncMock(return_value=apply_result)):
        with pytest.raises(PlatformAuthBootstrapError, match="still fails login"):
            await ensure_platform_auth(settings, runner)


@pytest.mark.asyncio
@respx.mock
async def test_generate_root_raises_when_shares_never_complete(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    _fake_jwt(monkeypatch, tmp_path)
    _fake_unseal_secret(monkeypatch)
    settings = _settings()
    runner = TerraformRunner(settings)

    respx.post(_LOGIN_URL).mock(return_value=httpx.Response(400, json={}))
    respx.post(_ATTEMPT_URL).mock(
        return_value=httpx.Response(200, json={"nonce": "n-1", "otp": "otp-value"})
    )
    respx.post(_UPDATE_URL).mock(
        return_value=httpx.Response(200, json={"complete": False, "progress": 1})
    )

    with pytest.raises(PlatformAuthBootstrapError, match="did not complete"):
        await ensure_platform_auth(settings, runner)


# ---------------------------------------------------------------------------
# Missing RBAC on the unseal Secret read — specific, actionable error.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_read_unseal_shares_forbidden_raises_specific_rbac_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    _fake_jwt(monkeypatch, tmp_path)
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    ca_path = tmp_path / "ca.crt"
    ca_path.write_text(_DUMMY_CA_PEM)
    monkeypatch.setattr(k8s_api, "SA_CA_CERT_PATH", ca_path)
    settings = _settings()
    runner = TerraformRunner(settings)

    respx.post(_LOGIN_URL).mock(return_value=httpx.Response(400, json={}))
    respx.get(
        f"https://10.0.0.1:443/api/v1/namespaces/{settings.openbao_namespace}/secrets/"
        f"{settings.openbao_bootstrap_secret_name}"
    ).mock(return_value=httpx.Response(403, json={"message": "Forbidden"}))

    with pytest.raises(k8s_api.K8sRbacError) as exc_info:
        await ensure_platform_auth(settings, runner)

    msg = str(exc_info.value)
    assert settings.openbao_bootstrap_secret_name in msg
    assert settings.openbao_namespace in msg
    assert "RoleBinding" in msg


@pytest.mark.asyncio
async def test_read_namespaced_secret_decodes_base64_values(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    _fake_jwt(monkeypatch, tmp_path)
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    ca_path = tmp_path / "ca.crt"
    ca_path.write_text(_DUMMY_CA_PEM)
    monkeypatch.setattr(k8s_api, "SA_CA_CERT_PATH", ca_path)

    encoded = base64.b64encode(b"unseal-share-1").decode()
    with respx.mock:
        respx.get("https://10.0.0.1:443/api/v1/namespaces/openbao/secrets/openbao-bootstrap").mock(
            return_value=httpx.Response(200, json={"data": {"UNSEAL_KEY_1": encoded}})
        )
        result = await k8s_api.read_namespaced_secret("openbao", "openbao-bootstrap")

    assert result == {"UNSEAL_KEY_1": "unseal-share-1"}


# ---------------------------------------------------------------------------
# Login-probe transport failure — must fall through to break-glass, never
# raise (a network hiccup on the probe is not proof OpenBao is down; the
# break-glass path's own calls will surface a clear error if it genuinely
# is).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_ensure_platform_auth_breaks_glass_when_login_probe_unreachable(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    _fake_jwt(monkeypatch, tmp_path)
    _fake_unseal_secret(monkeypatch)
    settings = _settings()
    runner = TerraformRunner(settings)

    # First probe raises a transport error (not an HTTP error response) —
    # _k8s_login must swallow it and report None; second (post-apply)
    # probe succeeds with a usable client_token.
    login_route = respx.post(_LOGIN_URL).mock(
        side_effect=[httpx.ConnectError("connection refused"), _login_ok()]
    )
    respx.post(_ATTEMPT_URL).mock(
        return_value=httpx.Response(200, json={"nonce": "n-1", "otp": "otp-value"})
    )
    respx.post(_UPDATE_URL).mock(
        return_value=httpx.Response(200, json={"complete": True, "encoded_token": "encoded-abc"})
    )
    respx.post(_DECODE_URL).mock(
        return_value=httpx.Response(200, json={"data": {"token": "s.root-token-xyz"}})
    )
    respx.post(_REVOKE_URL).mock(return_value=httpx.Response(204))

    apply_result = TerraformResult(exit_code=0, stdout="applied", stderr="", outputs={})
    with (
        patch.object(runner, "plan_platform_auth", AsyncMock(return_value=0)),
        patch.object(runner, "apply_platform_auth", AsyncMock(return_value=apply_result)),
    ):
        action = await ensure_platform_auth(settings, runner)

    assert action == "break_glass_applied"
    assert login_route.call_count == 2


# ---------------------------------------------------------------------------
# Unseal-share Secret missing a configured key — specific, actionable error.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_read_unseal_shares_raises_when_a_share_is_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    """settings.openbao_unseal_key_count defaults to 3, but the Secret only
    carries 2 UNSEAL_KEY_N entries — _read_unseal_shares must raise, naming
    the specific missing key, rather than submit an incomplete share set to
    generate-root/update."""
    _fake_jwt(monkeypatch, tmp_path)
    _fake_unseal_secret(monkeypatch, count=2)
    settings = _settings()
    runner = TerraformRunner(settings)

    respx.post(_LOGIN_URL).mock(return_value=httpx.Response(400, json={}))

    with pytest.raises(PlatformAuthBootstrapError, match="UNSEAL_KEY_3"):
        await ensure_platform_auth(settings, runner)


# ---------------------------------------------------------------------------
# generate-root HTTP-flow edge cases that must refuse to proceed rather
# than mint/return a broken or empty root token.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_generate_root_raises_when_complete_but_no_encoded_token(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    _fake_jwt(monkeypatch, tmp_path)
    _fake_unseal_secret(monkeypatch)
    settings = _settings()
    runner = TerraformRunner(settings)

    respx.post(_LOGIN_URL).mock(return_value=httpx.Response(400, json={}))
    respx.post(_ATTEMPT_URL).mock(
        return_value=httpx.Response(200, json={"nonce": "n-1", "otp": "otp-value"})
    )
    # complete=True but no encoded_token — malformed/unexpected OpenBao
    # response shape.
    respx.post(_UPDATE_URL).mock(return_value=httpx.Response(200, json={"complete": True}))

    with pytest.raises(PlatformAuthBootstrapError, match="no encoded_token"):
        await ensure_platform_auth(settings, runner)


@pytest.mark.asyncio
@respx.mock
async def test_generate_root_raises_when_decoded_token_is_empty(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    _fake_jwt(monkeypatch, tmp_path)
    _fake_unseal_secret(monkeypatch)
    settings = _settings()
    runner = TerraformRunner(settings)

    respx.post(_LOGIN_URL).mock(return_value=httpx.Response(400, json={}))
    respx.post(_ATTEMPT_URL).mock(
        return_value=httpx.Response(200, json={"nonce": "n-1", "otp": "otp-value"})
    )
    respx.post(_UPDATE_URL).mock(
        return_value=httpx.Response(200, json={"complete": True, "encoded_token": "encoded-abc"})
    )
    # decode-token responds 200 but hands back an empty token string.
    respx.post(_DECODE_URL).mock(
        return_value=httpx.Response(200, json={"data": {"token": ""}})
    )

    with pytest.raises(PlatformAuthBootstrapError, match="empty root token"):
        await ensure_platform_auth(settings, runner)


# ---------------------------------------------------------------------------
# Revoke-self failures — always logged, NEVER allowed to raise or mask the
# apply outcome (see _revoke_token's docstring: a live-until-TTL token plus
# a loud log line beats losing the real apply result).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_revoke_transport_failure_is_logged_not_raised(
    monkeypatch: pytest.MonkeyPatch, tmp_path, caplog: pytest.LogCaptureFixture,
) -> None:
    _fake_jwt(monkeypatch, tmp_path)
    _fake_unseal_secret(monkeypatch)
    settings = _settings()
    runner = TerraformRunner(settings)

    respx.post(_LOGIN_URL).mock(
        side_effect=[httpx.Response(400, json={}), _login_ok()]
    )
    respx.post(_ATTEMPT_URL).mock(
        return_value=httpx.Response(200, json={"nonce": "n-1", "otp": "otp-value"})
    )
    respx.post(_UPDATE_URL).mock(
        return_value=httpx.Response(200, json={"complete": True, "encoded_token": "encoded-abc"})
    )
    respx.post(_DECODE_URL).mock(
        return_value=httpx.Response(200, json={"data": {"token": "s.root-token-xyz"}})
    )
    revoke_route = respx.post(_REVOKE_URL).mock(side_effect=httpx.ConnectError("network unreachable"))

    apply_result = TerraformResult(exit_code=0, stdout="applied", stderr="", outputs={})
    with (
        patch.object(runner, "plan_platform_auth", AsyncMock(return_value=0)),
        patch.object(runner, "apply_platform_auth", AsyncMock(return_value=apply_result)),
        caplog.at_level(logging.ERROR, logger="terraformer.openbao_bootstrap"),
    ):
        action = await ensure_platform_auth(settings, runner)

    # The transport failure must not propagate — apply succeeded and the
    # role re-verifies (plan clean), so the overall converge still reports
    # success. Two revoke attempts now: the root token (apply) and the
    # k8s-auth token (post-apply plan) — both fail the same way.
    assert action == "break_glass_applied"
    assert revoke_route.call_count == 2
    assert any(
        "revoke-self" in r.message and "failed" in r.message for r in caplog.records
    ), "expected a loud log line noting the revoke failure"
    # And no token value itself must ever leak into that log line.
    assert not any("root-token-xyz" in r.message for r in caplog.records)
    assert not any(_K8S_TOKEN in r.message for r in caplog.records)


@pytest.mark.asyncio
@respx.mock
async def test_revoke_non_success_status_is_logged_not_raised(
    monkeypatch: pytest.MonkeyPatch, tmp_path, caplog: pytest.LogCaptureFixture,
) -> None:
    _fake_jwt(monkeypatch, tmp_path)
    _fake_unseal_secret(monkeypatch)
    settings = _settings()
    runner = TerraformRunner(settings)

    respx.post(_LOGIN_URL).mock(
        side_effect=[httpx.Response(400, json={}), _login_ok()]
    )
    respx.post(_ATTEMPT_URL).mock(
        return_value=httpx.Response(200, json={"nonce": "n-1", "otp": "otp-value"})
    )
    respx.post(_UPDATE_URL).mock(
        return_value=httpx.Response(200, json={"complete": True, "encoded_token": "encoded-abc"})
    )
    respx.post(_DECODE_URL).mock(
        return_value=httpx.Response(200, json={"data": {"token": "s.root-token-xyz"}})
    )
    revoke_route = respx.post(_REVOKE_URL).mock(return_value=httpx.Response(500, text="oops"))

    apply_result = TerraformResult(exit_code=0, stdout="applied", stderr="", outputs={})
    with (
        patch.object(runner, "plan_platform_auth", AsyncMock(return_value=0)),
        patch.object(runner, "apply_platform_auth", AsyncMock(return_value=apply_result)),
        caplog.at_level(logging.ERROR, logger="terraformer.openbao_bootstrap"),
    ):
        action = await ensure_platform_auth(settings, runner)

    assert action == "break_glass_applied"
    assert revoke_route.call_count == 2
    assert any(
        "revoke-self" in r.message and "500" in r.message for r in caplog.records
    )
