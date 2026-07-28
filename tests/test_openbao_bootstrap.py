"""Tests for openbao_bootstrap.ensure_platform_auth — the converge flow
that replaces the expired static OPENBAO_ADMIN_TOKEN (see the module
docstring for the full incident + flow). All OpenBao/Kubernetes HTTP calls
are mocked via respx — never live.
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


def _settings(**overrides) -> Settings:
    return Settings(vault_addr=_VAULT_ADDR, **overrides)


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
# Steady state — login succeeds, no-op.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_ensure_platform_auth_noop_when_login_already_valid(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    _fake_jwt(monkeypatch, tmp_path)
    settings = _settings()
    runner = TerraformRunner(settings)

    login_route = respx.post(_LOGIN_URL).mock(return_value=httpx.Response(200, json={"auth": {}}))
    # No other route registered — respx raises if any is hit, proving no
    # generate-root/apply/revoke call is ever attempted on the happy path.

    with patch.object(runner, "apply_platform_auth", AsyncMock()) as apply_mock:
        action = await ensure_platform_auth(settings, runner)

    assert action == "noop_role_already_valid"
    assert login_route.call_count == 1
    apply_mock.assert_not_called()


# ---------------------------------------------------------------------------
# Cold start — break glass, module applied, revoke-self always fires.
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

    # First login probe fails (role missing); second (post-apply) succeeds.
    login_route = respx.post(_LOGIN_URL).mock(
        side_effect=[httpx.Response(400, json={"errors": ["role not found"]}), httpx.Response(200, json={})]
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
    with patch.object(
        runner, "apply_platform_auth", AsyncMock(return_value=apply_result)
    ) as apply_mock:
        action = await ensure_platform_auth(settings, runner)

    assert action == "break_glass_applied"
    assert login_route.call_count == 2
    apply_mock.assert_awaited_once_with("s.root-token-xyz")
    assert revoke_route.call_count == 1
    assert revoke_route.calls.last.request.headers["X-Vault-Token"] == "s.root-token-xyz"


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

    # Every login attempt fails — even after "applying" the module.
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
    # _k8s_login_ok must swallow it and report False; second (post-apply)
    # probe succeeds.
    login_route = respx.post(_LOGIN_URL).mock(
        side_effect=[httpx.ConnectError("connection refused"), httpx.Response(200, json={})]
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
    with patch.object(runner, "apply_platform_auth", AsyncMock(return_value=apply_result)):
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
        side_effect=[httpx.Response(400, json={}), httpx.Response(200, json={})]
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
    with patch.object(runner, "apply_platform_auth", AsyncMock(return_value=apply_result)):
        with caplog.at_level(logging.ERROR, logger="terraformer.openbao_bootstrap"):
            action = await ensure_platform_auth(settings, runner)

    # The transport failure must not propagate — apply succeeded and the
    # role re-verifies, so the overall converge still reports success.
    assert action == "break_glass_applied"
    assert revoke_route.call_count == 1
    assert any(
        "revoke-self" in r.message and "failed" in r.message for r in caplog.records
    ), "expected a loud log line noting the revoke failure"
    # And the token itself must never leak into that log line.
    assert not any("root-token-xyz" in r.message for r in caplog.records)


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
        side_effect=[httpx.Response(400, json={}), httpx.Response(200, json={})]
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
    with patch.object(runner, "apply_platform_auth", AsyncMock(return_value=apply_result)):
        with caplog.at_level(logging.ERROR, logger="terraformer.openbao_bootstrap"):
            action = await ensure_platform_auth(settings, runner)

    assert action == "break_glass_applied"
    assert revoke_route.call_count == 1
    assert any(
        "revoke-self" in r.message and "500" in r.message for r in caplog.records
    )
