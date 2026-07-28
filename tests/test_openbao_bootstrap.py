"""Tests for openbao_bootstrap.ensure_platform_auth — the converge flow
that replaces the expired static OPENBAO_ADMIN_TOKEN (see the module
docstring for the full incident + flow). All OpenBao/Kubernetes HTTP calls
are mocked via respx — never live.
"""

from __future__ import annotations

import base64
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

_VAULT_ADDR = "http://openbao.openbao.test:8200"
_LOGIN_URL = f"{_VAULT_ADDR}/v1/auth/kubernetes/login"
_ATTEMPT_URL = f"{_VAULT_ADDR}/v1/sys/generate-root/attempt"
_UPDATE_URL = f"{_VAULT_ADDR}/v1/sys/generate-root/update"
_DECODE_URL = f"{_VAULT_ADDR}/v1/sys/decode-token"
_REVOKE_URL = f"{_VAULT_ADDR}/v1/auth/token/revoke-self"

# A real (self-signed, throwaway) PEM — httpx's `verify=<path>` eagerly
# parses the CA file at AsyncClient construction time even under respx
# mocking (no real TLS handshake ever happens, but the SSLContext is still
# built), so a placeholder string like "fake-ca" raises ssl.SSLError
# before respx gets a chance to intercept anything. Any structurally valid
# certificate works here — content is never actually validated against a
# live connection in these tests.
_DUMMY_CA_PEM = """-----BEGIN CERTIFICATE-----
MIIDBTCCAe2gAwIBAgIUZ6WeT6cBvb9ObIpek1g8rurn2OAwDQYJKoZIhvcNAQEL
BQAwEjEQMA4GA1UEAwwHdGVzdC1jYTAeFw0yNjA3MjgyMDE2MzhaFw0zNjA3MjUy
MDE2MzhaMBIxEDAOBgNVBAMMB3Rlc3QtY2EwggEiMA0GCSqGSIb3DQEBAQUAA4IB
DwAwggEKAoIBAQDDNiKmiSJC4/uh4FQSA3AMqDCUbglaTWNd8kTi1kpTgPHMNnMm
nV3PVaTKoHG41ieNf2yM4TYly5h3LMTR5BEak1ZsCRMsvqEJYHgdHe98ZPjZ6gCW
+ruAd7WUytype5hZe0+oZUwJ2pBbDYr/7eNdmQaFoenyp5FnHh5zwTtTyCMPT4x4
vuRfi8rbWfLzZAB2BFvS5Sj79YRHgE7jbFxt39vMpiemRcu5WTZjN/2rVdNDz17T
AoSInRcTJ4io7IqJ7SzjlQVG0RrbCg3COsOyHKonbZwWeIMViS1Ka7RiFmLfFyr/
OluYfCmsLgZRASDognsjElYzvMZDc9E6ZP9fAgMBAAGjUzBRMB0GA1UdDgQWBBRr
wU0ORE8DO9ZTDg1zDYS6qItczDAfBgNVHSMEGDAWgBRrwU0ORE8DO9ZTDg1zDYS6
qItczDAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQBT+3FE53ED
ObHiyqlOdQ5xmM9wtofVNBhZYohIwfRXxA8o5hRiIynVrXomUw5rTxcogDm2bSfz
y8yoRpBcWu6E88H+Www+4my6jMqI20wQNSDOuCrfLC3idHd+xX4N1OdP43VILMqp
5Rat3pI7x9h6Yc6qpJ7QJKN+PyubBtWlNgs17ppZ/HWzqMTnRvaht6mjCZUZQTnS
gQ6K/wyHP+8nl2IzVFfEInYk6UTp7DObscEc2ZKL4nfANYkrhGCEO6Wi0M7jZ2rs
qkWObyszYQdfHMJEKMxBo1PR/iSiuX3FVr1gwD4sVmVHo+wVBRK7c7fbUOuukGQD
JnaJVLtL5gHm
-----END CERTIFICATE-----
"""


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
