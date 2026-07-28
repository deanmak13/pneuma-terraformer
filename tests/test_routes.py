"""HTTP route tests — exercise FastAPI auth + reconcile/destroy/state paths
with a stubbed TerraformRunner.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from services.terraformer.src.terraform_runner import TerraformResult


@pytest.fixture
def client() -> TestClient:
    from services.terraformer.src.main import app
    return TestClient(app)


def _payload() -> dict[str, str | None]:
    return {
        "tenant_id": "t-001",
        "tenant_slug": "acme",
        "env": "tst",
        "compliance_profile": None,
        "pooled_namespace": "platform-tst",
    }


def test_health_unauthenticated(client: TestClient) -> None:
    assert client.get("/health").json() == {"status": "ok"}


def test_reconcile_requires_admin_key(client: TestClient) -> None:
    resp = client.post("/provisioning/reconcile", json=_payload())
    assert resp.status_code == 401


def test_reconcile_rejects_wrong_admin_key(client: TestClient) -> None:
    resp = client.post(
        "/provisioning/reconcile",
        json=_payload(),
        headers={"X-Admin-Key": "nope"},
    )
    assert resp.status_code == 401


def test_reconcile_returns_outputs(client: TestClient) -> None:
    fake = TerraformResult(
        exit_code=0,
        stdout="apply complete",
        stderr="",
        outputs={"vhost": "/acme-tst"},
    )
    with patch(
        "services.terraformer.src.routes.provisioning.get_runner"
    ) as mock_get:
        mock_get.return_value.reconcile = AsyncMock(return_value=fake)
        resp = client.post(
            "/provisioning/reconcile",
            json=_payload(),
            headers={"X-Admin-Key": "test-admin-key-1234567890"},
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["tenant_id"] == "t-001"
    assert body["outputs"] == {"vhost": "/acme-tst"}


def test_reconcile_surfaces_terraform_error(client: TestClient) -> None:
    from services.terraformer.src.terraform_runner import TerraformError

    err_result = TerraformResult(
        exit_code=1,
        stdout="",
        stderr="Hetzner rate-limited",
        outputs={},
    )
    with patch(
        "services.terraformer.src.routes.provisioning.get_runner"
    ) as mock_get:
        mock_get.return_value.reconcile = AsyncMock(
            side_effect=TerraformError("apply", err_result)
        )
        resp = client.post(
            "/provisioning/reconcile",
            json=_payload(),
            headers={"X-Admin-Key": "test-admin-key-1234567890"},
        )
    assert resp.status_code == 500
    detail = resp.json()["detail"]
    assert detail["phase"] == "apply"
    assert "rate-limited" in detail["stderr_tail"]


def test_destroy_returns_ok(client: TestClient) -> None:
    fake = TerraformResult(exit_code=0, stdout="destroyed", stderr="", outputs={})
    with patch(
        "services.terraformer.src.routes.provisioning.get_runner"
    ) as mock_get:
        mock_get.return_value.destroy = AsyncMock(return_value=fake)
        resp = client.post(
            "/provisioning/destroy",
            json=_payload(),
            headers={"X-Admin-Key": "test-admin-key-1234567890"},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["tenant_id"] == "t-001"


def test_state_absent(client: TestClient) -> None:
    with patch(
        "services.terraformer.src.routes.provisioning.get_runner"
    ) as mock_get:
        mock_get.return_value.state = AsyncMock(
            return_value={"exists": False, "outputs": {}}
        )
        resp = client.get(
            "/provisioning/state/t-999",
            headers={"X-Admin-Key": "test-admin-key-1234567890"},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body == {"tenant_id": "t-999", "exists": False, "outputs": {}}


def test_validation_rejects_empty_tenant_id(client: TestClient) -> None:
    bad = _payload() | {"tenant_id": ""}
    resp = client.post(
        "/provisioning/reconcile",
        json=bad,
        headers={"X-Admin-Key": "test-admin-key-1234567890"},
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# POST /provisioning/admin/bootstrap/openbao-auth — on-demand re-run of the
# same openbao_bootstrap.ensure_platform_auth convergence main.py runs at
# boot (see routes/provisioning.py's module-level comment on this route).
# ---------------------------------------------------------------------------


def test_bootstrap_openbao_auth_requires_admin_key(client: TestClient) -> None:
    resp = client.post("/provisioning/admin/bootstrap/openbao-auth")
    assert resp.status_code == 401


def test_bootstrap_openbao_auth_returns_action(client: TestClient) -> None:
    with patch(
        "services.terraformer.src.routes.provisioning.ensure_platform_auth",
        AsyncMock(return_value="noop_role_already_valid"),
    ):
        resp = client.post(
            "/provisioning/admin/bootstrap/openbao-auth",
            headers={"X-Admin-Key": "test-admin-key-1234567890"},
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["action"] == "noop_role_already_valid"
    assert body["role"] and body["mount"]


def test_bootstrap_openbao_auth_surfaces_terraform_error(client: TestClient) -> None:
    from services.terraformer.src.terraform_runner import TerraformError

    err_result = TerraformResult(
        exit_code=1, stdout="", stderr="vault_policy apply failed", outputs={},
    )
    with patch(
        "services.terraformer.src.routes.provisioning.ensure_platform_auth",
        AsyncMock(side_effect=TerraformError("apply", err_result)),
    ):
        resp = client.post(
            "/provisioning/admin/bootstrap/openbao-auth",
            headers={"X-Admin-Key": "test-admin-key-1234567890"},
        )
    assert resp.status_code == 500
    detail = resp.json()["detail"]
    assert detail["phase"] == "apply"
    assert "vault_policy apply failed" in detail["stderr_tail"]


def test_bootstrap_openbao_auth_surfaces_generic_failure(client: TestClient) -> None:
    """A non-TerraformError failure (e.g. PlatformAuthBootstrapError from
    the re-verify step, or a K8sRbacError reading the unseal Secret) must
    still surface as a 500 with the exception text — not an unhandled
    5xx with no detail."""
    from services.terraformer.src.openbao_bootstrap import PlatformAuthBootstrapError

    with patch(
        "services.terraformer.src.routes.provisioning.ensure_platform_auth",
        AsyncMock(
            side_effect=PlatformAuthBootstrapError(
                "role=terraformer still fails login after applying platform-auth-bootstrap"
            )
        ),
    ):
        resp = client.post(
            "/provisioning/admin/bootstrap/openbao-auth",
            headers={"X-Admin-Key": "test-admin-key-1234567890"},
        )
    assert resp.status_code == 500
    assert "still fails login" in resp.json()["detail"]
