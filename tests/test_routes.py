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


def _payload() -> dict[str, str]:
    return {
        "tenant_id": "t-001",
        "tenant_slug": "acme",
        "env": "tst",
        "compliance_profile": "standard",
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
