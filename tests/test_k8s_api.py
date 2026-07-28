"""Direct unit tests for services.terraformer.src.k8s_api — the minimal
in-cluster Kubernetes API client openbao_bootstrap's break-glass path uses
to read the Shamir unseal-share Secret. All HTTP is mocked via respx or
never reached at all; no real cluster involved.
"""

from __future__ import annotations

import httpx
import pytest
import respx

from services.terraformer.src import k8s_api
from tests.conftest import DUMMY_CA_PEM

_SECRET_URL = "https://10.0.0.1:443/api/v1/namespaces/openbao/secrets/openbao-bootstrap"


def _fake_sa_identity(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    token_path = tmp_path / "sa-token"
    token_path.write_text("fake-sa-jwt")
    monkeypatch.setattr(k8s_api, "SA_TOKEN_PATH", token_path)
    ca_path = tmp_path / "ca.crt"
    ca_path.write_text(DUMMY_CA_PEM)
    monkeypatch.setattr(k8s_api, "SA_CA_CERT_PATH", ca_path)
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")


@pytest.mark.asyncio
async def test_read_namespaced_secret_raises_when_not_in_cluster(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No KUBERNETES_SERVICE_HOST means this process isn't running
    in-cluster — the client must say exactly that rather than attempt a
    doomed connection to a made-up host."""
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)

    with pytest.raises(k8s_api.K8sApiError, match="not running in-cluster"):
        await k8s_api.read_namespaced_secret("openbao", "openbao-bootstrap")


@pytest.mark.asyncio
async def test_read_namespaced_secret_raises_k8s_api_error_on_transport_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    """A network-level failure reaching the API server (not an HTTP error
    response) must surface as K8sApiError naming the Secret/namespace —
    never a bare httpx exception leaking out of this module."""
    _fake_sa_identity(monkeypatch, tmp_path)

    with respx.mock:
        respx.get(_SECRET_URL).mock(side_effect=httpx.ConnectError("connection refused"))
        with pytest.raises(k8s_api.K8sApiError, match="unreachable"):
            await k8s_api.read_namespaced_secret("openbao", "openbao-bootstrap")


@pytest.mark.asyncio
async def test_read_namespaced_secret_raises_on_unexpected_status(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    """Any non-200, non-403 response (e.g. a 500 from an overloaded API
    server) must raise K8sApiError naming the actual status code — 403 is
    the only status with its own dedicated (RBAC) error type."""
    _fake_sa_identity(monkeypatch, tmp_path)

    with respx.mock:
        respx.get(_SECRET_URL).mock(return_value=httpx.Response(500, text="internal error"))
        with pytest.raises(k8s_api.K8sApiError, match="500") as exc_info:
            await k8s_api.read_namespaced_secret("openbao", "openbao-bootstrap")

    assert not isinstance(exc_info.value, k8s_api.K8sRbacError)
