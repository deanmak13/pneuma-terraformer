"""Shared fixtures for terraformer tests.

These fixtures populate Settings via env so the module-level settings/runner
singletons can be constructed without crashing on missing required vars.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterator

import pytest

# A real (self-signed, throwaway) PEM — httpx's `verify=<path>` eagerly
# parses the CA file at AsyncClient construction time even under respx
# mocking (no real TLS handshake ever happens, but the SSLContext is still
# built), so a placeholder string like "fake-ca" raises ssl.SSLError before
# respx gets a chance to intercept anything. Any structurally valid
# certificate works here — content is never actually validated against a
# live connection in these tests. Shared across test_k8s_api.py and
# test_openbao_bootstrap.py — both need a SA_CA_CERT_PATH to construct a
# k8s_api SSL context.
DUMMY_CA_PEM = """-----BEGIN CERTIFICATE-----
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

_REQUIRED_ENV = {
    "ENV": "tst",
    "ADMIN_API_KEY": "test-admin-key-1234567890",
    "SUPABASE_URL": "http://supabase-rest.platform-tst.svc.cluster.local:3000",
    "SUPABASE_SERVICE_KEY": "test-service-role-jwt-1234567890",
    "TF_STATE_BACKEND_ENDPOINT": "http://minio.test:9000",
    "TF_STATE_BACKEND_ACCESS_KEY": "terraformer-test",
    "TF_STATE_BACKEND_SECRET_KEY": "terraformer-test-secret",
    "HETZNER_API_TOKEN": "x" * 40,
    "CLOUDFLARE_API_TOKEN": "x" * 40,
    "POSTGRES_SUPERUSER_PASSWORD": "pg-pass-1234",
    "RABBITMQ_ADMIN_PASSWORD": "rmq-pass-1234",
    "MINIO_ADMIN_PASSWORD": "minio-pass-1234",
    "VAULT_ADDR": "http://openbao.openbao.svc.cluster.local:8200",
}


@pytest.fixture(autouse=True)
def _populate_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    for k, v in _REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("TERRAFORM_MODULES_ROOT", str(tmp_path / "modules"))
    monkeypatch.setenv("TERRAFORM_WORKDIR_ROOT", str(tmp_path / "workspaces"))
    monkeypatch.setenv("TERRAFORM_BINARY", "/usr/bin/true")
    from services.terraformer.src import settings as settings_mod
    settings_mod._settings = None
    from services.terraformer.src import terraform_runner as runner_mod
    runner_mod._runner = None
    yield
    settings_mod._settings = None
    runner_mod._runner = None
