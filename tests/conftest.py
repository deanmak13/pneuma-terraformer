"""Shared fixtures for terraformer tests.

These fixtures populate Settings via env so the module-level settings/runner
singletons can be constructed without crashing on missing required vars.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterator

import pytest

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
