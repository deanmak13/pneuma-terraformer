"""Settings should hard-fail when enabled-surface secrets are missing.

Inactive provider credentials stay optional so TST can run on its current
k3s/Contabo estate without unrelated Hetzner credentials.
"""

from __future__ import annotations

import pytest


def test_settings_construct_from_env() -> None:
    from services.terraformer.src.settings import Settings

    s = Settings()
    assert s.env == "tst"
    assert s.tenant_infra_provider == "in_cluster"
    assert len(s.admin_api_key) >= 16
    assert s.terraform_modules_root.name == "modules"
    assert s.apply_timeout_seconds == 600


def test_missing_inactive_provider_secret_is_allowed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("HETZNER_API_TOKEN", raising=False)

    from services.terraformer.src.settings import Settings

    s = Settings()
    assert s.tenant_infra_provider == "in_cluster"
    assert s.hetzner_api_token is None


def test_hetzner_provider_requires_provider_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TENANT_INFRA_PROVIDER", "hetzner")
    monkeypatch.delenv("HETZNER_API_TOKEN", raising=False)
    from pydantic import ValidationError

    from services.terraformer.src.settings import Settings

    with pytest.raises(ValidationError) as exc_info:
        Settings()
    assert "tenant_infra_provider=hetzner requires seeded hetzner_api_token" in (
        str(exc_info.value).lower()
    )


def test_short_admin_key_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ADMIN_API_KEY", "short")
    from pydantic import ValidationError

    from services.terraformer.src.settings import Settings

    with pytest.raises(ValidationError):
        Settings()
