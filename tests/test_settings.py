"""Settings should hard-fail when any required secret is missing.

This is the no-optional-secrets LAW expressed as a unit test.
"""

from __future__ import annotations

import pytest


def test_settings_construct_from_env() -> None:
    from services.terraformer.src.settings import Settings

    s = Settings()
    assert s.env == "tst"
    assert len(s.admin_api_key) >= 16
    assert s.terraform_modules_root.name == "modules"
    assert s.apply_timeout_seconds == 600


def test_missing_required_secret_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("HETZNER_API_TOKEN", raising=False)
    from services.terraformer.src.settings import Settings
    from pydantic import ValidationError

    with pytest.raises(ValidationError) as exc_info:
        Settings()
    assert "hetzner_api_token" in str(exc_info.value).lower()


def test_short_admin_key_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ADMIN_API_KEY", "short")
    from services.terraformer.src.settings import Settings
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        Settings()
