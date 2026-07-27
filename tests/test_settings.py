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


# ---------------------------------------------------------------------------
# Concurrency-knob lower bounds (review finding B2) — a values-file typo of
# "0" for MAX_CONCURRENT_TERRAFORM_RUNS makes asyncio.Semaphore(0) block
# every acquire forever, wedging the service with no error and green
# /health+/ready checks (they never touch the runner). Reject at construction
# instead of at first apply.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad_value", ["0", "-1"])
def test_max_concurrent_terraform_runs_rejects_non_positive(
    monkeypatch: pytest.MonkeyPatch, bad_value: str,
) -> None:
    from pydantic import ValidationError

    from services.terraformer.src.settings import Settings

    monkeypatch.setenv("MAX_CONCURRENT_TERRAFORM_RUNS", bad_value)
    with pytest.raises(ValidationError):
        Settings()


def test_max_concurrent_terraform_runs_accepts_positive_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services.terraformer.src.settings import Settings

    monkeypatch.setenv("MAX_CONCURRENT_TERRAFORM_RUNS", "5")
    s = Settings()
    assert s.max_concurrent_terraform_runs == 5


@pytest.mark.parametrize("bad_value", ["0", "-0.5", "1.5"])
def test_spawn_queue_timeout_fraction_rejects_out_of_range(
    monkeypatch: pytest.MonkeyPatch, bad_value: str,
) -> None:
    """gt=0/le=1 — a value of 0 (or negative) would zero out the queue
    wait entirely (a caller could never wait for a slot at all), and a
    value above 1 would let queueing alone exceed the operation's own
    timeout. Both must be rejected at startup, not discovered at first
    dispatch."""
    from pydantic import ValidationError

    from services.terraformer.src.settings import Settings

    monkeypatch.setenv("SPAWN_QUEUE_TIMEOUT_FRACTION", bad_value)
    with pytest.raises(ValidationError):
        Settings()


def test_spawn_queue_timeout_fraction_default() -> None:
    from services.terraformer.src.settings import Settings

    assert Settings().spawn_queue_timeout_fraction == 0.5


def test_spawn_queue_timeout_fraction_accepts_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services.terraformer.src.settings import Settings

    monkeypatch.setenv("SPAWN_QUEUE_TIMEOUT_FRACTION", "0.25")
    assert Settings().spawn_queue_timeout_fraction == 0.25
