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


# ---------------------------------------------------------------------------
# OpenBao kubernetes-auth role/mount (feat/openbao-k8s-auth) — replaces the
# static OPENBAO_ADMIN_TOKEN that expired and 403'd every tenant apply.
# Settings has no such field any more; role/mount must be present, non-empty,
# and never a hardcoded literal in the generated HCL (see terraform_runner
# tests for the rendered-HCL-changes-with-settings assertion).
# ---------------------------------------------------------------------------


def test_openbao_admin_token_field_removed() -> None:
    from services.terraformer.src.settings import Settings

    assert not hasattr(Settings(), "openbao_admin_token")


def test_vault_k8s_auth_role_and_mount_defaults() -> None:
    from services.terraformer.src.settings import Settings

    s = Settings()
    assert s.vault_k8s_auth_role == "terraformer"
    assert s.vault_k8s_auth_mount == "kubernetes"


def test_vault_k8s_auth_role_and_mount_accept_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services.terraformer.src.settings import Settings

    monkeypatch.setenv("VAULT_K8S_AUTH_ROLE", "terraformer-prod")
    monkeypatch.setenv("VAULT_K8S_AUTH_MOUNT", "kubernetes-prod")
    s = Settings()
    assert s.vault_k8s_auth_role == "terraformer-prod"
    assert s.vault_k8s_auth_mount == "kubernetes-prod"


@pytest.mark.parametrize("env_var", ["VAULT_K8S_AUTH_ROLE", "VAULT_K8S_AUTH_MOUNT"])
def test_vault_k8s_auth_role_and_mount_reject_empty(
    monkeypatch: pytest.MonkeyPatch, env_var: str,
) -> None:
    from pydantic import ValidationError

    from services.terraformer.src.settings import Settings

    monkeypatch.setenv(env_var, "")
    with pytest.raises(ValidationError):
        Settings()


# ---------------------------------------------------------------------------
# OpenBao self-bootstrap settings (openbao_bootstrap.ensure_platform_auth) —
# fixes the boot deadlock: OPENBAO_ADMIN_TOKEN required-but-unfulfillable.
# ---------------------------------------------------------------------------


def test_openbao_admin_token_never_required_to_boot() -> None:
    """The service must start with only VAULT_ADDR + its own SA token +
    k8s API access — no OPENBAO_ADMIN_TOKEN env var at all."""
    import os

    from services.terraformer.src.settings import Settings

    assert "OPENBAO_ADMIN_TOKEN" not in os.environ
    s = Settings()
    assert not hasattr(s, "openbao_admin_token")


def test_vault_addr_is_required(monkeypatch: pytest.MonkeyPatch) -> None:
    from pydantic import ValidationError

    from services.terraformer.src.settings import Settings

    monkeypatch.delenv("VAULT_ADDR", raising=False)
    with pytest.raises(ValidationError):
        Settings()


def test_vault_addr_accepts_override(monkeypatch: pytest.MonkeyPatch) -> None:
    from services.terraformer.src.settings import Settings

    monkeypatch.setenv("VAULT_ADDR", "http://openbao.openbao.svc.cluster.local:8200")
    s = Settings()
    assert s.vault_addr == "http://openbao.openbao.svc.cluster.local:8200"


def test_openbao_bootstrap_defaults() -> None:
    from services.terraformer.src.settings import Settings

    s = Settings()
    assert s.openbao_namespace == "openbao"
    assert s.openbao_bootstrap_secret_name == "openbao-bootstrap"
    assert s.openbao_unseal_key_count == 3
    assert s.terraformer_service_account_name == "terraformer"


def test_openbao_bootstrap_settings_accept_override(monkeypatch: pytest.MonkeyPatch) -> None:
    from services.terraformer.src.settings import Settings

    monkeypatch.setenv("OPENBAO_NAMESPACE", "openbao-secondary")
    monkeypatch.setenv("OPENBAO_BOOTSTRAP_SECRET_NAME", "bao-unseal")
    monkeypatch.setenv("OPENBAO_UNSEAL_KEY_COUNT", "5")
    monkeypatch.setenv("TERRAFORMER_SERVICE_ACCOUNT_NAME", "terraformer-prod")
    s = Settings()
    assert s.openbao_namespace == "openbao-secondary"
    assert s.openbao_bootstrap_secret_name == "bao-unseal"
    assert s.openbao_unseal_key_count == 5
    assert s.terraformer_service_account_name == "terraformer-prod"


def test_openbao_unseal_key_count_rejects_non_positive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantic import ValidationError

    from services.terraformer.src.settings import Settings

    monkeypatch.setenv("OPENBAO_UNSEAL_KEY_COUNT", "0")
    with pytest.raises(ValidationError):
        Settings()
