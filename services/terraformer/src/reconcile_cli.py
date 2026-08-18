"""One-shot CLI entrypoint for the platform-tier reconcile-on-change
automation (ArgoCD PostSync hook Job / CronJob backstop — see
pneuma-deployments platform/base/platform-secrets-reconcile/ and
docs/standards/platform-secrets.md "Automated lifecycle").

Runs IN the terraformer image, as a Kubernetes Job that reuses the
terraformer ServiceAccount (so it inherits the exact same OpenBao
kubernetes-auth identity `_ensure_openbao_auth` already proves at pod
startup — see openbao_bootstrap.ensure_platform_auth — never a second,
job-scoped Vault role or a static token). This is a DIFFERENT invocation
path from the gRPC ApplyPlatformSecrets/ApplyPlatformBusTopology RPCs
(those are dispatched by the cycle-executor over the network for the
operator-gated draft->active flip); this CLI calls TerraformRunner
in-process for the periodic/on-merge reconcile, which needs no caller
identity beyond "runs as the terraformer ServiceAccount".

Usage:
    python -m services.terraformer.src.reconcile_cli platform-secrets --env=tst
    python -m services.terraformer.src.reconcile_cli platform-resources --env=tst
    python -m services.terraformer.src.reconcile_cli all --env=tst

Fail-loud by design: any TerraformError or unhandled exception exits
non-zero (Job goes to Error/BackoffLimitExceeded, alertable via standard
k8s Job-failure monitoring) — never a silent skip. Every reconciled
target's `newly_generated_keys` / `unsupported_generate_entries` outputs
(platform-secrets) are logged at INFO/WARNING so a fan-out change is
visible in the Job's logs without requiring a manual `terraform output`.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

_LOG = logging.getLogger("terraformer.reconcile_cli")

_TARGETS = ("platform-secrets", "platform-resources")


def _configure_logging() -> None:
    import os

    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


async def _reconcile_platform_secrets(env: str) -> None:
    from services.terraformer.src.terraform_runner import (
        PlatformSecretsInputs,
        TerraformError,
        get_runner,
    )

    runner = get_runner()
    try:
        result = await runner.reconcile_platform_secrets(PlatformSecretsInputs(env=env))
    except TerraformError as exc:
        _LOG.error("platform-secrets reconcile FAILED (env=%s): %s", env, exc)
        raise SystemExit(1) from exc

    newly_generated = result.outputs.get("newly_generated_keys")
    unsupported = result.outputs.get("unsupported_generate_entries")
    entry_count = result.outputs.get("entry_count")
    _LOG.info(
        "platform-secrets reconcile OK (env=%s): entry_count=%s newly_generated_keys=%s "
        "unsupported_generate_entries=%s",
        env,
        entry_count,
        newly_generated,
        unsupported,
    )
    if unsupported:
        _LOG.warning(
            "platform-secrets reconcile (env=%s) has unsupported_generate_entries — "
            "these generator kinds (fernet/static/neo4j-compound) are NOT applied by "
            "this automation and remain seed-vault.sh bootstrap-only. See "
            "docs/standards/platform-secrets.md 'Residual manual surface'.",
            env,
        )


async def _reconcile_platform_resources(env: str) -> None:
    from services.terraformer.src.terraform_runner import (
        PlatformResourcesInputs,
        TerraformError,
        get_runner,
    )

    runner = get_runner()
    try:
        result = await runner.reconcile_platform_resources(PlatformResourcesInputs(env=env))
    except TerraformError as exc:
        _LOG.error("platform-resources reconcile FAILED (env=%s): %s", env, exc)
        raise SystemExit(1) from exc

    _LOG.info(
        "platform-resources reconcile OK (env=%s): outputs=%s",
        env,
        result.outputs,
    )


async def _run(target: str, env: str) -> None:
    from services.terraformer.src.openbao_bootstrap import ensure_platform_auth
    from services.terraformer.src.settings import get_settings
    from services.terraformer.src.terraform_runner import get_runner

    settings = get_settings()
    # Same converge-or-fail-loud OpenBao identity check the FastAPI
    # lifespan runs at pod startup (main._ensure_openbao_auth) — this Job
    # is a separate process/container invocation of the same image and
    # gets no benefit from the long-running pod's prior bootstrap.
    action = await ensure_platform_auth(settings, get_runner())
    _LOG.info("reconcile_cli openbao auth bootstrap complete: action=%s", action)

    if target in ("platform-secrets", "all"):
        await _reconcile_platform_secrets(env)
    if target in ("platform-resources", "all"):
        await _reconcile_platform_resources(env)


def main(argv: list[str] | None = None) -> int:
    _configure_logging()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "target",
        choices=(*_TARGETS, "all"),
        help="Which standalone harness to reconcile.",
    )
    parser.add_argument(
        "--env",
        required=True,
        choices=("dev", "tst", "prod"),
        help="Cluster env — must match the overlay this Job is deployed into.",
    )
    args = parser.parse_args(argv)

    try:
        asyncio.run(_run(args.target, args.env))
    except SystemExit as exc:
        return int(exc.code or 1)
    except Exception:
        _LOG.exception("reconcile_cli FAILED (target=%s env=%s)", args.target, args.env)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
