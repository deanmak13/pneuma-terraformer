"""Unit tests for reconcile_cli — the one-shot Job/CronJob entrypoint.

Only new coverage this unit adds: the fail-closed exit contract when
`reconcile_platform_resources` raises `TerraformError`. No production
code change was needed in reconcile_cli.py — it already fails loud
(`_reconcile_platform_resources` catches `TerraformError`, logs, and
raises `SystemExit(1)`; `main` maps that to a non-zero return code).
This test pins that existing path now that the platform-resources
import fix (services/terraformer/src/terraform_runner.py) can newly
raise `TerraformError` from a real defect instead of always swallowing
it, so a Job failure here is now reachable and must actually exit 1.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from services.terraformer.src import reconcile_cli
from services.terraformer.src.terraform_runner import (
    PlatformResourcesInputs,
    TerraformError,
    TerraformResult,
)


class _FakeLease:
    async def __aenter__(self) -> "_FakeLease":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False


class _RaisingRunner:
    async def reconcile_platform_resources(self, inputs: PlatformResourcesInputs):  # noqa: ANN201
        raise TerraformError(
            "import",
            TerraformResult(exit_code=1, stdout="", stderr="UNCLASSIFIED boom", outputs={}),
        )


def test_platform_resources_import_failure_exits_1() -> None:
    """A TerraformError raised out of reconcile_platform_resources (the
    fail-closed path an UNCLASSIFIED import failure now takes) must
    surface as CLI exit code 1 — never a silent 0."""
    with patch(
        "services.terraformer.src.openbao_bootstrap.ensure_platform_auth",
        AsyncMock(return_value="noop"),
    ), patch(
        "services.terraformer.src.kube_lease_mutex.KubeLeaseMutex",
        lambda *args, **kwargs: _FakeLease(),  # noqa: ARG005
    ), patch(
        "services.terraformer.src.terraform_runner.get_runner",
        lambda: _RaisingRunner(),
    ):
        assert reconcile_cli.main(["platform-resources", "--env=tst"]) == 1
