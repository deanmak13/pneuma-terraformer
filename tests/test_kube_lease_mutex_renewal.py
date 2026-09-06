"""Lease renewal (opt-in `renew_interval_seconds`).

Regression for TST 2026-09-06 10:02Z: the per-tenant Lease was sized to
the whole apply budget (900s) with no renewal, so when kubelet evicted
the terraformer pod 14s into an apply the successor pod polled the dead
holder's lease for the remaining ~15 minutes while the owner sat on
/signup/created. With renewal the lease can be short: a live holder
keeps pushing `renewTime` forward, a dead one expires within one
duration.

Every test drives the REAL `KubeLeaseMutex` HTTP calls against an
in-memory fake `coordination.k8s.io/v1` Leases API (GET/POST/PUT on one
named Lease) — nothing in the mutex is mocked away.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

import httpx
import pytest

from services.terraformer.src import terraform_runner as runner_mod
from services.terraformer.src.kube_lease_mutex import (
    KubeLeaseMutex,
    _now_rfc3339,
    _parse_rfc3339,
)


class _FakeLeasesApi:
    """Just enough of the Leases API for acquire/renew/release, plus the
    hooks the tests use to inject faults and observe writes."""

    def __init__(self) -> None:
        self.leases: dict[str, dict] = {}
        self.put_count = 0
        self.fail_next_puts = 0

    def handler(self, request: httpx.Request) -> httpx.Response:
        name = request.url.path.rsplit("/", 1)[-1]
        if request.method == "GET":
            if name in self.leases:
                return httpx.Response(200, json=self.leases[name])
            return httpx.Response(404, json={})
        if request.method == "POST":
            body = json.loads(request.content)
            self.leases[body["metadata"]["name"]] = body
            return httpx.Response(201, json=body)
        if request.method == "PUT":
            self.put_count += 1
            if self.fail_next_puts > 0:
                self.fail_next_puts -= 1
                return httpx.Response(500, json={"message": "injected"})
            body = json.loads(request.content)
            self.leases[name] = body
            return httpx.Response(200, json=body)
        raise AssertionError(f"unexpected method {request.method}")  # pragma: no cover

    def spec(self, name: str) -> dict:
        return self.leases[name]["spec"]


@pytest.fixture
def fake_api(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> _FakeLeasesApi:
    token = tmp_path / "token"
    ca = tmp_path / "ca.crt"
    token.write_text("fake-token")
    ca.write_text("fake-ca")
    monkeypatch.setattr(runner_mod, "_KUBE_SA_TOKEN_PATH", token)
    monkeypatch.setattr(runner_mod, "_KUBE_SA_CA_CERT_PATH", ca)
    monkeypatch.setenv("PNEUMA_NAMESPACE", "platform-tst")
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.43.0.1")

    api = _FakeLeasesApi()

    def _fake_client(self: KubeLeaseMutex) -> httpx.AsyncClient:  # noqa: ARG001
        return httpx.AsyncClient(transport=httpx.MockTransport(api.handler))

    monkeypatch.setattr(KubeLeaseMutex, "_client", _fake_client)
    return api


async def _wait_until(predicate, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_holder_renews_renew_time_while_held(fake_api: _FakeLeasesApi) -> None:
    mutex = KubeLeaseMutex(
        "tf-tenant-t-001",
        lease_duration_seconds=1,
        renew_interval_seconds=0.05,
        acquire_timeout_seconds=1,
    )
    async with mutex:
        initial = _parse_rfc3339(fake_api.spec("tf-tenant-t-001")["renewTime"])
        assert initial is not None
        await _wait_until(lambda: fake_api.put_count >= 2)
        renewed = _parse_rfc3339(fake_api.spec("tf-tenant-t-001")["renewTime"])
        assert renewed is not None
        assert renewed > initial, "renewal must push renewTime forward"
        assert fake_api.spec("tf-tenant-t-001")["holderIdentity"] == mutex.holder
        renew_task = mutex._renew_task
        assert renew_task is not None and not renew_task.done()

    # Exiting the block cancels the renewal task (not merely lets it run
    # on until it notices the release — a failed best-effort release must
    # not leave a zombie renewer holding the tenant) and releases the lease.
    assert mutex._renew_task is None
    assert renew_task.cancelled()
    assert fake_api.spec("tf-tenant-t-001")["holderIdentity"] is None
    puts_at_exit = fake_api.put_count
    await asyncio.sleep(0.15)
    assert fake_api.put_count == puts_at_exit, "no renewal may run after release"


@pytest.mark.asyncio
async def test_renewal_stops_and_never_clobbers_a_new_holder(
    fake_api: _FakeLeasesApi,
) -> None:
    mutex = KubeLeaseMutex(
        "tf-tenant-t-002",
        lease_duration_seconds=1,
        renew_interval_seconds=0.05,
        acquire_timeout_seconds=1,
    )
    async with mutex:
        # Another pod legitimately stole the lease (we overran it).
        stolen_at = _now_rfc3339()
        fake_api.spec("tf-tenant-t-002")["holderIdentity"] = "other-pod-deadbeef"
        fake_api.spec("tf-tenant-t-002")["renewTime"] = stolen_at

        assert mutex._renew_task is not None
        await _wait_until(lambda: mutex._renew_task is not None and mutex._renew_task.done())
        assert mutex._renew_task.exception() is None

        spec = fake_api.spec("tf-tenant-t-002")
        assert spec["holderIdentity"] == "other-pod-deadbeef"
        assert spec["renewTime"] == stolen_at, "a renewal must never touch a stolen lease"

    # release() must not clobber the new holder either.
    assert fake_api.spec("tf-tenant-t-002")["holderIdentity"] == "other-pod-deadbeef"


@pytest.mark.asyncio
async def test_transient_renewal_failure_keeps_renewing(fake_api: _FakeLeasesApi) -> None:
    mutex = KubeLeaseMutex(
        "tf-tenant-t-003",
        lease_duration_seconds=1,
        renew_interval_seconds=0.05,
        acquire_timeout_seconds=1,
    )
    async with mutex:
        acquired_at = _parse_rfc3339(fake_api.spec("tf-tenant-t-003")["renewTime"])
        assert acquired_at is not None
        fake_api.fail_next_puts = 1
        # One failed PUT, then at least one successful renewal after it.
        await _wait_until(lambda: fake_api.put_count >= 3)
        assert mutex._renew_task is not None and not mutex._renew_task.done()
        renewed = _parse_rfc3339(fake_api.spec("tf-tenant-t-003")["renewTime"])
        assert renewed is not None and renewed > acquired_at


@pytest.mark.asyncio
async def test_dead_holder_is_stolen_after_one_short_duration(
    fake_api: _FakeLeasesApi,
) -> None:
    """The 2026-09-06 shape: a holder that died without releasing. With a
    short duration the next waiter takes over as soon as it expires,
    instead of after the whole apply budget."""
    dead_renew = time.time() - 2.0
    fake_api.leases["tf-tenant-t-004"] = {
        "apiVersion": "coordination.k8s.io/v1",
        "kind": "Lease",
        "metadata": {"name": "tf-tenant-t-004", "namespace": "platform-tst"},
        "spec": {
            "holderIdentity": "terraformer-evicted-2ddqj",
            "leaseDurationSeconds": 1,
            "acquireTime": _now_rfc3339(),
            "renewTime": time.strftime(
                "%Y-%m-%dT%H:%M:%S.000000Z", time.gmtime(dead_renew)
            ),
            "leaseTransitions": 3,
        },
    }
    mutex = KubeLeaseMutex(
        "tf-tenant-t-004",
        lease_duration_seconds=1,
        renew_interval_seconds=0.2,
        acquire_timeout_seconds=1,
        poll_interval_seconds=0.01,
    )
    started = time.monotonic()
    async with mutex:
        assert time.monotonic() - started < 0.5, "an expired lease is stolen at once"
        spec = fake_api.spec("tf-tenant-t-004")
        assert spec["holderIdentity"] == mutex.holder
        assert spec["leaseTransitions"] == 4


@pytest.mark.asyncio
async def test_live_holder_blocks_a_waiter_until_expiry(fake_api: _FakeLeasesApi) -> None:
    """The other half of the contract: a holder that IS renewing never
    loses the lease to a waiter, even across many durations."""
    holder = KubeLeaseMutex(
        "tf-tenant-t-005",
        lease_duration_seconds=1,
        renew_interval_seconds=0.05,
        acquire_timeout_seconds=1,
    )
    waiter = KubeLeaseMutex(
        "tf-tenant-t-005",
        lease_duration_seconds=1,
        renew_interval_seconds=0.05,
        # Must outlast the sleep below: the waiter's own budget expiring
        # would end the task too, for a reason unrelated to theft.
        acquire_timeout_seconds=5,
        poll_interval_seconds=0.02,
    )
    async with holder:
        task = asyncio.ensure_future(waiter.acquire())
        await asyncio.sleep(1.3)  # > lease_duration_seconds
        assert not task.done(), "a renewing holder must not be stolen from"
        assert fake_api.spec("tf-tenant-t-005")["holderIdentity"] == holder.holder
    await asyncio.wait_for(task, timeout=2)
    assert fake_api.spec("tf-tenant-t-005")["holderIdentity"] == waiter.holder
    await waiter.release()


@pytest.mark.asyncio
async def test_renewal_stops_when_the_lease_object_is_deleted(
    fake_api: _FakeLeasesApi,
) -> None:
    """An operator deleting the Lease out from under a holder is the
    other 'not ours any more' shape: stop renewing, and let release()
    find nothing to clear."""
    mutex = KubeLeaseMutex(
        "tf-tenant-t-006",
        lease_duration_seconds=1,
        renew_interval_seconds=0.05,
        acquire_timeout_seconds=1,
    )
    async with mutex:
        del fake_api.leases["tf-tenant-t-006"]
        assert mutex._renew_task is not None
        await _wait_until(lambda: mutex._renew_task is not None and mutex._renew_task.done())
        assert mutex._renew_task.exception() is None
    assert "tf-tenant-t-006" not in fake_api.leases, "renewal must not resurrect it"


@pytest.mark.asyncio
async def test_renewal_treats_a_write_conflict_as_still_held(
    fake_api: _FakeLeasesApi, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 409 on the renewal PUT (resourceVersion conflict) is not
    evidence of loss — the next tick re-reads and decides."""
    real_handler = fake_api.handler
    conflicts = {"left": 1}

    def _handler(request: httpx.Request) -> httpx.Response:
        if request.method == "PUT" and conflicts["left"] > 0:
            conflicts["left"] -= 1
            return httpx.Response(409, json={"message": "conflict"})
        return real_handler(request)

    monkeypatch.setattr(fake_api, "handler", _handler)
    mutex = KubeLeaseMutex(
        "tf-tenant-t-007",
        lease_duration_seconds=1,
        renew_interval_seconds=0.05,
        acquire_timeout_seconds=1,
    )
    async with mutex:
        acquired_at = _parse_rfc3339(fake_api.spec("tf-tenant-t-007")["renewTime"])
        assert acquired_at is not None
        await _wait_until(lambda: fake_api.put_count >= 3)
        assert conflicts["left"] == 0
        assert mutex._renew_task is not None and not mutex._renew_task.done()
        renewed = _parse_rfc3339(fake_api.spec("tf-tenant-t-007")["renewTime"])
        assert renewed is not None and renewed > acquired_at


@pytest.mark.asyncio
async def test_crashed_renewer_still_releases_on_exit(
    fake_api: _FakeLeasesApi, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the renewal task dies at startup (its client cannot even be
    built), the mutex must still release the lease on exit rather than
    let the crash mask the release."""
    real_client = KubeLeaseMutex._client
    calls = {"n": 0}

    def _flaky_client(self: KubeLeaseMutex) -> httpx.AsyncClient:
        calls["n"] += 1
        if calls["n"] == 2:  # 1 = acquire, 2 = renew loop, 3 = release
            raise RuntimeError("token file vanished")
        return real_client(self)

    monkeypatch.setattr(KubeLeaseMutex, "_client", _flaky_client)
    mutex = KubeLeaseMutex(
        "tf-tenant-t-008",
        lease_duration_seconds=1,
        renew_interval_seconds=0.05,
        acquire_timeout_seconds=1,
    )
    async with mutex:
        assert mutex._renew_task is not None
        await _wait_until(lambda: mutex._renew_task is not None and mutex._renew_task.done())
        assert isinstance(mutex._renew_task.exception(), RuntimeError)
    assert calls["n"] == 3
    assert fake_api.spec("tf-tenant-t-008")["holderIdentity"] is None


def test_renew_interval_must_fit_inside_the_duration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PNEUMA_NAMESPACE", "platform-tst")
    with pytest.raises(ValueError):
        KubeLeaseMutex("x", lease_duration_seconds=60, renew_interval_seconds=60)
    with pytest.raises(ValueError):
        KubeLeaseMutex("x", lease_duration_seconds=60, renew_interval_seconds=0)
    # No renewal at all stays valid — reconcile_cli's contract.
    mutex = KubeLeaseMutex("x", lease_duration_seconds=60)
    assert mutex.renew_interval_seconds is None
