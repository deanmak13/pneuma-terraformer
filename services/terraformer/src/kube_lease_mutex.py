"""Single-flight mutex for the platform-secrets-reconcile automation,
backed by a `coordination.k8s.io/v1` Lease.

WHY THIS EXISTS: reconcile_cli.py (this package) is invoked from THREE
independent paths that can overlap in time — the CronJob's own 15-min
schedule (`concurrencyPolicy: Forbid` only serialises THAT CronJob's own
scheduled Jobs against each other), and two on-merge ad-hoc
`kubectl create job --from=cronjob/platform-secrets-reconcile` triggers
(one per source repo: pneuma-deployments, pneuma-helm-charts) that are
NOT CronJob-scheduled Jobs and so sit outside `concurrencyPolicy`'s
reach entirely. The Terraform 1.9 S3 backend these harnesses use has no
native lockfile either (see platform-secrets-apply's own header comment:
"State is held in-memory... each apply is a fresh reconcile"). Without
an explicit mutex, two of these three triggers landing within the same
apply window race the same env-scoped tfstate.

DESIGN: a plain REST client against the in-cluster Kubernetes API using
the pod's own projected ServiceAccount token — same auth mechanism
`terraform_runner._provider_env` already uses for the Kubernetes
Terraform provider (see `_KUBE_SA_TOKEN_PATH` / `_KUBE_SA_CA_CERT_PATH`,
imported from there rather than re-derived here). No `kubernetes` python
client dependency added — this repo's convention is raw `httpx` against
typed/REST surfaces, not a vendored SDK.

This is a Lease used as an ADVISORY MUTEX, not full leader-election: one
holder at a time, steal-on-expiry, best-effort release. Renewal is
opt-in (`renew_interval_seconds`): reconcile_cli.py sizes its
`lease_duration_seconds` to the Job's own hard deadline and never
renews, while the per-tenant lease in terraform_runner.py renews on a
short duration so a holder that dies mid-apply (kubelet eviction,
OOM, node loss) frees the tenant within seconds rather than leaving
the next signup to wait out a 15-minute lease — which is exactly what
happened on TST 2026-09-06 10:02Z: a pod evicted 14s into an apply,
and its successor polled the dead holder's lease for the full duration.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import httpx

from services.terraformer.src.terraform_runner import (
    _KUBE_SA_CA_CERT_PATH,
    _KUBE_SA_TOKEN_PATH,
)

_LOG = logging.getLogger("terraformer.kube_lease_mutex")

_LEASE_API_VERSION = "coordination.k8s.io/v1"


class LeaseAcquireTimeout(RuntimeError):
    """Raised when the mutex could not be acquired within
    `acquire_timeout_seconds` — the caller should treat this as a
    fail-loud error (non-zero exit), never a silent skip. A timeout
    here means either a genuinely long-running concurrent reconcile (in
    which case waiting longer wouldn't have helped within this Job's
    own `activeDeadlineSeconds`) or a stuck/never-released lease that
    hasn't hit its `leaseDurationSeconds` expiry yet."""


def _now_rfc3339() -> str:
    # Lease acquireTime/renewTime are metav1.MicroTime fields: the API
    # server rejects (400 Bad Request) timestamps without exactly six
    # fractional digits, so `%f` (always six digits) is load-bearing.
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _parse_rfc3339(value: str | None) -> float | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(value, fmt).replace(
                tzinfo=timezone.utc
            ).timestamp()
        except ValueError:
            continue
    return None


def _lease_expired(spec: dict[str, Any]) -> bool:
    renew_time = _parse_rfc3339(spec.get("renewTime"))
    duration = spec.get("leaseDurationSeconds")
    if renew_time is None or not isinstance(duration, (int, float)):
        # A holder present but with an unparseable/missing renewTime or
        # duration is treated as expired — a Lease this mutex cannot
        # interpret is not one it can safely wait out.
        return True
    return time.time() > renew_time + float(duration)


class KubeLeaseMutex:
    """`async with KubeLeaseMutex("platform-secrets-reconcile"):` around
    the reconcile section. Namespace defaults to `PNEUMA_NAMESPACE`
    (downward-API env var every reconcile Job pod already carries — see
    pneuma-deployments platform/base/platform-secrets-reconcile/
    cronjob.yaml)."""

    def __init__(
        self,
        name: str,
        *,
        namespace: str | None = None,
        lease_duration_seconds: int = 900,
        acquire_timeout_seconds: int = 900,
        poll_interval_seconds: float = 5.0,
        renew_interval_seconds: float | None = None,
        holder: str | None = None,
    ) -> None:
        self.name = name
        self.namespace = namespace or os.environ.get("PNEUMA_NAMESPACE", "")
        if not self.namespace:
            raise ValueError(
                "PNEUMA_NAMESPACE is not set — required to scope the "
                "reconcile Lease mutex to this pod's own namespace"
            )
        if renew_interval_seconds is not None and not (
            0 < renew_interval_seconds < lease_duration_seconds
        ):
            # A renewal that lands after the lease has already expired is
            # no renewal at all — the next waiter has legitimately stolen
            # it by then.
            raise ValueError(
                f"renew_interval_seconds={renew_interval_seconds!r} must be "
                f"positive and shorter than lease_duration_seconds="
                f"{lease_duration_seconds!r}"
            )
        self.lease_duration_seconds = lease_duration_seconds
        self.acquire_timeout_seconds = acquire_timeout_seconds
        self.poll_interval_seconds = poll_interval_seconds
        self.renew_interval_seconds = renew_interval_seconds
        self._renew_task: asyncio.Task[None] | None = None
        # HOSTNAME is the pod name on every k8s pod (kubelet sets it) —
        # combined with a short random suffix so two pods that somehow
        # share a HOSTNAME (never true in-cluster, but defends a local
        # dev shell) can't be mistaken for the same holder.
        self.holder = holder or f"{os.environ.get('HOSTNAME', 'reconcile-cli')}-{uuid.uuid4().hex[:8]}"

    def _base_url(self) -> str:
        host = os.environ.get("KUBERNETES_SERVICE_HOST", "")
        port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
        if not host:
            raise RuntimeError(
                "KUBERNETES_SERVICE_HOST is not set — the reconcile Lease "
                "mutex only works in-cluster (this is the standard "
                "kubelet-injected env var every pod gets)"
            )
        return f"https://{host}:{port}/apis/{_LEASE_API_VERSION}/namespaces/{self.namespace}/leases"

    def _client(self) -> httpx.AsyncClient:
        token = _KUBE_SA_TOKEN_PATH.read_text().strip()
        return httpx.AsyncClient(
            verify=str(_KUBE_SA_CA_CERT_PATH),
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            timeout=10.0,
        )

    async def acquire(self) -> None:
        deadline = time.monotonic() + self.acquire_timeout_seconds
        async with self._client() as client:
            while True:
                if await self._try_acquire(client):
                    _LOG.info(
                        "reconcile lease %s/%s acquired by %s",
                        self.namespace,
                        self.name,
                        self.holder,
                    )
                    return
                if time.monotonic() >= deadline:
                    raise LeaseAcquireTimeout(
                        f"could not acquire lease {self.name!r} in "
                        f"{self.namespace!r} within "
                        f"{self.acquire_timeout_seconds}s — another "
                        f"reconcile is holding it and has not expired"
                    )
                await asyncio.sleep(self.poll_interval_seconds)

    async def _try_acquire(self, client: httpx.AsyncClient) -> bool:
        base = self._base_url()
        resp = await client.get(f"{base}/{self.name}")
        now = _now_rfc3339()

        if resp.status_code == 404:
            body = {
                "apiVersion": _LEASE_API_VERSION,
                "kind": "Lease",
                "metadata": {"name": self.name, "namespace": self.namespace},
                "spec": {
                    "holderIdentity": self.holder,
                    "leaseDurationSeconds": self.lease_duration_seconds,
                    "acquireTime": now,
                    "renewTime": now,
                    "leaseTransitions": 0,
                },
            }
            create_resp = await client.post(base, json=body)
            if create_resp.status_code in (200, 201):
                return True
            if create_resp.status_code == 409:
                # Raced: someone else created it between our GET 404 and
                # this POST. Not acquired — loop and re-evaluate.
                return False
            create_resp.raise_for_status()
            return False

        resp.raise_for_status()
        lease = resp.json()
        spec = lease.get("spec", {}) or {}
        current_holder = spec.get("holderIdentity")

        if current_holder == self.holder:
            # Already ours (e.g. a retry after a transient network error
            # on the previous acquire attempt's response).
            return True

        if current_holder and not _lease_expired(spec):
            return False

        # Either unheld (holderIdentity empty/null — a prior release) or
        # expired — steal it.
        spec["holderIdentity"] = self.holder
        spec["acquireTime"] = now
        spec["renewTime"] = now
        spec["leaseDurationSeconds"] = self.lease_duration_seconds
        spec["leaseTransitions"] = int(spec.get("leaseTransitions") or 0) + 1
        lease["spec"] = spec

        update_resp = await client.put(f"{base}/{self.name}", json=lease)
        if update_resp.status_code == 200:
            return True
        if update_resp.status_code == 409:
            # Someone else raced us for the same steal — resourceVersion
            # conflict. Not acquired — loop and re-evaluate.
            return False
        update_resp.raise_for_status()
        return False

    async def release(self) -> None:
        """Best-effort: clears holderIdentity so the NEXT acquire doesn't
        have to wait out the full `leaseDurationSeconds`. Never raises —
        a failed release just leaves the lease to self-expire, the same
        steady-state a crashed holder already relies on."""
        try:
            async with self._client() as client:
                base = self._base_url()
                resp = await client.get(f"{base}/{self.name}")
                if resp.status_code != 200:
                    return
                lease = resp.json()
                spec = lease.get("spec", {}) or {}
                if spec.get("holderIdentity") != self.holder:
                    # Already stolen (we overran leaseDurationSeconds) —
                    # releasing now would clobber the new holder.
                    return
                spec["holderIdentity"] = None
                lease["spec"] = spec
                await client.put(f"{base}/{self.name}", json=lease)
        except Exception:
            _LOG.warning(
                "reconcile lease %s/%s release failed (non-fatal — "
                "self-expires after leaseDurationSeconds)",
                self.namespace,
                self.name,
                exc_info=True,
            )

    async def _renew_once(self, client: httpx.AsyncClient) -> bool:
        """Push `renewTime` forward if the lease is still ours. Returns
        False — and the caller stops renewing — once another holder has
        taken it: we overran `leaseDurationSeconds` somewhere (a stalled
        event loop, an API outage longer than the lease) and the steal
        was legitimate; clobbering it back would put two appliers on the
        same tenant, which is the one thing this mutex exists to stop."""
        base = self._base_url()
        resp = await client.get(f"{base}/{self.name}")
        if resp.status_code == 404:
            return False
        resp.raise_for_status()
        lease = resp.json()
        spec = lease.get("spec", {}) or {}
        if spec.get("holderIdentity") != self.holder:
            return False
        spec["renewTime"] = _now_rfc3339()
        spec["leaseDurationSeconds"] = self.lease_duration_seconds
        lease["spec"] = spec
        update_resp = await client.put(f"{base}/{self.name}", json=lease)
        if update_resp.status_code == 409:
            # resourceVersion conflict — someone touched the object
            # between our GET and PUT. Treat as still-ours for this tick;
            # the next tick re-reads and decides.
            return True
        update_resp.raise_for_status()
        return True

    async def _renew_loop(self, interval: float) -> None:
        async with self._client() as client:
            while True:
                await asyncio.sleep(interval)
                try:
                    still_ours = await self._renew_once(client)
                except Exception:
                    # (CancelledError is a BaseException — it passes
                    # straight through this clause, as it must.)
                    # Transient API failure: keep trying — the lease has
                    # `lease_duration_seconds - interval` of slack before
                    # a waiter may steal it, and a single failed tick is
                    # not evidence that we lost it.
                    _LOG.warning(
                        "lease %s/%s renewal failed for %s (retrying)",
                        self.namespace,
                        self.name,
                        self.holder,
                        exc_info=True,
                    )
                    continue
                if not still_ours:
                    _LOG.error(
                        "lease %s/%s no longer held by %s — renewal "
                        "stopped; the holder overran its lease duration",
                        self.namespace,
                        self.name,
                        self.holder,
                    )
                    return

    async def __aenter__(self) -> "KubeLeaseMutex":
        await self.acquire()
        if self.renew_interval_seconds is not None:
            self._renew_task = asyncio.create_task(
                self._renew_loop(self.renew_interval_seconds),
                name=f"lease-renew:{self.namespace}/{self.name}",
            )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        task, self._renew_task = self._renew_task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                _LOG.warning(
                    "lease %s/%s renewal task ended with an error",
                    self.namespace,
                    self.name,
                    exc_info=True,
                )
        await self.release()
