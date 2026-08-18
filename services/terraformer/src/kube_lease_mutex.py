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
holder at a time, steal-on-expiry, best-effort release. There is no
lease-renewal loop during the held section (a bounded terraform apply
inside `lease_duration_seconds` is the whole point — see
reconcile_cli.py's call site for the duration this is sized against).
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
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_rfc3339(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        ).timestamp()
    except ValueError:
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
        holder: str | None = None,
    ) -> None:
        self.name = name
        self.namespace = namespace or os.environ.get("PNEUMA_NAMESPACE", "")
        if not self.namespace:
            raise ValueError(
                "PNEUMA_NAMESPACE is not set — required to scope the "
                "reconcile Lease mutex to this pod's own namespace"
            )
        self.lease_duration_seconds = lease_duration_seconds
        self.acquire_timeout_seconds = acquire_timeout_seconds
        self.poll_interval_seconds = poll_interval_seconds
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

    async def __aenter__(self) -> "KubeLeaseMutex":
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.release()
