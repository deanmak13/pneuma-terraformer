"""Minimal in-cluster Kubernetes API client.

No `kubernetes` python package in this service's dependency closure (see
pyproject.toml) — adding one just to read a single Secret would be a heavy
new dependency for one call. Instead this talks to the API server directly
over HTTPS using the pod's own projected ServiceAccount credentials, the
same in-cluster identity signal terraform_runner._provider_env already
relies on for the Kubernetes *provider*'s env vars.

Used by services.terraformer.src.openbao_bootstrap to read the
Shamir-unseal-share Secret on the break-glass path — the ONLY caller today,
deliberately kept generic (namespace + name, not hardcoded to that one
Secret) so a future cross-namespace read doesn't need a second ad-hoc
client.
"""

from __future__ import annotations

import base64
import os
import ssl
from pathlib import Path

import httpx

# Standard projected-ServiceAccount paths — every pod gets these for free,
# mirrors the constants terraform_runner.py defines for the same files
# (kept as separate module-level names, not a shared import, so tests can
# monkeypatch either module's copy independently — see terraform_runner's
# own comment on why these are module constants rather than Settings
# fields).
SA_TOKEN_PATH = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")
SA_CA_CERT_PATH = Path("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")


class K8sApiError(RuntimeError):
    """Base class for every error this module raises."""


class K8sRbacError(K8sApiError):
    """The pod's own ServiceAccount lacks the RBAC to perform the read.

    Always carries a specific, actionable message naming the exact
    Role/RoleBinding shape needed — never a bare 'Forbidden'. Land the
    named RBAC in the terraformer Helm chart (pneuma-helm-charts) to
    resolve.
    """


def read_own_sa_jwt() -> str:
    """This pod's own projected ServiceAccount JWT — re-read from disk on
    every call (never cached) so kubelet's automatic token rotation is
    picked up for free, same convention as the generated provider_vault.tf
    (see terraform_runner._vault_provider_hcl)."""
    return SA_TOKEN_PATH.read_text().strip()


def _api_server_base_url() -> str:
    host = os.environ.get("KUBERNETES_SERVICE_HOST", "")
    port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
    if not host:
        raise K8sApiError(
            "KUBERNETES_SERVICE_HOST is not set — this process is not "
            "running in-cluster, so the Kubernetes API is unreachable."
        )
    return f"https://{host}:{port}"


async def read_namespaced_secret(namespace: str, name: str) -> dict[str, str]:
    """GET a Secret's `.data` map, base64-decoded to strings.

    Raises K8sRbacError (with the exact missing-RBAC shape named in the
    message) on a 403, K8sApiError for any other non-200 or transport
    failure. Never retries and never logs the response body — Secret
    payloads are exactly the thing this function exists to keep off disk
    and out of logs.
    """
    base_url = _api_server_base_url()
    token = read_own_sa_jwt()
    url = f"{base_url}/api/v1/namespaces/{namespace}/secrets/{name}"
    ssl_context = ssl.create_default_context(cafile=str(SA_CA_CERT_PATH))
    async with httpx.AsyncClient(verify=ssl_context, timeout=10.0) as client:
        try:
            resp = await client.get(url, headers={"Authorization": f"Bearer {token}"})
        except httpx.HTTPError as exc:
            raise K8sApiError(
                f"Kubernetes API unreachable reading Secret {name!r} in "
                f"namespace {namespace!r}: {exc}"
            ) from exc

    if resp.status_code == 403:
        raise K8sRbacError(
            f"Forbidden: this pod's ServiceAccount cannot read Secret {name!r} "
            f"in namespace {namespace!r}. This is a cross-namespace read — the "
            f"terraformer chart's own namespace RBAC cannot grant it. Requires "
            f"a Role in namespace {namespace!r} granting "
            f'`resources: ["secrets"], resourceNames: ["{name}"], verbs: '
            f'["get"]` bound via a RoleBinding in that same namespace to this '
            f"pod's ServiceAccount (see the terraformer Helm chart's "
            f"pneuma-helm-charts values for the SA name/namespace)."
        )
    if resp.status_code != 200:
        raise K8sApiError(
            f"Unexpected HTTP {resp.status_code} reading Secret {name!r} in "
            f"namespace {namespace!r}"
        )

    body = resp.json()
    data = body.get("data") or {}
    return {k: base64.b64decode(v).decode("utf-8") for k, v in data.items()}
