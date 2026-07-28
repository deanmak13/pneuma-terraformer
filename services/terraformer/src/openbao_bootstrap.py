"""Terraformer bootstraps its own OpenBao authority at every boot.

INCIDENT: the terraformer's stored OPENBAO_ADMIN_TOKEN (the static
VAULT_TOKEN retired in feat/openbao-k8s-auth) expired silently, and
`auth/token/lookup-self` — which needs no policy at all — returned 403,
meaning every tenant `apply` failed with `permission denied`. A stored,
expirable credential is the failure class, not a particular expiry date;
the replacement (Kubernetes auth — see settings.vault_k8s_auth_role /
vault_k8s_auth_mount and terraform_runner._vault_provider_hcl) removes it
by having the pod exchange its own identity for a short-lived token on
every terraform run.

But that k8s-auth ROLE has to exist first, and creating it normally
requires an OpenBao root token — which nobody can mint by hand in
production (no host access, no operator terraform run: LAW
platform-bootstrap-zero-touch). Per bootstrap-in-service-not-infra, the
OWNING SERVICE converges its own auth dependency at startup, idempotently,
on every boot, with the same function re-triggerable via an admin
endpoint (POST /provisioning/admin/bootstrap/openbao-auth).

Converge flow (ensure_platform_auth), run on every boot:

  (a) Try `POST {VAULT_ADDR}/v1/auth/kubernetes/login` with this pod's own
      projected ServiceAccount JWT. Success means the role already exists
      and is current — the steady state. No-op, return.
  (b) On failure (cold start: the role doesn't exist yet), BREAK GLASS
      in-process:
        1. Read the Shamir unseal shares from the k8s Secret named by
           settings.openbao_bootstrap_secret_name, in namespace
           settings.openbao_namespace (see k8s_api.py).
        2. Drive OpenBao's HTTP generate-root flow
           (/v1/sys/generate-root/attempt -> .../update per share ->
           /v1/sys/decode-token) to mint a ONE-SHOT root token.
        3. Apply the `platform-auth-bootstrap` standalone Terraform
           harness through the existing TerraformRunner — creates
           vault_policy.terraformer + vault_kubernetes_auth_backend_role.terraformer
           (see TerraformRunner.apply_platform_auth for exactly what that
           harness contains and where it's baked from).
        4. Revoke the minted root token via `/v1/auth/token/revoke-self`
           in a `finally` — unconditionally, even if step 3 raised. The
           token is never written to disk, logged, or returned from any
           function in this module.
  (c) Re-run step (a) to PROVE the role now works. If it still fails,
      raise PlatformAuthBootstrapError — the pod must not start serving
      traffic against an OpenBao identity it cannot prove works.

Decoding the root token via OpenBao's own `/v1/sys/decode-token` endpoint
(rather than re-implementing the OTP XOR client-side) keeps this module
from re-deriving vendor token-crypto it can get wrong silently; the
endpoint takes exactly the two public values generate-root already
produced (encoded_token, otp) and needs no prior authentication, which is
the whole point of the flow (there is no token yet at this point).
"""

from __future__ import annotations

import logging
from typing import Literal

import httpx

from services.terraformer.src import k8s_api
from services.terraformer.src.settings import Settings
from services.terraformer.src.terraform_runner import TerraformRunner

_LOG = logging.getLogger("terraformer.openbao_bootstrap")

BootstrapAction = Literal["noop_role_already_valid", "break_glass_applied"]


class PlatformAuthBootstrapError(RuntimeError):
    """The converge loop could not prove its own OpenBao identity works,
    even after applying the break-glass module. The pod must not start."""


async def _k8s_login_ok(settings: Settings, jwt: str) -> bool:
    """POST the kubernetes-auth login. True on 200 (role exists and is
    current); False on any other status or transport failure — both mean
    'cannot log in today', which is the one signal this function reports.
    Never raises: a network hiccup here is handled by falling through to
    the break-glass path, whose OWN calls will surface a clear error if
    OpenBao itself is genuinely unreachable."""
    url = f"{settings.vault_addr.rstrip('/')}/v1/auth/kubernetes/login"
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.post(
                url,
                json={"role": settings.vault_k8s_auth_role, "jwt": jwt},
            )
        except httpx.HTTPError as exc:
            _LOG.warning("kubernetes-auth login probe could not reach OpenBao: %s", exc)
            return False
    return resp.status_code == 200


async def _read_unseal_shares(settings: Settings) -> list[str]:
    secret = await k8s_api.read_namespaced_secret(
        settings.openbao_namespace, settings.openbao_bootstrap_secret_name
    )
    shares: list[str] = []
    for i in range(1, settings.openbao_unseal_key_count + 1):
        key_name = f"UNSEAL_KEY_{i}"
        if key_name not in secret:
            raise PlatformAuthBootstrapError(
                f"Secret {settings.openbao_bootstrap_secret_name!r} in namespace "
                f"{settings.openbao_namespace!r} is missing key {key_name!r} — "
                f"expected {settings.openbao_unseal_key_count} unseal shares "
                "(openbao_unseal_key_count)."
            )
        shares.append(secret[key_name])
    return shares


async def _generate_root_token(settings: Settings, shares: list[str]) -> str:
    """Drive OpenBao's generate-root flow end to end and return the
    decoded, plaintext root token. Never logs any request/response body —
    every one of them carries either an unseal share, the OTP, or the
    token itself."""
    base_url = settings.vault_addr.rstrip("/")
    async with httpx.AsyncClient(timeout=15.0) as client:
        attempt = await client.post(f"{base_url}/v1/sys/generate-root/attempt", json={})
        attempt.raise_for_status()
        state = attempt.json()
        nonce = state["nonce"]
        otp = state["otp"]

        complete_state: dict | None = None
        for share in shares:
            update = await client.post(
                f"{base_url}/v1/sys/generate-root/update",
                json={"key": share, "nonce": nonce},
            )
            update.raise_for_status()
            complete_state = update.json()
            if complete_state.get("complete"):
                break

        if not complete_state or not complete_state.get("complete"):
            raise PlatformAuthBootstrapError(
                "generate-root did not complete after submitting all "
                f"{len(shares)} configured unseal shares — check "
                "openbao_unseal_key_count against the cluster's actual "
                "Shamir threshold."
            )

        encoded_token = complete_state.get("encoded_token")
        if not encoded_token:
            raise PlatformAuthBootstrapError(
                "generate-root completed but returned no encoded_token."
            )

        decode = await client.post(
            f"{base_url}/v1/sys/decode-token",
            json={"encoded_token": encoded_token, "otp": otp},
        )
        decode.raise_for_status()
        token = decode.json()["data"]["token"]

    if not token:
        raise PlatformAuthBootstrapError("decode-token returned an empty root token.")
    return token


async def _revoke_token(settings: Settings, token: str) -> None:
    """Best-effort revoke of the break-glass root token. Logged failures
    here never re-raise: this always runs from a `finally`, and letting a
    revoke failure mask the real apply outcome (success OR the original
    apply exception) would be strictly worse than a live-until-TTL token
    (token_ttl on generate-root defaults short) plus a loud log line."""
    base_url = settings.vault_addr.rstrip("/")
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.post(
                f"{base_url}/v1/auth/token/revoke-self",
                headers={"X-Vault-Token": token},
            )
        except httpx.HTTPError:
            _LOG.exception(
                "revoke-self for the break-glass root token failed — it may "
                "still be live until its own TTL expires"
            )
            return
    if resp.status_code not in (200, 204):
        _LOG.error(
            "revoke-self for the break-glass root token returned HTTP %d — "
            "it may still be live until its own TTL expires",
            resp.status_code,
        )


async def ensure_platform_auth(settings: Settings, runner: TerraformRunner) -> BootstrapAction:
    """Converge this pod's OpenBao kubernetes-auth identity. Idempotent —
    safe (and cheap: one HTTP call) to call on every boot and from the
    admin endpoint on demand."""
    jwt = k8s_api.read_own_sa_jwt()
    if await _k8s_login_ok(settings, jwt):
        _LOG.info(
            "openbao kubernetes-auth role=%s mount=%s already valid — no-op",
            settings.vault_k8s_auth_role, settings.vault_k8s_auth_mount,
        )
        return "noop_role_already_valid"

    _LOG.warning(
        "openbao kubernetes-auth login failed for role=%s — breaking glass "
        "in-process to (re)create it",
        settings.vault_k8s_auth_role,
    )
    shares = await _read_unseal_shares(settings)
    root_token = await _generate_root_token(settings, shares)
    try:
        await runner.apply_platform_auth(root_token)
    finally:
        # Unconditional: even if apply_platform_auth raised, the minted
        # root token must not outlive this function.
        await _revoke_token(settings, root_token)

    jwt2 = k8s_api.read_own_sa_jwt()
    if not await _k8s_login_ok(settings, jwt2):
        raise PlatformAuthBootstrapError(
            f"openbao kubernetes-auth role={settings.vault_k8s_auth_role!r} "
            "still fails login after applying platform-auth-bootstrap — "
            "refusing to start."
        )
    _LOG.info(
        "openbao kubernetes-auth role=%s mount=%s (re)created via break-glass "
        "and verified",
        settings.vault_k8s_auth_role, settings.vault_k8s_auth_mount,
    )
    return "break_glass_applied"
