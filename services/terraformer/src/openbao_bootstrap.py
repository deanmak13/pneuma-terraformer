"""Terraformer bootstraps its own OpenBao authority AND proves that
authority's policy is current with git, at every boot.

INCIDENT 1 (feat/openbao-k8s-auth): the terraformer's stored
OPENBAO_ADMIN_TOKEN (the static VAULT_TOKEN) expired silently, and
`auth/token/lookup-self` — which needs no policy at all — returned 403,
meaning every tenant `apply` failed with `permission denied`. A stored,
expirable credential is the failure class, not a particular expiry date;
the replacement (Kubernetes auth — see settings.vault_k8s_auth_role /
vault_k8s_auth_mount and terraform_runner._vault_provider_hcl) removes it
by having the pod exchange its own identity for a short-lived token on
every terraform run.

INCIDENT 2 (2026-09-07, pneuma#669): the FIRST version of this module's
converge flow treated a successful kubernetes-auth LOGIN as proof the
role AND its policy were current, and returned early. Login only proves
the role exists and trusts this ServiceAccount — it says nothing about
whether the POLICY attached to that role matches what git declares. A
policy amendment (e.g. the platform-secrets fan-out targets grant) landed
in the standalone `platform-auth-bootstrap` harness and sat inert for 20
days: every boot logged in fine, no-op'd, and never re-applied the
amendment, because nothing ever re-ran `terraform plan` against the live
policy. The CronJob's `lastSuccessfulTime` stayed empty the whole time.

Converge flow (ensure_platform_auth), run on every boot:

  (a) Try `POST {VAULT_ADDR}/v1/auth/<mount>/login` with this pod's own
      projected ServiceAccount JWT (see _k8s_login). A 200 carrying a
      usable `auth.client_token` mints a short-lived kubernetes-auth
      token; anything else (non-200, transport error, or a 200 with no
      usable token — unknown is never benign) reports "cannot log in".
  (b) If login minted a token, spend it PROVING currency, not merely
      existence: run `terraform plan -detailed-exitcode` against the
      SAME platform-auth-bootstrap harness apply_platform_auth applies
      (TerraformRunner.plan_platform_auth). Exit 0 means the live
      policy/role match git — the steady state, no-op, return. Exit 2
      (drift) or anything else (the plan could not even run — e.g. this
      identity's self-introspection grant itself went stale) both fall
      through to break-glass: an identity whose currency cannot be
      proven is not proven to work.
  (c) BREAK GLASS in-process (cold start, drift, or an unprovable plan):
        1. Read the Shamir unseal shares from the k8s Secret named by
           settings.openbao_bootstrap_secret_name, in namespace
           settings.openbao_namespace (see k8s_api.py).
        2. Drive OpenBao's HTTP generate-root flow
           (/v1/sys/generate-root/attempt -> .../update per share ->
           /v1/sys/decode-token) to mint a ONE-SHOT root token.
        3. Apply the `platform-auth-bootstrap` standalone Terraform
           harness through the existing TerraformRunner — converges
           vault_policy.terraformer + vault_kubernetes_auth_backend_role.terraformer
           to exactly what git declares (see TerraformRunner.
           apply_platform_auth for exactly what that harness contains
           and where it's baked from).
        4. Revoke the minted root token via `/v1/auth/token/revoke-self`
           in a `finally` — unconditionally, even if step 3 raised. The
           token is never written to disk, logged, or returned from any
           function in this module.
  (d) Re-prove: login again AND plan again. BOTH must pass — a fresh
      login with a plan exit other than 0 means the just-applied harness
      still does not match what this pod can prove, and a failed login
      means the role itself never came up. Either raises
      PlatformAuthBootstrapError — the pod must not start serving traffic
      against an OpenBao identity it cannot prove works. This is bounded
      by construction: at most ONE root-token mint per call, no retry
      loop — the re-proof either passes or the function raises.

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

BootstrapAction = Literal["noop_role_and_policy_current", "break_glass_applied"]


class PlatformAuthBootstrapError(RuntimeError):
    """The converge loop could not prove its own OpenBao identity works,
    even after applying the break-glass module. The pod must not start."""


async def _k8s_login(settings: Settings, jwt: str) -> str | None:
    """POST the kubernetes-auth login and return the minted client token
    on success, else None. None means 'cannot log in today, or logged in
    without a usable token' — both send the caller to break-glass. A 200
    carrying no auth.client_token is NOT treated as success: an identity
    whose token cannot be used cannot prove anything (unknown is never
    benign). Never raises; a transport hiccup falls through to the
    break-glass path, whose own calls surface a clear error if OpenBao is
    genuinely unreachable."""
    url = f"{settings.vault_addr.rstrip('/')}/v1/auth/{settings.vault_k8s_auth_mount}/login"
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            resp = await client.post(
                url,
                json={"role": settings.vault_k8s_auth_role, "jwt": jwt},
            )
        except httpx.HTTPError as exc:
            _LOG.warning("kubernetes-auth login probe could not reach OpenBao: %s", exc)
            return None
    if resp.status_code != 200:
        return None
    try:
        body = resp.json()
    except ValueError:
        return None
    return (body.get("auth") or {}).get("client_token") or None


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
    """Converge this pod's OpenBao kubernetes-auth identity AND prove its
    policy is current with git. Idempotent — safe to call on every boot
    and from the admin endpoint on demand. Bounded: at most one
    break-glass root-token mint per call, no in-process retry loop."""
    role = settings.vault_k8s_auth_role
    mount = settings.vault_k8s_auth_mount

    jwt = k8s_api.read_own_sa_jwt()
    token = await _k8s_login(settings, jwt)

    if token is not None:
        try:
            code = await runner.plan_platform_auth(token)
        finally:
            # Unconditional: this k8s-auth token must not outlive the
            # single plan call it was minted for.
            await _revoke_token(settings, token)
        if code == 0:
            _LOG.info(
                "openbao kubernetes-auth role=%s mount=%s policy current "
                "(plan clean) — no-op", role, mount,
            )
            return "noop_role_and_policy_current"
        if code == 2:
            _LOG.warning(
                "openbao platform-auth DRIFT: `terraform plan` reports pending "
                "changes to role=%s / its policy — breaking glass to converge",
                role,
            )
        else:
            _LOG.warning(
                "openbao platform-auth currency CANNOT BE PROVEN (plan exit=%s) "
                "for role=%s — an identity whose currency cannot be proven is "
                "not proven to work; breaking glass to converge", code, role,
            )
    else:
        _LOG.warning(
            "openbao kubernetes-auth login failed for role=%s — breaking glass "
            "in-process to (re)create it", role,
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
    token2 = await _k8s_login(settings, jwt2)
    if token2 is None:
        raise PlatformAuthBootstrapError(
            f"openbao kubernetes-auth role={role!r} still fails login after "
            "applying platform-auth-bootstrap — refusing to start."
        )
    try:
        code2 = await runner.plan_platform_auth(token2)
    finally:
        await _revoke_token(settings, token2)
    if code2 != 0:
        raise PlatformAuthBootstrapError(
            f"openbao platform-auth for role={role!r} still reports "
            f"drift/unprovable currency (plan exit={code2}) after applying "
            "platform-auth-bootstrap — refusing to start. NOT retried "
            "in-process: a second identical apply cannot fix what the first "
            "could not."
        )
    _LOG.info(
        "openbao kubernetes-auth role=%s mount=%s converged via break-glass "
        "and verified (login ok, plan clean)", role, mount,
    )
    return "break_glass_applied"
