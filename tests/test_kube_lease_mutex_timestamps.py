"""Regression: Lease acquireTime/renewTime are metav1.MicroTime fields.

The Kubernetes API server rejects (400 Bad Request) any MicroTime value
without exactly six fractional digits. A seconds-only `_now_rfc3339`
wedged EVERY tenant provisioning on TST (2026-08-19): each
RunTenantReconcile died at lease creation before terraform ever ran.
These tests pin the wire format in both directions so the format can
never silently regress to a shape the API refuses.
"""

import re

from services.terraformer.src.kube_lease_mutex import (
    _now_rfc3339,
    _parse_rfc3339,
)

_MICROTIME = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$")


def test_now_rfc3339_is_microtime_shaped():
    for _ in range(3):
        value = _now_rfc3339()
        assert _MICROTIME.match(value), (
            f"{value!r} is not MicroTime-shaped — the Lease API 400s any "
            "acquireTime/renewTime without exactly six fractional digits"
        )


def test_parse_accepts_api_returned_microtime():
    # What the API server itself returns for stored leases.
    assert _parse_rfc3339("2026-08-18T22:49:31.123456Z") is not None


def test_parse_accepts_legacy_seconds_only():
    # Leases written by the pre-fix build must still be readable so an
    # in-place upgrade can evaluate (and expire) them.
    assert _parse_rfc3339("2026-08-18T22:49:31Z") is not None


def test_emit_parse_round_trip():
    value = _now_rfc3339()
    parsed = _parse_rfc3339(value)
    assert parsed is not None


def test_parse_rejects_garbage():
    assert _parse_rfc3339("not-a-time") is None
    assert _parse_rfc3339(None) is None
