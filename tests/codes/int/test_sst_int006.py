"""SST-INT006: a diagnostic's fingerprint differs between two identical runs."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import D, Severity, unstable_fingerprints


def test_sst_int006_fires() -> None:
    first = (D("SST-REF001", model="orders", subject="metric:m"),)
    second = (D("SST-REF001", model="orders_2", subject="metric:m"),)
    [diagnostic] = unstable_fingerprints(first, second)
    assert (diagnostic.code, diagnostic.severity) == ("SST-INT006", Severity.ERROR)
    assert diagnostic.message == "fingerprint for SST-REF001 changed between identical runs"


def test_sst_int006_silent() -> None:
    first = (D("SST-REF001", model="orders", subject="metric:m"), D("SST-LOD003", file="a.yml"))
    assert unstable_fingerprints(first, tuple(reversed(first))) == ()
