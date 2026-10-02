"""SST-INT901: a code's template names a placeholder its emit site did not supply."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import D, Severity


def test_sst_int901_fires() -> None:
    diagnostic = D("SST-REF002", model="orders")
    assert (diagnostic.code, diagnostic.severity) == ("SST-INT901", Severity.ERROR)
    assert diagnostic.message == "SST-REF002 template needs column, which was not supplied"


def test_sst_int901_silent() -> None:
    assert D("SST-REF002", model="orders", column="id").code == "SST-REF002"
