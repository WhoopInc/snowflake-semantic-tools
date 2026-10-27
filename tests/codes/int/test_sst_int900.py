"""SST-INT900: a code the registry does not hold was passed to `D`."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import D, Severity


def test_sst_int900_fires() -> None:
    diagnostic = D("SST-ABC001", value="x")
    assert (diagnostic.code, diagnostic.severity) == ("SST-INT900", Severity.ERROR)
    assert diagnostic.message == "unregistered code SST-ABC001"


def test_sst_int900_silent() -> None:
    assert D("SST-REF001", model="orders").code == "SST-REF001"
