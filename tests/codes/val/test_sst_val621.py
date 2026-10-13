"""SST-VAL621: a JavaScript or SQL routine sets a key only Java, Python and Scala routines take."""

from __future__ import annotations

from typing import Any

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.agent_builders import group, procedure_member
from tests.helpers.diagnostic_filters import coded
from tests.helpers.val_codes import checked_tools

HANDLER_KEYS: dict[str, Any] = {
    "runtime_version": "3.11",
    "handler": "run",
    "packages": ("snowflake-snowpark-python",),
    "imports": ("@s/x.py",),
}


def test_sst_val621_fires() -> None:
    member = procedure_member(reference=False, language="javascript", body="return 1;", **HANDLER_KEYS)
    found = coded(checked_tools(group("platform", member)), "SST-VAL621")
    assert [item.context["key"] for item in found] == ["runtime_version", "handler", "packages", "imports"]
    assert {item.severity for item in found} == {Severity.ERROR}
    assert found[2].message == "tool member 'lookup' is written in javascript, which takes no 'packages'"
    assert found[2].subject == "tool:lookup"
    sql_member = procedure_member(reference=False, packages=("x",))
    assert [item.context["key"] for item in coded(checked_tools(group("platform", sql_member)), "SST-VAL621")] == [
        "packages"
    ]


def test_sst_val621_silent() -> None:
    python = procedure_member(reference=False, language="python", body="def run(): return 1", **HANDLER_KEYS)
    assert (
        coded(checked_tools(group("platform", python, procedure_member("plain", reference=False))), "SST-VAL621") == []
    )
