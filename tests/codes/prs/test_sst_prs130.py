"""SST-PRS130: a tool member's signature parameter writes `required`, though every one is required."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.tools import load_tool_catalog
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Origin, Severity
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from tests.helpers.diagnostic_filters import coded
from tests.helpers.file_trees import write_tree

MEMBER = (
    "tool:\n  name: fee_calc\n  type: function\n  description: Fee.\n  language: sql\n  returns: NUMBER\n"
    "  body: \"{{{{ file('fee.sql') }}}}\"\n  signature:\n    - name: order_total\n      type: NUMBER(10, 2)\n"
    "    - name: tip_pct\n      type: NUMBER\n{extra}"
)


def _found(tmp_path: Path, extra: str) -> tuple[list[Diagnostic], list[str]]:
    write_tree(tmp_path, {"tools/fee.yml": MEMBER.format(extra=extra), "tools/fee.sql": "order_total * 0.1\n"})
    catalog = load_tool_catalog(
        tmp_path, DbtCatalog("v12", None, None, ()), target_name="dev", declared_targets=frozenset(("dev",))
    )
    [member] = catalog.members
    return coded(catalog.diagnostics, "SST-PRS130"), [parameter.name for parameter in member.signature]


def test_sst_prs130_fires(tmp_path: Path) -> None:
    [diagnostic], parameters = _found(tmp_path, "      required: false\n")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "tool:fee_calc: 'signature.tip_pct.required' is not written: every signature parameter is required"
    )
    assert diagnostic.subject == "tool:fee_calc"
    assert diagnostic.origin == Origin("tools/fee.yml", 2, 3)
    assert parameters == ["order_total", "tip_pct"]


def test_sst_prs130_silent(tmp_path: Path) -> None:
    assert _found(tmp_path, "") == ([], ["order_total", "tip_pct"])
