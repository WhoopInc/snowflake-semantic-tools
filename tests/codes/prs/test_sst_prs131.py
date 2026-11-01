"""SST-PRS131: a field SST reads without checking its kind holds a value of another kind."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.tools import load_tool_catalog
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Origin, Severity
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from tests.helpers.diagnostic_filters import coded
from tests.helpers.file_trees import write_tree

MEMBER = (
    "tool:\n  name: fee_calc\n  type: function\n{description}  language: sql\n  returns: NUMBER\n"
    "  body: \"{{{{ file('fee.sql') }}}}\"\n"
)


def _found(tmp_path: Path, description: str) -> list[Diagnostic]:
    write_tree(
        tmp_path, {"tools/fee.yml": MEMBER.format(description=description), "tools/fee.sql": "order_total * 0.1\n"}
    )
    catalog = load_tool_catalog(
        tmp_path, DbtCatalog("v12", None, None, ()), target_name="dev", declared_targets=frozenset(("dev",))
    )
    return coded(catalog.diagnostics, "SST-PRS131")


def test_sst_prs131_fires(tmp_path: Path) -> None:
    [diagnostic] = _found(tmp_path, "  description:\n    text: Fee.\n")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "tool:fee_calc: 'description' is a mapping, not text"
    assert diagnostic.subject == "tool:fee_calc"
    assert diagnostic.origin == Origin("tools/fee.yml", 5, 5)


def test_sst_prs131_silent(tmp_path: Path) -> None:
    assert _found(tmp_path, "  description: Fee.\n") == []
