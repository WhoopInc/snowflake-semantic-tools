"""SST-PRS013: a closed field, here an eval system metric's version, holds a value it does not allow."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.yaml.tools import load_tool_catalog
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from tests.helpers.diagnostic_filters import coded
from tests.helpers.prs_codes import VALUE, eval_catalog_findings


def test_sst_prs013_fires() -> None:
    metric = replace(VALUE.config.system_metrics[0], version="v2")
    [diagnostic] = eval_catalog_findings(
        "SST-PRS013", replace(VALUE, config=replace(VALUE.config, system_metrics=(metric,)))
    )
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message.endswith("is 'v2', expected one of v3")
    assert diagnostic.subject == "eval:sales_agent"


def test_sst_prs013_silent() -> None:
    assert eval_catalog_findings("SST-PRS013") == []


def _refresh_mode_findings(tmp_path: Path, written: str) -> list[Diagnostic]:
    """The SST-PRS013 a search service whose `refresh_mode` is written as `written` reports."""
    tool = "tool:\n  name: menu_search\n  type: cortex_search_service\n  description: Searches products.\n"
    (tmp_path / "tools").mkdir()
    (tmp_path / "tools" / "menu_search.yml").write_text(tool + f"  refresh_mode: {written}\n", encoding="utf-8")
    catalog = load_tool_catalog(
        tmp_path, DbtCatalog("v12", None, None, ()), target_name="dev", declared_targets=frozenset(("dev",))
    )
    return coded(catalog.diagnostics, "SST-PRS013")


@pytest.mark.parametrize(
    ("written", "found"), [("yes", "true"), ("true", "true"), ("no", "false"), ("", "null"), ("''", ""), ("3", "3")]
)
def test_a_refresh_mode_that_is_not_text_is_refused_not_dropped(tmp_path: Path, written: str, found: str) -> None:
    [diagnostic] = _refresh_mode_findings(tmp_path, written)
    assert diagnostic.message == (
        f"tool:menu_search: 'refresh_mode' is '{found}', expected one of AUTO, FULL, INCREMENTAL"
    )
    assert diagnostic.origin is not None and diagnostic.origin.line == 5


@pytest.mark.parametrize("written", ["FULL", "incremental"])
def test_a_refresh_mode_snowflake_accepts_is_not_refused(tmp_path: Path, written: str) -> None:
    assert _refresh_mode_findings(tmp_path, written) == []
