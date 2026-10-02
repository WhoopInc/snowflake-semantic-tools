"""SST-APL001: Snowflake rejected a statement apply ran."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.app_ports import InMemorySnowflake, failed
from tests.helpers.apply_runs import apply_plan, codes, only
from tests.helpers.artifact_builders import change, changeset, rendered


def test_sst_apl001_fires() -> None:
    port = InMemorySnowflake()
    port.execute_results = [failed("Warehouse 'W' cannot be resumed")]
    diagnostic = only(apply_plan(changeset(change(rendered())), port), "SST-APL001")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "semantic_view:v: create failed: Warehouse 'W' cannot be resumed"
    assert diagnostic.context["artifact"] == "semantic_view:v"


def test_sst_apl001_silent() -> None:
    assert "SST-APL001" not in codes(apply_plan(changeset(change(rendered()))))
