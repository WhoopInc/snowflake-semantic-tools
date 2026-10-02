"""SST-DBT006: a literal relation names a model by its name, where an `alias:` built it elsewhere."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.dbt.manifest import catalog_from_document
from snowflake_semantic_tools.adapters.yaml.tools import load_tool_catalog
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.seam_projects import manifest, model_node

CATALOG = catalog_from_document(
    manifest({"model.fixture.pricing_periods": model_node("pricing_periods", "DB.SCH.pricing_calendar")})
)


def _tools(tmp_path: Path, relation: str) -> None:
    (tmp_path / "tools").mkdir()
    (tmp_path / "tools" / "partner.yml").write_text(
        "tools:\n  - group: partner\n    immutable: true\n    reference:\n"
        "      - name: prices\n        type: generic\n        description: Partner prices.\n"
        f"        relations:\n          dev: {relation}\n",
        encoding="utf-8",
    )


def test_sst_dbt006_fires(tmp_path: Path) -> None:
    _tools(tmp_path, "DB.SCH.PRICING_PERIODS")
    catalog = load_tool_catalog(tmp_path, CATALOG, target_name="dev", declared_targets=frozenset({"dev"}))
    [diagnostic] = [item for item in catalog.diagnostics if item.code == "SST-DBT006"]
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "model 'pricing_periods' resolves to relation 'DB.SCH.PRICING_CALENDAR'"
    assert diagnostic.subject == "partner:prices"


def test_sst_dbt006_silent(tmp_path: Path) -> None:
    _tools(tmp_path, "DB.SCH.PRICING_CALENDAR")
    catalog = load_tool_catalog(tmp_path, CATALOG, target_name="dev", declared_targets=frozenset({"dev"}))
    assert "SST-DBT006" not in [item.code for item in catalog.diagnostics]
