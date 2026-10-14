from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.dbt.manifest import load_manifest_catalog
from snowflake_semantic_tools.adapters.project_source import YamlProjectSource
from snowflake_semantic_tools.app.compile.tools import CompileTools, ToolPlacement
from tests.helpers.projects import project_paths

ROOT = Path(__file__).parents[2]
FIXTURE = ROOT / "tests" / "fixtures" / "reference_project"
GOLDEN = ROOT / "tests" / "golden" / "expected" / "tool" / "menu_docs_search.sql"


def test_reference_search_service_matches_golden() -> None:
    manifest_path = ROOT / "tests" / "fixtures" / "reference_project_manifest.json"
    catalog = YamlProjectSource(
        project_paths(FIXTURE),
        target_name="dev",
        manifest_path=manifest_path,
        invoke_dbt=False,
    ).load_tools()
    dbt = load_manifest_catalog(manifest_path)
    result = CompileTools(
        catalog,
        default=ToolPlacement(
            "SST_REF_DEV", "JAFFLE", "SST_REF_WH", "1 hour", "snowflake-arctic-embed-m-v1.5", "caller"
        ),
        dbt_relations={
            model.name: ("SST_REF_DEV.JAFFLE.PRODUCT_DOCS" if model.name == "product_docs" else model.relation_name)
            for model in dbt.models
        },
    ).run_result()
    assert not result.diagnostics.has_errors
    assert len(result.compiled) == 1
    assert result.compiled[0].rendered_artifact.ddl == GOLDEN.read_text(encoding="utf-8").rstrip() + "\n"
