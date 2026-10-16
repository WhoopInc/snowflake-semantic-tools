"""Deterministic manifest projection for compiled semantic views."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from snowflake_semantic_tools.app.compile import CompiledView, CompileResult
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.domain.model.semantic_view import Metric, SemanticView, Table
from snowflake_semantic_tools.domain.render.semantic_view import render
from tests.helpers.reference_project import VIEWS, edited, edited_manifest, load, project_copy, set_column_meta
from tests.helpers.sql_values import authored, statement


def test_manifest_uses_compiled_fingerprint_and_member_keys() -> None:
    view = SemanticView(
        fqn="DB.SCH.SALES",
        tables=(Table(logical_name="ORDERS", fqn="DB.SCH.ORDERS"),),
        metrics=(Metric(name="ORDER_COUNT", expr=authored("COUNT(1)"), table="ORDERS"),),
    )
    compiled = CompiledView(view, statement("CREATE OR REPLACE SEMANTIC VIEW DB.SCH.SALES\n  COPY GRANTS"))
    document: Any = build_manifest(CompileResult((compiled,))).as_dict()
    artifact = document["artifacts"]["semantic_view:sales"]
    assert artifact["fingerprint"] == compiled.fingerprint
    assert artifact["render"] == {
        "dialect": "ddl",
        "byte_length": compiled.byte_length,
        "sha256": compiled.fingerprint,
    }
    assert artifact["member_keys"] == ["metric:order_count"]
    assert document["diagnostics_summary"] == {"error": 0, "warning": 0, "info": 0}


# dbt reads YAML 1.1, so an unquoted `no` in a model's `meta.sst` reaches its manifest as
# `false`. The cases below show such a value is reported, never published as 'false' or 'False'.


def test_a_boolean_synonym_or_sample_value_is_reported_and_left_out(tmp_path: Path) -> None:
    def edit(document: dict[str, Any]) -> None:
        set_column_meta(document, "customers", "customer_type", synonyms=[True, "cohort"])
        set_column_meta(document, "customers", "customer_type", sample_values=["new", "returning", False])

    manifest = edited_manifest(tmp_path, edit)
    loaded = load(project_copy(tmp_path), manifest)
    subject = "dbt_column:customers.customer_type"
    assert [item.message for item in loaded.diagnostics if item.code == "SST-PRS003"] == [
        f"{subject}: 'meta.sst.synonyms[0]' expects a string, found bool",
        f"{subject}: 'meta.sst.sample_values[2]' expects a string, found bool",
    ]
    lines = [line for view in loaded.views for line in str(render(view)).splitlines() if "CUSTOMER_TYPE AS" in line]
    assert lines
    assert all("SAMPLE_VALUES ('new', 'returning')" in line and "SYNONYMS ('cohort')" in line for line in lines)
    assert not any("'false'" in line or "'true'" in line for line in lines)


def test_a_boolean_columns_boolean_sample_values_are_its_values(tmp_path: Path) -> None:
    loaded = load(project_copy(tmp_path))
    assert [item for item in loaded.diagnostics if item.code == "SST-PRS003"] == []
    assert "SAMPLE_VALUES ('true', 'false')" in "\n".join(str(render(view)) for view in loaded.views)


def test_a_boolean_tag_value_is_reported_not_published_as_false(tmp_path: Path) -> None:
    project = edited(tmp_path, VIEWS, "value: analytics", "value: no")
    loaded = load(project)
    assert [item.message for item in loaded.diagnostics if item.code == "SST-PRS003"] == [
        "semantic_view:jaffle_sales: 'config.tags.value' expects a string, found bool"
    ]
    assert all("'False'" not in str(render(view)) for view in loaded.views)
