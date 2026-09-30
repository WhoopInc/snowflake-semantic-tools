"""Deterministic manifest projection for compiled semantic views."""

from __future__ import annotations

from snowflake_semantic_tools.app.compile import CompiledView, CompileResult
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.domain.model.semantic_view import Metric, SemanticView, Table


def test_manifest_uses_compiled_fingerprint_and_member_keys() -> None:
    view = SemanticView(
        fqn="DB.SCH.SALES",
        tables=(Table(logical_name="ORDERS", fqn="DB.SCH.ORDERS"),),
        metrics=(Metric(name="ORDER_COUNT", expr="COUNT(1)", table="ORDERS"),),
    )
    compiled = CompiledView(view, "CREATE OR REPLACE SEMANTIC VIEW DB.SCH.SALES\n  COPY GRANTS")
    document = build_manifest(CompileResult((compiled,))).as_dict()
    artifact = document["artifacts"]["semantic_view:sales"]
    assert artifact["fingerprint"] == compiled.fingerprint
    assert artifact["render"] == {
        "dialect": "ddl",
        "byte_length": compiled.byte_length,
        "sha256": compiled.fingerprint,
    }
    assert artifact["member_keys"] == ["metric:order_count"]
    assert document["diagnostics_summary"] == {"error": 0, "warning": 0, "info": 0}
