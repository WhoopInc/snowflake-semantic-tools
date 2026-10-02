from __future__ import annotations

import pytest

from snowflake_semantic_tools.app.compile import CompileArtifacts, CompiledView, CompileResult
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.domain.model.diagnostic import D, DiagnosticBag
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import CompositeFacts, PublishShape, RenderedArtifact
from snowflake_semantic_tools.domain.model.semantic_view import (
    Column,
    ColumnKind,
    Metric,
    SemanticView,
    Table,
    VerifiedQuery,
)
from tests.helpers.sql_values import authored, authored_query, statement


def test_compiled_view_emits_all_smoke_probe_kinds_and_required_relations() -> None:
    view = SemanticView(
        "DB.S.V",
        (Table("T", "DB.S.T"),),
        metrics=(
            Metric("PUBLIC", authored("COUNT(1)"), "T"),
            Metric("PRIVATE", authored("COUNT(1)"), "T", access_modifier="private_access"),
        ),
        verified_queries=(VerifiedQuery("Q", "q?", authored_query("SELECT 1;")),),
    )
    compiled = CompiledView(view, statement("CREATE OR REPLACE SEMANTIC VIEW DB.S.V COPY GRANTS"))
    rendered = compiled.rendered_artifact
    assert [probe.kind.value for probe in rendered.smoke] == ["view", "metric", "verified_query"]
    assert str(rendered.smoke[0].sql) == "SELECT SV.PUBLIC FROM SEMANTIC_VIEW(DB.S.V METRICS T.PUBLIC) AS SV LIMIT 0"
    assert "SELECT SV.PUBLIC" in str(rendered.smoke[1].sql)
    assert rendered.required_relations[0].sql == "DB.S.T"
    assert "LIMIT 0" in str(rendered.smoke[-1].sql)
    assert CompileResult((compiled,)).rendered == (rendered,)
    publish = compiled.rendered_for_publish("a" * 64)
    assert f"[sst:{'a' * 64}:{compiled.fingerprint}]" in publish.ddl
    assert publish.fingerprint == compiled.fingerprint
    assert CompileResult((compiled,)).rendered_for_publish("a" * 64) == (publish,)


def test_compiled_view_uses_dimension_probe_and_rejects_view_without_public_members() -> None:
    view = SemanticView(
        "DB.S.V",
        (Table("T", "DB.S.T"),),
        columns=(Column("T", "CATEGORY", ColumnKind.DIMENSION, authored("T.CATEGORY")),),
    )
    compiled = CompiledView(view, statement("CREATE OR REPLACE SEMANTIC VIEW DB.S.V COPY GRANTS"))
    assert compiled.byte_length == len(compiled.canonical_ddl.encode("utf-8"))
    assert str(compiled.rendered_artifact.smoke[0].sql) == (
        "SELECT SV.CATEGORY FROM SEMANTIC_VIEW(DB.S.V DIMENSIONS T.CATEGORY) AS SV LIMIT 0"
    )

    private_only = SemanticView(
        "DB.S.PRIVATE_V",
        (Table("T", "DB.S.T"),),
        metrics=(Metric("PRIVATE", authored("COUNT(1)"), "T", access_modifier="private_access"),),
    )
    with pytest.raises(ValueError, match="no public dimension or metric"):
        _ = CompiledView(private_only, statement("CREATE SEMANTIC VIEW DB.S.PRIVATE_V")).rendered_artifact


def test_manifest_builder_records_sources_members_impact_and_diagnostics() -> None:
    view = SemanticView(
        "DB.S.V",
        (Table("T", "DB.S.T"),),
        metrics=(Metric("M", authored("COUNT(1)"), "T"),),
        source_path="semantic_models/views.yml",
        source_files=("semantic_models/views.yml", "semantic_models/metrics.yml"),
        referenced_models=("t",),
    )
    compiled = CompiledView(view, statement("CREATE OR REPLACE SEMANTIC VIEW DB.S.V COPY GRANTS"))
    result = CompileResult(
        (compiled,),
        DiagnosticBag((D("SST-LOD003", file="empty.yml", subject="semantic_view:v"),)),
    )
    manifest = build_manifest(
        result,
        dbt_project_name="jaffle",
        file_checksums={"semantic_models/views.yml": "abc"},
    )
    entry = manifest.artifacts["semantic_view:v"]
    assert entry.source_files == ("semantic_models/views.yml", "semantic_models/metrics.yml")
    assert "metric:m" in entry.member_keys
    assert entry.diagnostic_codes == ("SST-LOD003",)
    assert manifest.impact.by_member["metric:m"] == ("semantic_view:v",)
    assert manifest.impact.by_file["semantic_models/views.yml"] == ("semantic_view:v",)
    assert manifest.impact.by_file["semantic_models/metrics.yml"] == ("semantic_view:v",)
    assert manifest.impact.by_dbt_model["t"] == ("semantic_view:v",)
    assert manifest.dbt_models["t"]["relation"] == "DB.S.T"  # type: ignore[index]
    assert manifest.sources["semantic_file_count"] == 1
    assert build_manifest(result).as_dict()["manifest_id"] == build_manifest(result).manifest_id


class _CompiledTool:
    def __init__(self, name: str = "SEARCH", artifact_key: str = "tool:search") -> None:
        self.name = name
        self.artifact_key = artifact_key

    artifact_type = "tool"
    source_files = ("tools/platform.yml",)
    member_keys: tuple[str, ...] = ()
    referenced_models = ("docs",)
    dbt_relations = (("docs", "DB.S.DOCS"),)

    @property
    def rendered_artifact(self) -> RenderedArtifact:
        return RenderedArtifact.create(
            key=self.artifact_key,
            artifact_type=self.artifact_type,
            target=QualifiedName.parse("DB.S.SEARCH"),
            ddl=statement("CREATE CORTEX SEARCH SERVICE DB.S.SEARCH ON BODY AS SELECT BODY FROM DB.S.DOCS"),
            shape=PublishShape("CORTEX SEARCH SERVICE"),
            depends_on=("semantic_view:v",),
        )

    def rendered_for_publish(self, manifest_id: str) -> RenderedArtifact:
        del manifest_id
        return self.rendered_artifact


class _Compiler:
    def __init__(self, result: CompileResult) -> None:
        self._result = result

    def run_result(self) -> CompileResult:
        return self._result


def test_generic_compiler_and_manifest_preserve_artifact_metadata() -> None:
    view = SemanticView("DB.S.V", (Table("T", "DB.S.T"),), metrics=(Metric("M", authored("COUNT(1)"), "T"),))
    compiled_view = CompiledView(view, statement("CREATE OR REPLACE SEMANTIC VIEW DB.S.V COPY GRANTS"))
    tool = _CompiledTool()
    result = CompileArtifacts(
        (_Compiler(CompileResult((tool,))), _Compiler(CompileResult((compiled_view,)))),
        {"semantic_view": 100, "tool": 200},
    ).run_result()
    assert [item.artifact_key for item in result.compiled] == ["semantic_view:v", "tool:search"]
    manifest = build_manifest(result)
    entry = manifest.artifacts["tool:search"]
    assert entry.object_type == "CORTEX SEARCH SERVICE"
    assert entry.depends_on == ("semantic_view:v",)
    assert manifest.impact.by_file["tools/platform.yml"] == ("tool:search",)
    assert manifest.impact.by_dbt_model["docs"] == ("tool:search",)


def test_manifest_round_trip_preserves_composite_component_metadata() -> None:
    rendered = RenderedArtifact.create(
        key="eval:a",
        artifact_type="eval",
        target=QualifiedName.parse("DB.S.EVAL_A"),
        ddl=statement("evaluation:\n  agent_params: {}\nmetrics: []"),
        shape=PublishShape("", render_dialect="eval_yaml"),
        composite=CompositeFacts(
            component_fingerprints=(("dataset", "a" * 64), ("config", "b" * 64)),
            physical_resources=(
                ("TABLE", QualifiedName.parse("DB.S.EVAL_SRC_A")),
                ("DATASET", QualifiedName.parse("DB.S.EVAL_A")),
            ),
        ),
    )
    manifest = build_manifest(CompileResult((_CompiledComposite(rendered),)))
    entry = manifest.artifacts["eval:a"]
    assert entry.component_fingerprints == (("dataset", "a" * 64), ("config", "b" * 64))
    assert entry.physical_resources == (("TABLE", "DB.S.EVAL_SRC_A"), ("DATASET", "DB.S.EVAL_A"))


class _CompiledComposite:
    name = "A"
    artifact_key = "eval:a"
    artifact_type = "eval"
    source_files = ("dataset.yml", "config.yml")
    member_keys: tuple[str, ...] = ()
    referenced_models: tuple[str, ...] = ()
    dbt_relations: tuple[tuple[str, str], ...] = ()

    def __init__(self, rendered: RenderedArtifact) -> None:
        self._rendered = rendered

    @property
    def rendered_artifact(self) -> RenderedArtifact:
        return self._rendered

    def rendered_for_publish(self, manifest_id: str) -> RenderedArtifact:
        del manifest_id
        return self._rendered


def test_manifest_merges_references_to_the_same_dbt_model() -> None:
    first = _CompiledTool()
    second = _CompiledTool("SEARCH_TWO", "tool:search_two")
    manifest = build_manifest(CompileResult((first, second)))
    assert manifest.dbt_models["docs"]["referenced_by"] == ["tool:search", "tool:search_two"]  # type: ignore[index]
