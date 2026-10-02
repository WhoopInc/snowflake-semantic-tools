from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

from snowflake_semantic_tools.app.compile import CompiledView, CompileResult
from snowflake_semantic_tools.app.listing import list_artifacts
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.app.smoke import RunSmokeSuite
from snowflake_semantic_tools.app.validate import ValidateArtifacts
from snowflake_semantic_tools.domain.diagnostics import D, DiagnosticBag
from snowflake_semantic_tools.domain.model.lifecycle import ProbeKind, RenderedArtifact, SmokeProbe
from snowflake_semantic_tools.domain.model.semantic_view import (
    Column,
    ColumnKind,
    Metric,
    SemanticView,
    Table,
    Variable,
    VerifiedQuery,
)
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.sql import datatype
from snowflake_semantic_tools.domain.state import STATE_SCHEMA_VERSION, AppliedEntry, State
from tests.helpers.app_ports import InMemorySnowflake
from tests.helpers.artifact_builders import rendered, target
from tests.helpers.sql_values import authored, authored_query, statement


def compile_result(diagnostics: DiagnosticBag = DiagnosticBag()) -> CompileResult:
    view = SemanticView("DB.S.V", (Table("T", "DB.S.T"),), metrics=(Metric("M", authored("COUNT(1)"), "T"),))
    return CompileResult(
        (CompiledView(view, statement("CREATE OR REPLACE SEMANTIC VIEW DB.S.V COPY GRANTS")),), diagnostics
    )


def test_validation_promotes_warnings_and_reports_connected_skip_or_failure() -> None:
    warning = DiagnosticBag((D("SST-LOD003", file="empty.yml"),))
    promoted = ValidateArtifacts().run(compile_result(warning), strict=True, connected=False)
    assert not promoted.success and promoted.promoted == 1
    # One skip notice per connected rule: the compile check, then the two spot checks.
    assert [(item.code, item.context.get("rule_id")) for item in promoted.diagnostics] == [
        ("SST-LOD003", None),
        ("SST-VAL020", "SST-VAL418"),
        ("SST-VAL020", "SST-VAL415"),
        ("SST-VAL020", "SST-VAL212"),
        ("SST-VAL020", "SST-VAL218"),
    ]
    skipped = ValidateArtifacts().run(compile_result(), strict=False, connected=True)
    assert skipped.success and skipped.diagnostics[0].code == "SST-VAL020"
    port = InMemorySnowflake()
    port.query_error = SnowflakePortError("bad yaml")
    invalid_view = SemanticView(
        "DB.S.V",
        (Table("T", "DB.S.T"),),
        metrics=(Metric("M", authored("SUM(T.VALUE)"), "T"),),
    )
    invalid_result = CompileResult(
        (CompiledView(invalid_view, statement("CREATE OR REPLACE SEMANTIC VIEW DB.S.V COPY GRANTS")),)
    )
    failed = ValidateArtifacts(port).run(invalid_result, strict=False, connected=True)
    assert not failed.success and failed.diagnostics[0].code == "SST-VAL418"
    healthy = ValidateArtifacts(InMemorySnowflake()).run(compile_result(), strict=False, connected=True)
    assert healthy.success


def test_connected_validation_compiles_expressions_and_verified_queries() -> None:
    view = SemanticView(
        "DB.S.V",
        (Table("T", "DB.S.T"),),
        metrics=(Metric("M", authored("SUM(T.VALUE)"), "T"),),
        verified_queries=(VerifiedQuery("Q", "q?", authored_query("SELECT 1;")),),
    )
    result = CompileResult((CompiledView(view, statement("CREATE OR REPLACE SEMANTIC VIEW DB.S.V COPY GRANTS")),))
    port = InMemorySnowflake()
    validated = ValidateArtifacts(port).run(result, strict=False, connected=True)
    assert validated.success
    assert [query for query, _params in port.queries] == [
        "EXPLAIN SELECT SUM(NULL) FROM (SELECT 1 AS SST_VALUE WHERE FALSE) AS SST_VALIDATE",
        "EXPLAIN SELECT 1",
        "SELECT COUNT(*) AS ROW_COUNT FROM (SELECT 1) AS SST_VQ",
    ]


def test_connected_validation_skips_non_views_and_projects_columns() -> None:
    view = SemanticView(
        "DB.S.V",
        (Table("T", "DB.S.T"),),
        variables=(Variable("THRESHOLD", datatype("NUMBER"), statement("1")),),
        columns=(
            Column("T", "CATEGORY", ColumnKind.DIMENSION, authored("T.CATEGORY")),
            Column("T", "ACTIVE", ColumnKind.FILTER, authored("T.ACTIVE = TRUE")),
        ),
        metrics=(Metric("TOTAL", authored("SUM(T.VALUE) + THRESHOLD + TOTAL"), "T"),),
    )
    result = CompileResult(
        (
            _NonViewArtifact(),
            CompiledView(view, statement("CREATE OR REPLACE SEMANTIC VIEW DB.S.V COPY GRANTS")),
        )
    )
    port = InMemorySnowflake()
    validated = ValidateArtifacts(port).run(result, strict=False, connected=True)
    assert validated.success
    assert [query for query, _params in port.queries] == [
        "EXPLAIN SELECT SUM(NULL) + NULL + NULL "
        "FROM (SELECT NULL AS ACTIVE, NULL AS CATEGORY WHERE FALSE) AS SST_VALIDATE",
        "EXPLAIN SELECT NULL FROM (SELECT NULL AS ACTIVE, NULL AS CATEGORY WHERE FALSE) AS SST_VALIDATE",
        "EXPLAIN SELECT NULL = TRUE FROM (SELECT NULL AS ACTIVE, NULL AS CATEGORY WHERE FALSE) AS SST_VALIDATE",
    ]


def test_smoke_suite_runs_each_probe_and_supports_fail_fast() -> None:
    artifact = RenderedArtifact.create(
        key="semantic_view:v",
        artifact_type="semantic_view",
        target=rendered().target,
        ddl=statement("ddl"),
        smoke=(
            SmokeProbe("view", ProbeKind.VIEW, statement("select 1")),
            SmokeProbe("metric", ProbeKind.METRIC, statement("select 2")),
        ),
    )
    good_port = InMemorySnowflake()
    good = RunSmokeSuite(good_port).run((artifact,))
    assert good.success and len(good.attempted) == 2

    bad_port = InMemorySnowflake()
    bad_port.query_error = SnowflakePortError("broken")
    bad = RunSmokeSuite(bad_port).run((artifact,), fail_fast=True)
    assert not bad.success and len(bad.attempted) == 1
    assert [item.code for item in bad.diagnostics] == ["SST-APL100", "SST-APL006"]

    all_failures = RunSmokeSuite(bad_port).run((artifact,), fail_fast=False)
    assert len(all_failures.attempted) == 2
    assert [item.code for item in all_failures.diagnostics] == ["SST-APL100", "SST-PLN100", "SST-APL006"]


def test_listing_projects_pending_and_applied_artifacts() -> None:
    manifest = build_manifest(compile_result())
    pending = list_artifacts(manifest)
    assert pending[0].status == "pending"
    entry = AppliedEntry(
        pending[0].fingerprint,
        pending[0].target,
        "now",
        "run",
        "applied",
        pending[0].fingerprint,
        manifest.manifest_id,
    )
    state = State(
        STATE_SCHEMA_VERSION,
        target(),
        manifest.manifest_id,
        "cfg",
        None,
        MappingProxyType({pending[0].key: entry}),
    )
    applied = list_artifacts(manifest, state)
    assert applied[0].status == "applied"

    # A publish that failed after writing, and a tombstone SST's own prune left,
    # are neither "applied" nor merely "pending".
    for outcome, status in (("failed_after_write", "failed"), ("deactivated", "deactivated")):
        recorded = replace(entry, outcome=outcome)
        listed = list_artifacts(manifest, replace(state, applied=MappingProxyType({pending[0].key: recorded})))
        assert listed[0].status == status
    changed = replace(entry, fingerprint="0" * 64)
    assert list_artifacts(manifest, replace(state, applied=MappingProxyType({pending[0].key: changed})))[0].status == (
        "pending"
    )


class _NonViewArtifact:
    name = "OTHER"
    artifact_key = "tool:other"
    artifact_type = "tool"
    source_files: tuple[str, ...] = ()
    member_keys: tuple[str, ...] = ()
    referenced_models: tuple[str, ...] = ()
    dbt_relations: tuple[tuple[str, str], ...] = ()

    @property
    def rendered_artifact(self) -> RenderedArtifact:
        return rendered("OTHER")

    def rendered_for_publish(self, manifest_id: str) -> RenderedArtifact:
        del manifest_id
        return self.rendered_artifact
