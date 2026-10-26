"""Reads the app layer makes of manifests, state, and samples, at the edges the broader suites skip."""

from __future__ import annotations

import dataclasses
from dataclasses import replace
from types import MappingProxyType

from snowflake_semantic_tools.app.compile import CompiledView, CompileResult
from snowflake_semantic_tools.app.listing import list_artifacts
from snowflake_semantic_tools.app.manifest import manifest_for, stale_manifest
from snowflake_semantic_tools.app.plan import PreparePlan
from snowflake_semantic_tools.app.plan_artifacts import adopted_names
from snowflake_semantic_tools.app.plan_values import PlanRefused, PlanScope
from snowflake_semantic_tools.app.prior import prior_definition
from snowflake_semantic_tools.app.sample_casts import SampleColumn, cast_findings, cast_reads, published_columns
from snowflake_semantic_tools.app.state import read_state
from snowflake_semantic_tools.app.verify_schema import verify_columns
from snowflake_semantic_tools.domain.model.identifier import Identifier
from snowflake_semantic_tools.domain.model.lifecycle import QueryResult, ShowRow
from snowflake_semantic_tools.domain.model.semantic_view import Column, ColumnKind, SemanticView, Table
from snowflake_semantic_tools.domain.state import AppliedEntry, State
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.artifact_builders import empty_state, rendered, target
from tests.helpers.clocks import FixedClock
from tests.helpers.compile_builders import compiled
from tests.helpers.project_inputs import EMPTY_SOURCES, InMemoryProjectInputs
from tests.helpers.snowflake_fake import FakeSnowflake
from tests.helpers.sql_values import authored, statement

EVERYTHING = PlanScope((), None, None, None, None, False)


def _entry(qualified_name: str, outcome: str) -> AppliedEntry:
    return AppliedEntry("a" * 64, qualified_name, "now", "run", outcome, "a" * 64, "m")


def test_a_manifest_whose_id_differs_with_the_same_content_is_stale_without_a_named_section() -> None:
    manifest = manifest_for(compiled(), EMPTY_SOURCES)
    relabeled = replace(manifest, manifest_id="0" * 64)
    stale = stale_manifest(relabeled, manifest, before="plan")
    assert stale is not None and stale.code == "SST-MAN009"
    assert stale.context["reason"] == "the project compiles to another manifest now"


def test_an_artifact_state_records_as_adopted_lists_as_adopted() -> None:
    manifest = manifest_for(compiled("SALES"), EMPTY_SOURCES)
    [(key, entry)] = manifest.artifacts.items()
    state = empty_state({key: _entry(entry.publish_target, "adopted")})
    assert [item.status for item in list_artifacts(manifest, state)] == ["adopted"]


def test_offline_a_cache_for_another_schema_is_set_aside_and_the_state_is_empty() -> None:
    elsewhere = dataclasses.replace(target(), schema=Identifier.parse("elsewhere"))
    store = InMemoryStateStore(State.empty(elsewhere))
    state, diagnostics = read_state(store, None, state_table=rendered().target, target=target())
    assert state.applied == {} and state.target == target()
    assert [item.code for item in diagnostics] == ["SST-MAN025"]


def test_adopted_names_folds_each_adopted_entry_and_skips_an_unparseable_name() -> None:
    state = empty_state(
        {
            "semantic_view:a": _entry("DB.S.A", "adopted"),
            "semantic_view:b": _entry("not a name", "adopted"),
            "semantic_view:c": _entry("DB.S.C", "applied"),
        }
    )
    assert adopted_names(state) == frozenset({("DB", "S", "A")})


def test_an_object_type_with_no_restorable_form_keeps_its_ddl_as_printed() -> None:
    port = FakeSnowflake(definitions={"DB.SCHEMA.V": "create or replace streamlit V"})
    row = ShowRow("V", "DB", "SCHEMA", "OWNER", "now", object_type="STREAMLIT")
    assert prior_definition(port, "STREAMLIT", row, rendered()) == ("create or replace streamlit V", ())


def _sample(**fields: object) -> SampleColumn:
    values: dict[str, object] = {
        "fqn": "DB.S.ORDERS",
        "model": "orders",
        "column": "amount",
        "declared": "NUMBER",
        "observed": None,
        "values": ("1",),
        "views": ("sales",),
    }
    values.update(fields)
    return SampleColumn(**values)  # type: ignore[arg-type]


def test_a_sample_column_whose_types_do_not_parse_has_no_cast_type() -> None:
    assert _sample(declared="NOT A TYPE", observed=None).cast_type is None
    assert _sample(declared=None, observed="NUMBER(38,0) ").cast_type == "NUMBER(38,0)"


def test_a_relation_whose_only_values_cannot_be_literals_reads_nothing() -> None:
    assert cast_reads([_sample(values=("bad\x00value",))]) == {}
    assert "DB.S.ORDERS" in cast_reads([_sample(values=("bad\x00value", "2"))])


def test_cast_findings_skip_rows_whose_index_is_unreadable_or_out_of_range() -> None:
    column = _sample(values=("x", "y"))
    rows = (("not a number", "x"), (5, "x"), (-1, "x"), ("only",), (0, "y"))
    found = cast_findings([column], "DB.S.ORDERS", QueryResult(("K", "V"), rows))
    assert [(item.code, item.context["value"]) for item in found] == [("SST-VAL419", "y")]


def _column(table: str, name: str, expr: str, kind: ColumnKind = ColumnKind.DIMENSION) -> Column:
    return Column(table, name, kind, authored(expr), data_type="NUMBER")


def _compiled(*columns: Column) -> CompileResult:
    view = SemanticView("DB.S.SALES", (Table("ORDERS", "DB.S.ORDERS"),), columns=columns)
    return CompileResult((CompiledView(view, statement("CREATE SEMANTIC VIEW DB.S.SALES")),))


def test_a_view_that_reads_one_column_twice_lists_it_once_under_that_view() -> None:
    result = _compiled(
        _column("ORDERS", "AMOUNT", "ORDERS.AMOUNT", ColumnKind.FACT),
        _column("ORDERS", "AMOUNT_LABEL", "ORDERS.AMOUNT"),
    )
    [column] = published_columns([item for item in result.compiled if isinstance(item, CompiledView)], {})
    assert (column.column, column.views) == ("amount", ("sales",))


def test_verify_schema_ignores_a_reference_to_a_table_the_view_does_not_hold() -> None:
    port = FakeSnowflake()
    port.tables = MappingProxyType({"DB.S.ORDERS": (("AMOUNT", "NUMBER"),)})  # type: ignore[assignment]
    result = _compiled(_column("ORDERS", "AMOUNT", "ORDERS.AMOUNT"), _column("ORDERS", "OTHER", "ELSEWHERE.X"))
    assert verify_columns(port, result) == ()


def test_a_compile_made_for_another_target_refuses_the_plan() -> None:
    result = compiled()
    other_target = manifest_for(result, replace(EMPTY_SOURCES, target_name="prod"))
    inputs = InMemoryProjectInputs(sources=replace(EMPTY_SOURCES, target_name="dev"))
    refused = PreparePlan(inputs, FixedClock()).select(
        result, other_target, EVERYTHING, partial=False, strict=None, connected=False, project="the-project"
    )
    assert isinstance(refused, PlanRefused)
    assert [(item.code, item.context["found"], item.context["expected"]) for item in refused.diagnostics] == [
        ("SST-MAN006", "prod", "dev")
    ]
