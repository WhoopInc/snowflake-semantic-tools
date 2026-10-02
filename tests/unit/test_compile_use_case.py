"""The compile use case, driven WITHOUT mocks.

This is the architecture's central claim made concrete. `CompileSemanticViews`
receives its source as a constructor argument, so these tests hand it a real class
that returns real models -- no `MagicMock`, no `patch`, no import interception. 0.3
carries roughly 570 inline mock usages because it has no seam to inject at; the
whole point of the `app/` ring is to be this boring to test.
"""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.app.compile import CompiledView, CompileResult, CompileSemanticViews
from snowflake_semantic_tools.domain.model.diagnostic import D, DiagnosticBag
from snowflake_semantic_tools.domain.model.project import SemanticViewProject
from snowflake_semantic_tools.domain.model.semantic_view import Column, ColumnKind, SemanticView, Table
from tests.helpers.compile_builders import compiled_as


class InMemorySource:
    """A real `SemanticViewSource`, satisfying the Protocol structurally."""

    def __init__(self, *views: SemanticView) -> None:
        self._views = views
        self.call_count = 0

    def load_project(self) -> SemanticViewProject:
        self.call_count += 1
        return SemanticViewProject(self._views)


def view(name: str, *, columns: tuple[Column, ...] = ()) -> SemanticView:
    return SemanticView(
        fqn=f"DB.SCH.{name}",
        tables=(Table(logical_name="T", fqn="DB.SCH.T", primary_key=("ID",)),),
        columns=columns,
    )


def test_compiles_each_view_to_ddl() -> None:
    use_case = CompileSemanticViews(InMemorySource(view("ALPHA")))
    compiled = use_case.run_result().compiled
    assert len(compiled) == 1
    assert isinstance(compiled[0], CompiledView)
    assert compiled[0].name == "ALPHA"
    assert compiled[0].ddl.startswith("CREATE OR REPLACE SEMANTIC VIEW DB.SCH.ALPHA")


def test_output_is_ordered_by_fqn_regardless_of_source_order() -> None:
    """Stable output run to run, so a diff of two compiles is meaningful."""
    source = InMemorySource(view("ZULU"), view("ALPHA"), view("MIKE"))
    assert [c.name for c in CompileSemanticViews(source).run_result().compiled] == ["ALPHA", "MIKE", "ZULU"]


def test_empty_source_compiles_to_nothing_rather_than_failing() -> None:
    assert CompileSemanticViews(InMemorySource()).run_result().compiled == ()


def test_the_source_is_consulted_once_per_run() -> None:
    source = InMemorySource(view("ALPHA"))
    use_case = CompileSemanticViews(source)
    use_case.run_result()
    use_case.run_result()
    assert source.call_count == 2, "each run should re-read, so a file edit is picked up"


def test_name_is_the_unqualified_tail_of_the_fqn() -> None:
    compiled = compiled_as(CompileSemanticViews(InMemorySource(view("ALPHA"))).run_result(), CompiledView)
    assert compiled.view.fqn == "DB.SCH.ALPHA"
    assert compiled.name == "ALPHA"


def test_model_and_ddl_are_both_retained() -> None:
    """The caller needs the name from the model and the bytes from the DDL."""
    columns = (Column(table="T", name="C", kind=ColumnKind.DIMENSION, expr="T.C"),)
    compiled = compiled_as(
        CompileSemanticViews(InMemorySource(view("ALPHA", columns=columns))).run_result(), CompiledView
    )
    assert compiled.view.dimensions == columns
    assert "T.C AS T.C" in compiled.ddl


def test_compiled_artifact_metadata_is_deterministic() -> None:
    compiled = compiled_as(CompileSemanticViews(InMemorySource(view("ALPHA"))).run_result(), CompiledView)
    assert compiled.artifact_key == "semantic_view:alpha"
    assert compiled.byte_length == len(compiled.canonical_ddl.encode("utf-8"))
    assert len(compiled.fingerprint) == 64
    assert (
        compiled.fingerprint
        == compiled_as(CompileSemanticViews(InMemorySource(view("ALPHA"))).run_result(), CompiledView).fingerprint
    )


def test_compile_result_retains_structured_diagnostics() -> None:
    result = CompileSemanticViews(InMemorySource(view("ALPHA"))).run_result()
    assert isinstance(result, CompileResult)
    assert result.success
    assert [compiled.name for compiled in result.compiled] == ["ALPHA"]
    assert result.diagnostics == ()


def test_compile_result_keeps_healthy_views_when_project_has_errors() -> None:
    class PartiallyPoisoned(InMemorySource):
        def load_project(self) -> SemanticViewProject:
            return SemanticViewProject(
                self._views,
                DiagnosticBag((D("SST-REF001", model="missing", subject="semantic_view:bad"),)),
            )

    result = CompileSemanticViews(PartiallyPoisoned(view("HEALTHY"))).run_result()
    assert [compiled.name for compiled in result.compiled] == ["HEALTHY"]
    assert not result.success
    assert [diagnostic.code for diagnostic in result.diagnostics] == ["SST-REF001"]


def test_compile_result_turns_render_invariant_into_diagnostic(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "snowflake_semantic_tools.app.compile.render", lambda value: (_ for _ in ()).throw(ValueError("bad"))
    )
    result = CompileSemanticViews(InMemorySource(view("BROKEN"))).run_result()
    assert result.compiled == ()
    assert result.diagnostics[0].code == "SST-INT902"


def test_a_source_that_raises_is_not_swallowed() -> None:
    """Translating an error into a message is cli's job, not the use case's."""

    class Failing:
        def load_project(self) -> SemanticViewProject:
            raise RuntimeError("disk on fire")

    with pytest.raises(RuntimeError, match="disk on fire"):
        CompileSemanticViews(Failing()).run_result()
