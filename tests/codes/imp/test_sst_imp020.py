"""SST-IMP020: `sst state adopt` finds the live object differs from the compiled artifact."""

from __future__ import annotations

from snowflake_semantic_tools.app.adopt import AdoptObjects
from snowflake_semantic_tools.app.compile import CompiledView, CompileResult
from snowflake_semantic_tools.app.plan_diff import Comparison
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle.member import Definition, Member
from snowflake_semantic_tools.domain.model.semantic_view import Column, ColumnKind, SemanticView, Table
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.artifact_builders import changeset, empty_state
from tests.helpers.clocks import FixedClock
from tests.helpers.recover_changes import TABLE, unmanaged
from tests.helpers.snowflake_fake import FakeSnowflake
from tests.helpers.sql_values import authored, statement

_VIEW = SemanticView(
    "DB.SCHEMA.V",
    (Table("T", "DB.SCHEMA.T"),),
    columns=(Column("T", "ID", ColumnKind.DIMENSION, authored("T.ID")),),
)
_COMPILED = CompileResult((CompiledView(_VIEW, statement("CREATE SEMANTIC VIEW V")),))


def _adopter(port: FakeSnowflake, live: str) -> AdoptObjects:
    comparison = Comparison(
        lambda _compiled: Definition((Member("view", "comment", (("text", "Sales."),)),)),
        lambda _port, _compiled, _live: Definition((Member("view", "comment", (("text", live),)),)),
    )
    return AdoptObjects(
        port,
        InMemoryStateStore(empty_state()),
        FixedClock(),
        state_table=TABLE,
        comparisons={"semantic_view": comparison},
    )


def test_sst_imp020_fires() -> None:
    artifacts, [diagnostic] = _adopter(FakeSnowflake(), "Old.").compare(changeset(unmanaged("V", None)), _COMPILED)
    assert (diagnostic.code, diagnostic.severity) == ("SST-IMP020", Severity.ERROR)
    assert diagnostic.message == (
        "semantic_view:v: DB.SCHEMA.V differs from what the project compiles: view comment (text: Old. -> Sales.)"
    )
    assert [item.recorded for item in artifacts] == [False]


def test_sst_imp020_silent() -> None:
    _, problems = _adopter(FakeSnowflake(), "Sales.").compare(changeset(unmanaged("V", None)), _COMPILED)
    assert problems == ()
    # Accepted, a difference is recorded rather than refused.
    artifacts, accepted = _adopter(FakeSnowflake(), "Old.").compare(
        changeset(unmanaged("V", None)), _COMPILED, accept_differences=True
    )
    assert accepted == () and [item.recorded for item in artifacts] == [True]
