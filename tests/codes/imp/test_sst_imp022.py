"""SST-IMP022: an artifact selected for adopt is not an unmanaged object adopt can compare."""

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
from tests.helpers.recover_changes import TABLE, proved, unmanaged
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


def test_sst_imp022_fires() -> None:
    _, [diagnostic] = _adopter(FakeSnowflake(), "Sales.").compare(changeset(proved("V")), _COMPILED)
    assert (diagnostic.code, diagnostic.severity) == ("SST-IMP022", Severity.ERROR)
    assert diagnostic.message == "semantic_view:v: it carries SST's ownership marker; `sst state recover` records it"


def test_sst_imp022_silent() -> None:
    _, problems = _adopter(FakeSnowflake(), "Sales.").compare(changeset(unmanaged("V", None)), _COMPILED)
    assert problems == ()
