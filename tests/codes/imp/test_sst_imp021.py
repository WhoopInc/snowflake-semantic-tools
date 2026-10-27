"""SST-IMP021: `sst state adopt` recorded an object SST did not publish."""

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


def test_sst_imp021_fires() -> None:
    port = FakeSnowflake()
    adopter = _adopter(port, "Sales.")
    planned = changeset(unmanaged("V", None))
    artifacts, _ = adopter.compare(planned, _COMPILED)
    result = adopter.record(artifacts, planned, "verify")
    [diagnostic] = [item for item in result.diagnostics if item.code == "SST-IMP021"]
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == (
        "semantic_view:v: recorded DB.SCHEMA.V in state as adopted; the next apply stamps SST's ownership marker"
    )
    entry = (port.remote_state or {})["semantic_view:v"]
    assert entry.outcome == "adopted" and result.wrote == ("semantic_view:v",)


def test_sst_imp021_silent() -> None:
    port = FakeSnowflake()
    adopter = _adopter(port, "Old.")
    planned = changeset(unmanaged("V", None))
    artifacts, _ = adopter.compare(planned, _COMPILED)
    result = adopter.record(artifacts, planned, "verify")
    assert [item.code for item in result.diagnostics] == [] and not port.remote_state
