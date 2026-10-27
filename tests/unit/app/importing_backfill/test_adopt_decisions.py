"""`sst state adopt`'s decisions and records: what it cannot compare, and a write the lock refuses."""

from __future__ import annotations

from dataclasses import replace

import pytest

from snowflake_semantic_tools.app.adopt import AdoptedArtifact, AdoptObjects
from snowflake_semantic_tools.app.compile import CompiledView, CompileResult
from snowflake_semantic_tools.app.plan_diff import Comparison
from snowflake_semantic_tools.domain.model.lifecycle import ChangeReason
from snowflake_semantic_tools.domain.model.lifecycle.member import Definition, Member
from snowflake_semantic_tools.domain.model.semantic_view import Column, ColumnKind, SemanticView, Table
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
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


def _comment(text: str) -> Definition:
    return Definition((Member("view", "comment", (("text", text),)),))


def _matching() -> Comparison:
    return Comparison(lambda _compiled: _comment("Sales."), lambda _port, _compiled, _live: _comment("Sales."))


def _adopter(
    port: FakeSnowflake, comparison: Comparison | None = None, store: InMemoryStateStore | None = None
) -> AdoptObjects:
    return AdoptObjects(
        port,
        store if store is not None else InMemoryStateStore(empty_state()),
        FixedClock(),
        state_table=TABLE,
        comparisons={"semantic_view": comparison or _matching()},
    )


def _reason(port: FakeSnowflake, change: object, comparison: Comparison | None = None) -> str:
    artifacts, [diagnostic] = _adopter(port, comparison).compare(changeset(change), _COMPILED)  # type: ignore[arg-type]
    assert diagnostic.code == "SST-IMP022"
    assert [item.recorded for item in artifacts] == [False]
    return str(diagnostic.context["reason"])


# -- what adopt cannot compare ------------------------------------------------------------------------


def test_a_type_with_no_single_object_is_not_adopted() -> None:
    change = replace(unmanaged("V", None), artifact_type="skill")
    assert _reason(FakeSnowflake(), change).startswith("adopt records semantic_view, tool, agent, cowork_profile")


def test_an_artifact_that_does_not_compile_for_the_target_is_not_adopted() -> None:
    change = replace(unmanaged("V", None), rendered=None)
    assert _reason(FakeSnowflake(), change) == "it does not compile for this target"


def test_an_artifact_with_no_live_object_is_left_for_apply_to_create() -> None:
    change = replace(unmanaged("V", None), observed=None)
    assert _reason(FakeSnowflake(), change) == "no object holds its name on the target; `sst apply` creates it"


def test_an_artifact_state_records_already_is_not_adopted() -> None:
    change = replace(unmanaged("V", None), reason=ChangeReason.FINGERPRINT_DIFFERS)
    assert _reason(FakeSnowflake(), change) == "state records it already"


def test_an_artifact_that_did_not_compile_has_nothing_to_compare() -> None:
    artifacts, [diagnostic] = _adopter(FakeSnowflake()).compare(changeset(unmanaged("V", None)), CompileResult(()))
    assert diagnostic.context["reason"] == "it did not compile, so there is nothing to compare the live object with"
    assert artifacts[0].found == diagnostic.context["reason"]


def test_an_artifact_its_comparison_does_not_describe_is_not_adopted() -> None:
    comparison = Comparison(lambda _compiled: None, lambda _port, _compiled, _live: _comment("x"))
    reason = _reason(FakeSnowflake(), unmanaged("V", None), comparison)
    assert reason == "SST does not compare this kind of object member by member"


def test_a_live_definition_that_cannot_be_read_is_not_adopted() -> None:
    def refused(_port: object, _compiled: object, _live: object) -> Definition:
        raise SnowflakePortError("DESCRIBE refused")

    reason = _reason(FakeSnowflake(), unmanaged("V", None), Comparison(lambda _compiled: _comment("x"), refused))
    assert reason == "its live definition could not be read: DESCRIBE refused"


# -- differences --------------------------------------------------------------------------------------


def test_a_member_only_one_side_holds_is_named_without_properties_and_long_lists_are_counted() -> None:
    desired = Definition(tuple(Member("metric", f"m{index}", (("expr", "1"),)) for index in range(8)))
    comparison = Comparison(lambda _compiled: desired, lambda _port, _compiled, _live: Definition(()))
    artifacts, [diagnostic] = _adopter(FakeSnowflake(), comparison).compare(changeset(unmanaged("V", None)), _COMPILED)
    assert diagnostic.code == "SST-IMP020"
    assert len(artifacts[0].differences) == 8
    assert all("(" not in item for item in artifacts[0].differences)
    assert str(diagnostic.context["detail"]).endswith(", and 2 more")


def test_a_long_or_empty_property_value_is_shortened_or_shown_unset() -> None:
    comparison = Comparison(lambda _compiled: _comment("x" * 80), lambda _port, _compiled, _live: _comment(""))
    artifacts, _ = _adopter(FakeSnowflake(), comparison).compare(changeset(unmanaged("V", None)), _COMPILED)
    [difference] = artifacts[0].differences
    assert f"text: unset -> {'x' * 57}..." in difference


# -- recording ----------------------------------------------------------------------------------------


def test_nothing_let_through_records_nothing_and_takes_no_lock() -> None:
    port = FakeSnowflake()
    planned = changeset(unmanaged("V", None))
    result = _adopter(port).record((AdoptedArtifact("semantic_view:v", "DB.SCHEMA.V", "differs"),), planned, "verify")
    assert result.pending == () and result.wrote == () and not result.refused
    assert port.run_locks.claims == []


def test_another_runs_lock_refuses_the_record() -> None:
    port = FakeSnowflake()
    store = InMemoryStateStore(empty_state())
    store.locked, store.holder = True, "other-run"
    adopter = _adopter(port, store=store)
    planned = changeset(unmanaged("V", None))
    artifacts, _ = adopter.compare(planned, _COMPILED)
    result = adopter.record(artifacts, planned, "verify")
    assert result.refused and result.wrote == ()
    assert [item.code for item in result.diagnostics] == ["SST-APL011"]
    assert [item.key for item in result.pending] == ["semantic_view:v"]


def test_a_write_the_fence_refuses_records_nothing() -> None:
    class LostFence(FakeSnowflake):
        def write_state(self, *args: object, **kwargs: object) -> bool:
            return False

    port = LostFence()
    adopter = _adopter(port)
    planned = changeset(unmanaged("V", None))
    artifacts, _ = adopter.compare(planned, _COMPILED)
    result = adopter.record(artifacts, planned, "verify")
    assert result.refused and result.wrote == () and not port.remote_state


def test_an_object_state_records_meanwhile_is_not_written_again() -> None:
    port = FakeSnowflake()
    adopter = _adopter(port)
    planned = changeset(unmanaged("V", None))
    artifacts, _ = adopter.compare(planned, _COMPILED)
    adopter.record(artifacts, planned, "verify")
    again = adopter.record(artifacts, planned, "verify")
    assert again.wrote == () and not again.refused
    assert [item.recorded for item in again.artifacts] == [False]
    assert "SST-IMP021" not in [item.code for item in again.diagnostics]


def test_a_record_with_no_local_state_writes_only_the_remote_entry() -> None:
    port = FakeSnowflake()
    store = InMemoryStateStore(None)
    adopter = _adopter(port, store=store)
    planned = changeset(unmanaged("V", None))
    artifacts, _ = adopter.compare(planned, _COMPILED)
    result = adopter.record(artifacts, planned, "verify")
    assert result.wrote == ("semantic_view:v",) and store.writes == []
    assert (port.remote_state or {})["semantic_view:v"].outcome == "adopted"


def test_recording_an_artifact_with_no_live_object_is_refused_as_a_programming_error() -> None:
    planned = changeset(replace(unmanaged("V", None), observed=None))
    artifact = AdoptedArtifact("semantic_view:v", "DB.SCHEMA.V", "matches", recorded=True)
    with pytest.raises(ValueError, match="semantic_view:v names no object to adopt"):
        _adopter(FakeSnowflake()).record((artifact,), planned, "verify")
