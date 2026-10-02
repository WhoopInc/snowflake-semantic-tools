"""Shared port contract for offline Snowflake implementations."""

from __future__ import annotations

from hashlib import md5
from types import MappingProxyType

import pytest

from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import (
    ExecResult,
    ExecutionError,
    GrantRow,
    OwnershipMarker,
    QueryResult,
    ShowRow,
)
from snowflake_semantic_tools.domain.ports.snowflake.catalog import StageObservation
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.stage import StagedFileMetadata
from snowflake_semantic_tools.domain.sql import sql
from snowflake_semantic_tools.domain.state import AppliedEntry
from snowflake_semantic_tools.domain.state.lock import LockClaim, StateWrite
from tests.helpers.recorded_snowflake import ReadOnlySnowflake, RecordedSnowflake, ScriptedSnowflake
from tests.helpers.sql_values import statement, statements


def values() -> tuple[QualifiedName, SchemaScope, OwnershipMarker, ShowRow, GrantRow, AppliedEntry]:
    name = QualifiedName.from_parts("db", "sch", "v")
    scope = SchemaScope(name.database, name.schema)
    marker = OwnershipMarker("a" * 64, "b" * 64)
    row = ShowRow("V", "DB", "SCH", "OWNER", "now", marker.text)
    grant = GrantRow("SELECT", "ROLE", "READER")
    entry = AppliedEntry("b" * 64, name.sql, "now", "run", "applied", "b" * 64, "a" * 64)
    return name, scope, marker, row, grant, entry


@pytest.mark.parametrize("adapter_type", [RecordedSnowflake, ScriptedSnowflake])
def test_offline_adapters_implement_the_full_read_write_contract(adapter_type: type[RecordedSnowflake]) -> None:
    name, scope, marker, row, grant, entry = values()
    port = adapter_type(
        objects={("SEMANTIC VIEW", scope.sql): (row,)},
        grants={name.sql: (grant,)},
        markers={name.sql: marker},
        existing=(name.sql,),
        state={"semantic_view:v": entry},
        agent_versions={(name.sql, "committed"): "VERSION$1"},
    )
    assert port.show_objects("SEMANTIC VIEW", scope) == (row,)
    assert port.show_grants("SEMANTIC VIEW", name) == (grant,)
    assert port.describe_marker(name) == marker
    assert port.object_exists("TABLE", name)
    assert port.dataset_exists(name)
    assert port.observe_stage(name) == StageObservation(True)
    assert port.describe_stage_file_format(name) is None
    assert port.observe_staged_file("@DB.SCH.STAGE/eval/abcdef0.yaml") is None
    assert not port.stage_file_exists("@DB.SCH.STAGE/eval/abcdef0.yaml")
    port.upload("@DB.SCH.STAGE/eval/abcdef0.yaml", b"{}")
    assert port.stage_file_exists("@DB.SCH.STAGE/eval/abcdef0.yaml")
    assert port.read_staged_file("@DB.SCH.STAGE/eval/abcdef0.yaml") == b"{}"
    assert port.observe_staged_file("@DB.SCH.STAGE/eval/abcdef0.yaml") == StagedFileMetadata(
        "@DB.SCH.STAGE/eval/abcdef0.yaml",
        "DB.SCH.STAGE/eval/abcdef0.yaml",
        2,
        md5(b"{}", usedforsecurity=False).hexdigest(),
    )
    stage = QualifiedName.from_parts("DB", "SCH", "EVAL_CONFIGS")
    port.stage_formats[stage.sql] = "TYPE='CSV' FIELD_DELIMITER=NONE"
    assert port.observe_stage(stage) == StageObservation(True, "TYPE='CSV' FIELD_DELIMITER=NONE")
    assert port.agent_has_live_version(name) is False
    assert port.resolve_agent_version(name, "committed") == "VERSION$1"
    assert port.current_role()
    assert port.current_account_locator()
    assert port.query(sql("select 1")).rows == ()
    assert port.query_in_context(scope, sql("select 1")).rows == ()
    assert port.execute_script(statements("one")).ok
    assert port.try_execute(statement("two")).ok
    assert port.read_state(name, "dev") == {"semantic_view:v": entry}
    port.write_state(name, "dev", "m", StateWrite(MappingProxyType({}), ("semantic_view:v",)))
    assert port.read_state(name, "dev") == {}
    assert port.read_state_manifest(name, "dev") == "m"
    port.ensure_state_table(name)
    port.write_state(name, "dev", "n", StateWrite(MappingProxyType({"semantic_view:v": entry})))
    assert port.read_state(name, "dev") == {"semantic_view:v": entry}
    claim = LockClaim("run-a")
    assert port.acquire_run_lock(name, "dev", claim, break_stale=False).acquired
    assert not port.acquire_run_lock(name, "dev", LockClaim("run-b"), break_stale=False).acquired
    assert port.extend_run_lock(name, "dev", claim)
    port.release_run_lock(name, "dev", "run-a")
    assert port.acquire_run_lock(name, "dev", LockClaim("run-b"), break_stale=False).acquired


def test_scripted_and_read_only_behavior() -> None:
    failure = ExecResult(False, error=ExecutionError("bad"))
    scripted = ScriptedSnowflake((failure,), (QueryResult(("value",), ((1,),)),))
    assert not scripted.execute_script(statements("bad")).ok
    scripted.query_failures.append(SnowflakePortError("offline"))
    with pytest.raises(SnowflakePortError):
        scripted.query(sql("select 1"))
    assert scripted.query(sql("select 1")).rows == ((1,),)
    assert scripted.query(sql("select 2")).rows == ()

    readonly = ReadOnlySnowflake(RecordedSnowflake(role="R"))
    assert readonly.current_role() == "R"
    with pytest.raises(SnowflakePortError):
        readonly.execute_script(statements("write"))
    with pytest.raises(SnowflakePortError):
        readonly.upload("@DB.SCH.STAGE/file", b"x")
    with pytest.raises(SnowflakePortError):
        readonly.try_execute(statement("write"))
    with pytest.raises(SnowflakePortError):
        readonly.write_state(values()[0], "dev", "m", StateWrite(MappingProxyType({})))
    with pytest.raises(SnowflakePortError):
        readonly.ensure_state_table(values()[0])
    with pytest.raises(SnowflakePortError):
        readonly.acquire_run_lock(values()[0], "dev", LockClaim("r"), break_stale=False)
