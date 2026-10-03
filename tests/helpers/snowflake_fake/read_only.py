"""A read-only view of a Snowflake port: reads pass through, and every write is refused.

It stands where the CLI hands a command a port that must never write, so a test proves the
command's reads alone suffice.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import ExecResult, QueryResult
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePort
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.sql import Sql
from snowflake_semantic_tools.domain.state.lock import LockAcquisition, LockClaim, LockFence, StateWrite


class ReadOnlySnowflake:
    """Delegates reads to `delegate`; scripts, uploads, state writes, and the run lock raise."""

    def __init__(self, delegate: SnowflakePort) -> None:
        self._delegate = delegate

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)

    def execute_script(self, statements: Sequence[Sql]) -> ExecResult:
        raise SnowflakePortError(f"read-only Snowflake adapter refused {len(statements)} statement(s)")

    def query_in_context(
        self,
        scope: SchemaScope,
        sql: Sql,
        params: Sequence[object] | Mapping[str, object] | None = None,
    ) -> QueryResult:
        return self._delegate.query_in_context(scope, sql, params)

    def upload(self, stage_path: str, content: bytes) -> None:
        del stage_path, content
        raise SnowflakePortError("read-only Snowflake adapter refused upload")

    def read_staged_file(self, stage_path: str) -> bytes | None:
        return self._delegate.read_staged_file(stage_path)

    def try_execute(self, sql: Sql) -> ExecResult:
        raise SnowflakePortError(f"read-only Snowflake adapter refused statement: {str(sql)[:40]}")

    def resolve_agent_version(self, qualified_name: QualifiedName, selector: str) -> str:
        return self._delegate.resolve_agent_version(qualified_name, selector)

    def write_state(
        self,
        state_table: QualifiedName,
        target_name: str,
        manifest_id: str,
        write: StateWrite,
        fence: LockFence,
    ) -> bool:
        del state_table, target_name, manifest_id, write, fence
        raise SnowflakePortError("read-only Snowflake adapter refused state write")

    def ensure_state_table(self, state_table: QualifiedName) -> None:
        del state_table
        raise SnowflakePortError("read-only Snowflake adapter refused state table creation")

    def acquire_run_lock(
        self,
        state_table: QualifiedName,
        target_name: str,
        claim: LockClaim,
        *,
        break_stale: bool,
    ) -> LockAcquisition:
        del state_table, target_name, claim, break_stale
        raise SnowflakePortError("read-only Snowflake adapter refused the run lock")
