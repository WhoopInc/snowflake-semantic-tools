"""Ask an agent built from a specification one question: a temporary agent, created, run and dropped.

Measured on a live account: SST's compiled specifications, every tool type included, run as
`CREATE OR REPLACE TEMPORARY AGENT ... FROM SPECIFICATION`, then `DATA_AGENT_RUN`, then
`DROP AGENT IF EXISTS`, all on one session, since a temporary agent exists in its session
alone; and the same in a personal `USER$<user>.PUBLIC` schema. The reply's `content` is a
list of `tool_use`, `tool_result`, `thinking`, `text` and `suggested_queries` items; a run
that fails answers `code` and `message` instead. Its statements bypass the read cache's write
rule as the model probe's do (`cortex_models`): nothing they do is visible to another session.
"""

from __future__ import annotations

import json
import secrets
import time
from collections.abc import Mapping
from types import MappingProxyType

from snowflake_semantic_tools.adapters.json_files import JsonFileError, parse_json
from snowflake_semantic_tools.adapters.snowflake.connector.session import (
    Session,
    _as_port_errors,
    _execute,
    _fetch,
    _headline,
)
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.ports.snowflake.agent_run import AgentReply, AgentRunPort, ToolResult, ToolUse
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.sql import Sql, dollar_quoted, qname, sql


def ask_agent_name(scope: SchemaScope, agent: str, suffix: str) -> QualifiedName:
    """Return the temporary agent's name in `scope`: `SST_ASK_<AGENT>_<suffix>`, upper-cased."""
    stem = "".join(char if char.isalnum() else "_" for char in agent).upper()
    return QualifiedName(scope.database, scope.schema, Identifier.parse(f"SST_ASK_{stem}_{suffix.upper()}"))


def ask_statements(agent: QualifiedName, spec_json: str) -> tuple[Sql, Sql, Sql]:
    """Return the CREATE, the run, and the DROP of one temporary agent built from `spec_json`.

    The run binds the agent's name and the request.

    Raises:
        ValueError: the specification holds `$$`, which would end the literal it is inlined in.
    """
    create = sql(
        "CREATE OR REPLACE TEMPORARY AGENT {agent} FROM SPECIFICATION {specification}",
        agent=qname(agent),
        specification=dollar_quoted(spec_json.rstrip()),
    )
    run = sql("SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN(%s, %s) AS RESPONSE")
    drop = sql("DROP AGENT IF EXISTS {agent}", agent=qname(agent))
    return create, run, drop


def ask_request(question: str) -> str:
    """Return the request body `DATA_AGENT_RUN` takes for one user question."""
    message = {"role": "user", "content": [{"type": "text", "text": question}]}
    return json.dumps({"messages": [message]}, separators=(",", ":"))


def read_agent_reply(raw: object, duration_ms: int = 0) -> AgentReply:
    """Read what `DATA_AGENT_RUN` returned: the reply's text, tool calls and tool results, or its error.

    A response with no `content`, or with a `code`, is an error, reported by its `message`;
    one that is not JSON, or not an object, is an error reported by its text. Items of other
    types, and items that are not objects, are skipped.
    """
    try:
        response = parse_json(raw) if isinstance(raw, str) else raw
    except JsonFileError:
        return AgentReply(error=_headline(str(raw)) or "the agent returned no response", duration_ms=duration_ms)
    if not isinstance(response, dict) or response.get("code") or not isinstance(response.get("content"), list):
        message = response.get("message") if isinstance(response, dict) else None
        detail = message if isinstance(message, str) and message else json.dumps(response, default=str)
        return AgentReply(error=_headline(detail), duration_ms=duration_ms)
    items = [item for item in response["content"] if isinstance(item, dict)]
    texts = [str(item.get("text") or "") for item in items if item.get("type") == "text"]
    return AgentReply(
        answer="\n\n".join(text for text in texts if text),
        tool_uses=tuple(_tool_use(item.get("tool_use")) for item in items if item.get("type") == "tool_use"),
        tool_results=tuple(
            _tool_result(item.get("tool_result")) for item in items if item.get("type") == "tool_result"
        ),
        duration_ms=duration_ms,
    )


def _tool_use(value: object) -> ToolUse:
    body = value if isinstance(value, dict) else {}
    given = body.get("input")
    return ToolUse(
        str(body.get("name") or ""),
        str(body.get("type") or ""),
        MappingProxyType(dict(given)) if isinstance(given, Mapping) else MappingProxyType({}),
    )


def _tool_result(value: object) -> ToolResult:
    body = value if isinstance(value, dict) else {}
    return ToolResult(str(body.get("name") or ""), str(body.get("status") or ""))


class AgentRunMethods(Session, AgentRunPort):
    """Ask a temporary agent built from a specification one question, then drop it."""

    def run_inline(self, scope: SchemaScope, agent: str, spec_json: str, question: str, timeout: int) -> AgentReply:
        name = ask_agent_name(scope, agent, secrets.token_hex(4))
        create, run, drop = ask_statements(name, spec_json)
        # The drop follows a failed CREATE too: whether a refused CREATE OR REPLACE left an
        # agent behind is not measured, and DROP IF EXISTS costs one statement.
        try:
            with _as_port_errors(), self._cursor() as cursor:
                _execute(cursor, create)
            started = time.monotonic()
            with _as_port_errors(), self._cursor() as cursor:
                result = _fetch(cursor, run, (name.sql, ask_request(question)), timeout_seconds=timeout)
        finally:
            self._drop_temporary(drop)
        elapsed = int((time.monotonic() - started) * 1000)
        return read_agent_reply(result.rows[0][0] if result.rows and result.rows[0] else None, elapsed)

    def _drop_temporary(self, drop: Sql) -> None:
        """Drop the temporary agent; one left behind ends with the session, so a failure is ignored."""
        try:
            with _as_port_errors(), self._cursor() as cursor:
                _execute(cursor, drop)
        except SnowflakePortError:
            return
