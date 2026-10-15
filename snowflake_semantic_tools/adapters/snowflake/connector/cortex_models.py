"""The Cortex base models SHOW lists, and a throwaway agent run on one model.

`CREATE AGENT` accepts any model name; a model agents refuse fails only when the agent runs,
through `DATA_AGENT_RUN`, with a message naming why. So one probe creates a TEMPORARY agent
with no tools and a small budget, runs it once, and drops it, all on one session, since a
temporary agent exists in its session alone. Its statements bypass the read cache's write
rule (`Session._forget_unless_read`): nothing they do is visible to another session's reads.
"""

from __future__ import annotations

import json
from typing import Any

from snowflake_semantic_tools.adapters.json_files import JsonFileError, parse_json
from snowflake_semantic_tools.adapters.snowflake.connector.cortex import _MODEL_NAME
from snowflake_semantic_tools.adapters.snowflake.connector.session import (
    Session,
    _as_port_errors,
    _execute,
    _fetch,
)
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.ports.snowflake.cortex_models import (
    AgentProbe,
    CortexBaseModel,
    CortexModelsPort,
    ProbeOutcome,
)
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.sql import Sql, dollar_quoted, qname, sql

# The budget of one probe: enough for one model turn, too little for anything else.
PROBE_SECONDS = 30
PROBE_TOKENS = 2000
# How long the run may take before the driver cancels it. The budget does not bound a run
# whose model never answers, which otherwise waits out the session's statement timeout.
PROBE_TIMEOUT_SECONDS = 60
# What the probe agent is asked: an answer of any text shows the model ran.
PROBE_REQUEST = json.dumps(
    {"messages": [{"role": "user", "content": [{"type": "text", "text": "Reply with the single word OK."}]}]},
    separators=(",", ":"),
)


def base_models_sql() -> Sql:
    """Return the SHOW of every Cortex base model the account can see."""
    return sql("SHOW CORTEX BASE MODELS IN SCHEMA SNOWFLAKE.MODELS")


def probe_agent_name(scope: SchemaScope, model: str) -> QualifiedName:
    """Return the probe agent's name in `scope`: one per model, so concurrent probes never share one."""
    suffix = "".join(char if char.isalnum() else "_" for char in model).upper()
    return QualifiedName(scope.database, scope.schema, Identifier.parse(f"SST_MODEL_PROBE_{suffix}"))


def probe_statements(agent: QualifiedName, model: str) -> tuple[Sql, Sql, Sql]:
    """Return the CREATE, the run, and the DROP of one probe agent orchestrated by `model`.

    The run binds the agent's name and the request; the model is checked as a plain name
    before it is written into the specification.

    Raises:
        SnowflakePortError: `model` is not a plain Cortex model name.
    """
    if not _MODEL_NAME.fullmatch(model):
        raise SnowflakePortError(f"{model!r} is not a Cortex model name")
    specification = json.dumps(
        {
            "models": {"orchestration": model},
            "orchestration": {"budget": {"seconds": PROBE_SECONDS, "tokens": PROBE_TOKENS}},
        },
        separators=(",", ":"),
    )
    create = sql(
        "CREATE OR REPLACE TEMPORARY AGENT {agent} FROM SPECIFICATION {specification}",
        agent=qname(agent),
        specification=dollar_quoted(specification),
    )
    run = sql("SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN(%s, %s) AS RESPONSE")
    drop = sql("DROP AGENT IF EXISTS {agent}", agent=qname(agent))
    return create, run, drop


def read_probe_response(raw: object) -> AgentProbe:
    """Read what `DATA_AGENT_RUN` returned: an answer accepts the model, an error message refuses it."""
    try:
        response = parse_json(raw) if isinstance(raw, str) else raw
    except JsonFileError:
        return AgentProbe(ProbeOutcome.REJECTED, _first_line(str(raw)))
    if isinstance(response, dict):
        if "content" in response and not response.get("code"):
            return AgentProbe(ProbeOutcome.ACCEPTED)
        message = response.get("message")
        if isinstance(message, str) and message:
            return AgentProbe(ProbeOutcome.REJECTED, _first_line(message))
    return AgentProbe(ProbeOutcome.REJECTED, _first_line(json.dumps(response, default=str)))


def base_model(row: dict[str, Any]) -> CortexBaseModel | None:
    """Read one SHOW row; None for a row with no name."""
    name = row.get("name")
    if not isinstance(name, str) or not name:
        return None
    return CortexBaseModel(
        name.casefold(),
        _text(row.get("lifecycle_status")),
        _text(row.get("legacy_date")),
        _text(row.get("eol_date")),
    )


class CortexModelsMethods(Session, CortexModelsPort):
    """List the account's Cortex base models, and run a throwaway agent on one."""

    def base_models(self) -> tuple[CortexBaseModel, ...]:
        rows = self._dict_rows(base_models_sql())
        return tuple(model for row in rows if (model := base_model(row)) is not None)

    def probe_orchestration_model(self, scope: SchemaScope, model: str) -> AgentProbe:
        agent = probe_agent_name(scope, model)
        create, run, drop = probe_statements(agent, model)
        try:
            with _as_port_errors(), self._cursor() as cursor:
                _execute(cursor, create)
        except SnowflakePortError as exc:
            return AgentProbe(ProbeOutcome.UNPROBED, _first_line(str(exc)))
        try:
            with _as_port_errors(), self._cursor() as cursor:
                result = _fetch(cursor, run, (agent.sql, PROBE_REQUEST), timeout_seconds=PROBE_TIMEOUT_SECONDS)
        except SnowflakePortError as exc:
            return AgentProbe(ProbeOutcome.UNPROBED, _first_line(str(exc)))
        finally:
            self._drop_quietly(drop)
        return read_probe_response(result.rows[0][0] if result.rows and result.rows[0] else None)

    def _drop_quietly(self, drop: Sql) -> None:
        """Drop the probe agent; one left behind ends with the session, so a failure is ignored."""
        try:
            with _as_port_errors(), self._cursor() as cursor:
                _execute(cursor, drop)
        except SnowflakePortError:
            return


def _first_line(message: str) -> str:
    """Return a message's first non-empty line, trimmed."""
    return next((line.strip() for line in message.splitlines() if line.strip()), "")


def _text(value: object) -> str | None:
    """Return a SHOW cell as text; None for NULL or an empty cell."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None
