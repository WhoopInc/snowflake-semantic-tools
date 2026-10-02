"""Check, before an eval runs, that one primary role holds what the run needs in its agent's schema.

An evaluation executes as Snowflake tasks, and a task runs as the session's primary role and
never its secondary roles. So a privilege the session holds only through a secondary role
works interactively and fails in the run. Each eval's agent schema is read once.
"""

from __future__ import annotations

from typing import Protocol

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.identifier import SchemaScope
from snowflake_semantic_tools.domain.ports.snowflake.preflight import PreflightPort

# The schema privileges a run creates its task, stage and file format with.
EVAL_RUN_PRIVILEGES = ("CREATE TASK", "CREATE STAGE", "CREATE FILE FORMAT")


class EvalRolePort(PreflightPort, Protocol):
    """The reads the check needs: the primary role, its own privileges, and the session's."""

    def current_role(self) -> str:
        """Return the session's primary role.

        Raises:
            SnowflakePortError: the query failed.
        """
        ...


def eval_role_diagnostics(port: EvalRolePort, evals: tuple[CompiledEval, ...]) -> tuple[Diagnostic, ...]:
    """Report, eval by eval, each run privilege the primary role lacks on its agent's schema.

    Raises:
        SnowflakePortError: the role or a schema's grants could not be read.

    Diagnostics:
        SST-VAL727: the session holds a run privilege only through a secondary role.
        SST-VAL728: no role in the session holds a run privilege.
    """
    role = port.current_role()
    lacking: dict[SchemaScope, tuple[tuple[str, ...], tuple[str, ...]]] = {}
    found: list[Diagnostic] = []
    for item in evals:
        scope = SchemaScope.from_qualified_name(item.agent_target)
        if scope not in lacking:
            lacking[scope] = _lacking(port, role, scope)
        secondary, absent = lacking[scope]
        if secondary:
            detail = f"{', '.join(secondary)} on schema {scope.sql} is held only by a secondary role, not {role}"
            found.append(D("SST-VAL727", subject=item.artifact_key, artifact=item.name, detail=detail))
        if absent:
            detail = f"{', '.join(absent)} on schema {scope.sql}"
            found.append(D("SST-VAL728", subject=item.artifact_key, artifact=item.name, value=role, detail=detail))
    return tuple(found)


def _lacking(port: EvalRolePort, role: str, scope: SchemaScope) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Split what the primary role lacks into what a secondary role holds and what nothing does."""
    primary = port.missing_role_privileges(role, scope, EVAL_RUN_PRIVILEGES)
    if not primary:
        return (), ()
    session = set(port.missing_privileges(scope, primary))
    return tuple(item for item in primary if item not in session), tuple(item for item in primary if item in session)
