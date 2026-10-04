"""The eval run's privilege check: what the primary role holds by itself, against what the session holds."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.privileges import EVAL_RUN_PRIVILEGES, eval_role_diagnostics
from snowflake_semantic_tools.domain.model.identifier import SchemaScope
from tests.helpers.eval_builders import EvalSnowflake, compiled_eval_of


def _two_evals_in_one_schema() -> tuple[CompiledEval, ...]:
    first = compiled_eval_of()
    return first, replace(first, resolved=replace(first.resolved, agent=replace(first.resolved.agent, name="ops")))


def test_each_schema_is_read_once_and_each_eval_in_it_is_reported() -> None:
    class Counting(EvalSnowflake):
        reads: list[str] = []

        def missing_role_privileges(
            self, role: str, scope: SchemaScope, privileges: tuple[str, ...]
        ) -> tuple[str, ...]:
            self.reads.append(role)
            return super().missing_role_privileges(role, scope, privileges)

    port = Counting([])
    port.preflight.role_lacking["DB.S"] = EVAL_RUN_PRIVILEGES
    port.preflight.lacking["DB.S"] = ("CREATE FILE FORMAT",)
    found = eval_role_diagnostics(port, _two_evals_in_one_schema())
    assert port.reads == ["TEST_ROLE"]
    assert [(item.code, item.subject) for item in found] == [
        ("SST-VAL727", "eval:sales_agent"),
        ("SST-VAL728", "eval:sales_agent"),
        ("SST-VAL727", "eval:ops"),
        ("SST-VAL728", "eval:ops"),
    ]
    assert found[0].message.endswith(
        "CREATE TASK, CREATE STAGE on schema DB.S is held only by a secondary role, not TEST_ROLE"
    )
    assert found[1].message.endswith("TEST_ROLE lacks CREATE FILE FORMAT on schema DB.S")


def test_a_primary_role_that_holds_everything_needs_no_session_read() -> None:
    port = EvalSnowflake([])
    port.refuse("missing_privileges")
    assert eval_role_diagnostics(port, _two_evals_in_one_schema()) == ()
