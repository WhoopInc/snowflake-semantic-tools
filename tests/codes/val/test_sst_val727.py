"""SST-VAL727: a privilege an eval run needs is held only through a secondary role.

Runs execute as tasks, which ignore secondary roles, so the gate refuses to start the run.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.evals.privileges import eval_role_diagnostics
from snowflake_semantic_tools.app.evals.suite import compiled_evals
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.eval_builders import EvalSnowflake, compile_eval


def test_sst_val727_fires() -> None:
    port = EvalSnowflake([])
    # The primary role lacks CREATE TASK by itself; the session holds it through a secondary role.
    port.preflight.role_lacking["DB.S"] = ("CREATE TASK",)
    [diagnostic] = eval_role_diagnostics(port, compiled_evals(compile_eval()))
    assert (diagnostic.code, diagnostic.severity) == ("SST-VAL727", Severity.ERROR)
    assert diagnostic.message == (
        "eval config for 'sales_agent': CREATE TASK on schema DB.S is held only by a secondary role, not TEST_ROLE"
    )
    assert diagnostic.subject == "eval:sales_agent"


def test_sst_val727_silent() -> None:
    assert eval_role_diagnostics(EvalSnowflake([]), compiled_evals(compile_eval())) == ()
