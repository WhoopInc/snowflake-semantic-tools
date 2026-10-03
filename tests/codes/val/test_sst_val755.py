"""SST-VAL755: an agent eval run would overlap an apply that may regenerate a view the agent uses.

The eval run takes the run lease `sst apply` takes, so the two serialise: while an apply holds
the target's lock, the run does not start, and says which of its agent's views could change.
"""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.evals.suite import (
    EvalGateOutcome,
    EvalGateRefused,
    EvalGateRequest,
    RunEvalGate,
    compiled_evals,
)
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.agent import AgentTool
from snowflake_semantic_tools.domain.model.eval import EvalCatalog, EvalDefaults
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.state.lock import LockClaim
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.artifact_builders import target
from tests.helpers.clocks import FixedClock
from tests.helpers.eval_builders import EvalSnowflake, compile_eval, resolved_eval
from tests.helpers.eval_state_store import InMemoryEvalStateStore
from tests.helpers.project_inputs import InMemoryProjectInputs

STATE_TABLE = QualifiedName.parse("DB.S.SST_STATE")
ORIGIN = Origin("agents/sales/agent.yml", 1)


def _run(port: EvalSnowflake) -> EvalGateOutcome | EvalGateRefused:
    value = resolved_eval()
    tools = (
        AgentTool("cortex_analyst_text_to_sql", ORIGIN, semantic_view="orders"),
        AgentTool("cortex_analyst_text_to_sql", ORIGIN, semantic_view="orders"),
    )
    compiled = compile_eval(replace(value, agent=replace(value.agent, tools=tools)))
    inputs = InMemoryProjectInputs(revision="abcdef0", evals=EvalCatalog((), (), EvalDefaults()))
    gate = RunEvalGate(port, inputs, InMemoryStateStore(), InMemoryEvalStateStore(), FixedClock())
    return gate.run(
        compiled_evals(compiled), build_manifest(compiled), EvalGateRequest(), target=target(), state_table=STATE_TABLE
    )


def test_sst_val755_fires() -> None:
    port = EvalSnowflake([])
    port.run_locks.acquire_run_lock(STATE_TABLE, "verify", LockClaim("apply-run"), break_stale=False)
    refused = _run(port)
    assert isinstance(refused, EvalGateRefused)
    [overlap] = [item for item in refused.diagnostics if item.code == "SST-VAL755"]
    assert overlap.severity is Severity.WARNING
    assert overlap.message == (
        "eval config for 'sales_agent': the run overlaps a regenerate of view 'orders', which the agent uses"
    )
    assert overlap.subject == "eval:sales_agent"
    assert port.queries == []


def test_sst_val755_silent() -> None:
    port = EvalSnowflake([])
    port.run_locks.acquire_run_lock(STATE_TABLE, "verify", LockClaim("eval-earlier"), break_stale=False)
    refused = _run(port)
    assert isinstance(refused, EvalGateRefused)
    assert "SST-VAL755" not in [item.code for item in refused.diagnostics]
