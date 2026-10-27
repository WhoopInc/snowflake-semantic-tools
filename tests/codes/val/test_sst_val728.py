"""SST-VAL728: no role in the session holds a schema privilege an eval run creates its objects with."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.app.evals.privileges import eval_role_diagnostics
from snowflake_semantic_tools.app.evals.suite import EvalGateOutcome, EvalGateRequest, RunEvalGate, compiled_evals
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag, Severity
from snowflake_semantic_tools.domain.model.eval import EvalCatalog, EvalDefaults
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.app_ports import InMemoryStateStore
from tests.helpers.artifact_builders import target
from tests.helpers.clocks import FixedClock
from tests.helpers.eval_builders import EvalSnowflake, compile_eval
from tests.helpers.eval_state_store import InMemoryEvalStateStore
from tests.helpers.project_inputs import InMemoryProjectInputs


def _lacking_everywhere(port: EvalSnowflake, *privileges: str) -> None:
    port.preflight.role_lacking["DB.S"] = privileges
    port.preflight.lacking["DB.S"] = privileges


def test_sst_val728_fires(monkeypatch: pytest.MonkeyPatch) -> None:
    port = EvalSnowflake([])
    _lacking_everywhere(port, "CREATE STAGE", "CREATE FILE FORMAT")
    [diagnostic] = eval_role_diagnostics(port, compiled_evals(compile_eval()))
    assert (diagnostic.code, diagnostic.severity) == ("SST-VAL728", Severity.ERROR)
    assert diagnostic.message == (
        "eval config for 'sales_agent': TEST_ROLE lacks CREATE STAGE, CREATE FILE FORMAT on schema DB.S"
    )
    assert diagnostic.subject == "eval:sales_agent"
    # The gate starts no run on it.
    monkeypatch.setattr(
        "snowflake_semantic_tools.app.evals.suite.validate_eval_publication", lambda *args: DiagnosticBag()
    )
    compiled = compile_eval()
    inputs = InMemoryProjectInputs(revision="abcdef0", evals=EvalCatalog((), (), EvalDefaults()))
    outcome = RunEvalGate(port, inputs, InMemoryStateStore(), InMemoryEvalStateStore(), FixedClock()).run(
        compiled_evals(compiled),
        build_manifest(compiled),
        EvalGateRequest(),
        target=target(),
        state_table=QualifiedName.parse("DB.S.SST_STATE"),
    )
    assert isinstance(outcome, EvalGateOutcome) and outcome.suite is None and not outcome.passed
    assert [item.code for item in outcome.diagnostics] == ["SST-VAL728"]
    assert port.scripts == []


def test_sst_val728_silent() -> None:
    port = EvalSnowflake([])
    # Missing elsewhere, and the run schema holds everything.
    port.preflight.lacking["DB.OTHER"] = ("CREATE TASK",)
    port.preflight.role_lacking["DB.OTHER"] = ("CREATE TASK",)
    assert eval_role_diagnostics(port, compiled_evals(compile_eval())) == ()
