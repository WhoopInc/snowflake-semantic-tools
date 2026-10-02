"""SST-VAL704: resolved eval schema differs from the agent schema.

A dataset name template qualified into another schema would land the eval outside its agent's
schema, so the eval does not compile; one qualified into the agent's own schema is placed there.
"""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.eval import EvalDatasetConfig, ResolvedEval
from tests.helpers.eval_builders import compile_eval, resolved_eval
from tests.helpers.eval_inputs import codes, only


def with_dataset_template(template: str) -> ResolvedEval:
    value = resolved_eval()
    dataset = EvalDatasetConfig("auto", template, "EVAL_SRC_{{ agent | upper }}_{{ sha7 }}")
    return replace(value, config=replace(value.config, dataset=dataset))


def test_sst_val704_fires() -> None:
    result = compile_eval(with_dataset_template("SCRATCH.ME.EVAL_{{ agent | upper }}_{{ sha7 }}"))
    diagnostic = only(result.diagnostics, "SST-VAL704")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == (
        "dataset 'SCRATCH.ME.EVAL_SALES_AGENT_0000000' resolves to SCRATCH.ME; agent 'sales_agent' resolves to DB.S"
    )
    assert diagnostic.subject == "eval:sales_agent"
    assert result.compiled == ()


def test_sst_val704_silent() -> None:
    result = compile_eval(with_dataset_template("DB.S.EVAL_{{ agent | upper }}_{{ sha7 }}"))
    assert "SST-VAL704" not in codes(result.diagnostics)
    [compiled] = result.compiled
    assert isinstance(compiled, CompiledEval)
    assert compiled.dataset_target.sql.startswith("DB.S.EVAL_SALES_AGENT_")
