"""SST-VAL720: referenced agent version does not exist.

An eval pinned to an alias the agent no longer carries cannot run; a pin that resolves runs.
"""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.run import EvalRunOptions, RunEvalSuite
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.ports.snowflake.errors import AgentVersionNotFound
from tests.helpers.clocks import FixedClock
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_builders import EvalSnowflake, compiled_eval_of, result_rows, status_result


class DroppedAliasSnowflake(EvalSnowflake):
    def resolve_agent_version(self, qualified_name: QualifiedName, selector: str) -> str:
        raise AgentVersionNotFound(f"agent {qualified_name.sql} selector {selector!r} names no version")


def pinned(selector: str) -> CompiledEval:
    compiled = compiled_eval_of()
    config = replace(compiled.resolved.config, agent_version=selector)
    return replace(compiled, resolved=replace(compiled.resolved, config=config))


def test_sst_val720_fires() -> None:
    port = DroppedAliasSnowflake([])
    result = RunEvalSuite(port, FixedClock()).run(
        (pinned("alias:prod"),), options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z")
    )
    diagnostic = only(result.diagnostics, "SST-VAL720")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "eval config for 'sales_agent': version 'alias:prod' does not exist or is dropped"
    assert diagnostic.subject == "eval:sales_agent"
    assert not result.success and port.scripts == []


def test_sst_val720_silent() -> None:
    port = EvalSnowflake(
        [status_result("COMPLETED"), result_rows()], agent_versions={("DB.S.SALES_AGENT", "alias:prod"): "VERSION$2"}
    )
    result = RunEvalSuite(port, FixedClock()).run(
        (pinned("alias:prod"),), options=EvalRunOptions("abcdef0", timestamp="20260928T010203Z")
    )
    assert "SST-VAL720" not in codes(result.diagnostics)
    assert result.success
