"""The orchestration-model check: which models it probes, where, and when it runs at all."""

from __future__ import annotations

import dataclasses
from types import MappingProxyType

from snowflake_semantic_tools.app.compile import CompileResult
from snowflake_semantic_tools.app.compile.agents.compiled import CompiledAgent
from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.app.validate import ValidateArtifacts
from snowflake_semantic_tools.app.validate_models import ModelCheck, configured_models, probe_scope
from snowflake_semantic_tools.domain.model.identifier import Identifier, SchemaScope, TargetIdentity
from snowflake_semantic_tools.domain.model.setting import Setting
from tests.helpers.cortex_models import FakeCortexModels, agent_compile, checked, ga


def test_auto_is_never_probed_and_each_model_once_ignoring_case() -> None:
    port = FakeCortexModels((ga("claude-sonnet-4-6"), ga("claude-opus-5")))
    assert checked(["auto", "AUTO", "claude-sonnet-4-6", "CLAUDE-SONNET-4-6", "claude-opus-5"], port) == []
    assert [model for _, model in port.probed] == ["claude-sonnet-4-6", "claude-opus-5"]
    assert port.shown == 1


def test_a_list_of_only_auto_reads_nothing() -> None:
    port = FakeCortexModels()
    assert checked(["auto"], port) == []
    assert port.probed == [] and port.shown == 0


def test_probes_run_in_the_first_published_agents_schema_by_key() -> None:
    port = FakeCortexModels((ga("m"),))
    checked(["m"], port, agent_compile("zeta", "alpha"))
    assert port.probed == [(SchemaScope(Identifier.parse("DB"), Identifier.parse("ALPHA_S")), "m")]
    assert probe_scope(CompileResult(())) is None


def _with_scratch(compiled: CompileResult, value: str) -> CompileResult:
    """The compile's one agent with `agents.+scratch_schema` set to `value`."""
    agent = compiled.compiled[0]
    assert isinstance(agent, CompiledAgent)
    model = agent.resolved.model
    settings = MappingProxyType({**model.settings, "scratch_schema": Setting("scratch_schema", value, "root")})
    resolved = dataclasses.replace(agent.resolved, model=dataclasses.replace(model, settings=settings))
    return CompileResult((dataclasses.replace(agent, resolved=resolved),))


def test_probes_run_in_the_scratch_schema_when_set_as_sst_asks_temporary_agents_do() -> None:
    identity = TargetIdentity("dev", "acme", Identifier.parse("DEV_DB"), Identifier.parse("PUBLIC"))
    scratched = _with_scratch(agent_compile("alpha"), "{{ target.database }}.SCRATCH")
    assert probe_scope(scratched, identity) == SchemaScope(Identifier.parse("DEV_DB"), Identifier.parse("SCRATCH"))
    # Without the target to render it for, and when it renders to no schema, the agent's own.
    own = SchemaScope(Identifier.parse("DB"), Identifier.parse("ALPHA_S"))
    assert probe_scope(scratched) == own
    assert probe_scope(_with_scratch(agent_compile("alpha"), "{{ target.database }}"), identity) == own


def test_offline_validation_never_probes_and_reports_the_check_skipped() -> None:
    port = FakeCortexModels((ga("m"),))
    result = ValidateArtifacts(models=ModelCheck(("m",), Fanout(port))).run(
        agent_compile("sales"), strict=False, connected=False
    )
    assert port.probed == [] and port.shown == 0
    skipped = [item.context["rule_id"] for item in result.diagnostics if item.code == "SST-VAL020"]
    assert "SST-VAL551" in skipped


def test_the_configured_list_defaults_to_auto() -> None:
    assert configured_models({}) == ("auto",)
    assert configured_models({"snowflake": {"orchestration_models": "auto"}}) == ("auto",)
    assert configured_models({"snowflake": {"orchestration_models": ["auto", "m"]}}) == ("auto", "m")
