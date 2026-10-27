"""SST-VAL544: an auto-orchestrated agent or CoWork profile has a blocking eval."""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.project import CompileProject
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, DiagnosticBag, Severity
from snowflake_semantic_tools.domain.model.eval import EvalCatalog
from tests.helpers.agent_builders import agent, compile_agents, cross, evaluation
from tests.helpers.cowork_profiles import cowork_profile, described_agent, subagent
from tests.helpers.diagnostic_filters import coded
from tests.helpers.eval_inputs import concierge_eval, settings_of, settled
from tests.helpers.project_inputs import InMemoryProjectInputs

# The profile publishes to a rehearsal schema, where its eval compiles; the model it pins is listed.
PROFILES = {
    "snowflake": {"orchestration_models": ["auto", "claude-sonnet-4-6"]},
    "cowork_profiles": {"+schema": "PROFILES"},
}


def _tiered(tier: str) -> list[Diagnostic]:
    model = agent("sales_agent", orchestration_model="auto")
    result = compile_agents(model)
    return coded(cross(result, evals=(evaluation(model, "How many orders?", tier=tier),)), "SST-VAL544")


def _profile(tier: str, **config: object) -> list[Diagnostic]:
    """Compile the `concierge` profile, routing to `sales`, with an eval at `tier`; `config` is its own."""
    profile = cowork_profile("concierge", subagent(), settings=settings_of(config))
    inputs = InMemoryProjectInputs(
        tree=PROFILES,
        agent_models=(described_agent(),),
        cowork_profile_models=(profile,),
        evals=EvalCatalog((settled(concierge_eval(profile=profile), eval_tier=tier),), (), diagnostics=DiagnosticBag()),
    )
    return [item for item in CompileProject(inputs).run().diagnostics if item.subject == "cowork_profile:concierge"]


def test_sst_val544_fires() -> None:
    [diagnostic] = _tiered("blocking")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message == "agent 'sales_agent': orchestration_model is auto and a blocking eval is configured"
    assert diagnostic.subject == "agent:sales_agent"


def test_sst_val544_silent() -> None:
    assert _tiered("report") == []


def test_sst_val544_fires_for_a_cowork_profile_kept_on_auto_deliberately() -> None:
    [diagnostic] = coded(_profile("blocking", allow_auto_model=True), "SST-VAL544")
    assert diagnostic.message == (
        "cowork profile 'concierge': orchestration_model is auto and a blocking eval is configured"
    )
    assert diagnostic.suggestion == (
        "pin the model with `cowork_profiles.+orchestration_model` or the cowork profile's `config:`"
    )
    # One message per subject: the warning about `auto` is not reported beside the refusal.
    assert coded(_profile("blocking"), "SST-VAL560") == []


def test_sst_val544_silent_for_a_cowork_profile_pinned_or_not_gated() -> None:
    assert coded(_profile("blocking", orchestration_model="claude-sonnet-4-6"), "SST-VAL544") == []
    reporting = _profile("report")
    assert coded(reporting, "SST-VAL544") == []
    assert [item.code for item in coded(reporting, "SST-VAL560")] == ["SST-VAL560"]
