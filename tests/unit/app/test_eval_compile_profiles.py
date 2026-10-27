"""Compiling a CoWork profile's eval (a Snowflake private preview), and refusing it unpublished.

A profile's eval compiles into the profile's schema, with each expected subagent rendered as the
qualified name the profile's router gives it on the target. It is left out where its profile did
not compile, and where the target publishes the profile to SNOWFLAKE_INTELLIGENCE.PROFILES.
"""

from __future__ import annotations

import json

from snowflake_semantic_tools.app.compile.evals import CompiledEval, CompileEvals
from snowflake_semantic_tools.app.compile.project import CompileProject
from snowflake_semantic_tools.app.evals.inline import unpublished_refusals
from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.model.eval import EvalCatalog
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.cowork_profiles import LISTED, cowork_profile, described_agent, subagent
from tests.helpers.eval_builders import REHEARSAL, compile_eval, compile_profile_eval, compiled_eval_of
from tests.helpers.eval_inputs import concierge_eval, routed
from tests.helpers.project_inputs import InMemoryProjectInputs


def _evals(result_compiled: tuple[object, ...]) -> list[CompiledEval]:
    return [item for item in result_compiled if isinstance(item, CompiledEval)]


def test_a_profile_eval_compiles_into_the_profile_schema_with_its_subagents_qualified() -> None:
    rows = (routed("Who sold most?", "agent:sales", "tool:partner_agent"), routed("Suppliers?"))
    result = compile_profile_eval(concierge_eval(*rows))
    [compiled] = _evals(result.compiled)
    assert compiled.agent_target == REHEARSAL
    assert compiled.dataset_target.sql.startswith("DB.PROFILES.EVAL_CONCIERGE_")
    expected = [row["ground_truth"]["ground_truth_subagents"] for row in json.loads(compiled.rendered.dataset_payload)]
    assert expected == [["DB.AGENTS.SALES", "PARTNER.AGENTS.DELIVERY"], []]
    assert compiled.resolved.depends_on == ("cowork_profile:concierge",)
    assert compiled.rendered_artifact.depends_on == ("cowork_profile:concierge",)


def test_a_profile_eval_whose_profile_did_not_compile_is_left_out_without_a_report() -> None:
    resolved = concierge_eval()
    result = CompileEvals(EvalCatalog((resolved,), ()), agent_targets={"concierge": REHEARSAL}).run_result()
    assert (result.compiled, tuple(result.diagnostics)) == ((), ())


def test_an_agent_eval_with_no_target_still_reports_an_internal_error() -> None:
    result = CompileEvals(EvalCatalog((compiled_eval_of().resolved,), ()), agent_targets={}).run_result()
    assert [item.code for item in result.diagnostics] == ["SST-INT902"]


def test_an_expected_subagent_the_target_does_not_hold_is_an_internal_error() -> None:
    rows = (routed("Who sold most?", "agent:ops"), routed("Suppliers?"))
    result = compile_profile_eval(concierge_eval(*rows))
    assert [item.code for item in result.diagnostics] == ["SST-INT902"]
    assert _evals(result.compiled) == []


def test_a_profile_eval_where_cowork_lists_the_profile_plans_nothing_there() -> None:
    listed = QualifiedName.parse("SNOWFLAKE_INTELLIGENCE.PROFILES.CONCIERGE")
    result = compile_profile_eval(concierge_eval(), listed)
    assert [item.code for item in result.diagnostics] == ["SST-VAL886"]
    assert result.compiled == ()


def test_the_project_compiles_a_profile_eval_against_the_profile_it_compiled() -> None:
    profile = cowork_profile("concierge", subagent())
    resolved = concierge_eval(routed("Who sold most?", "agent:sales"), routed("Suppliers?"), profile=profile)
    inputs = InMemoryProjectInputs(
        tree={"cowork_profiles": {"+schema": "PROFILES"}},
        agent_models=(described_agent(),),
        cowork_profile_models=(profile, cowork_profile("retired", enabled=False)),
        evals=EvalCatalog((resolved,), (), diagnostics=DiagnosticBag()),
    )
    result = CompileProject(inputs).run()
    [compiled] = _evals(result.compiled)
    assert compiled.agent_target.sql == "DB.PROFILES.CONCIERGE"
    [row, _] = json.loads(compiled.rendered.dataset_payload)
    assert row["ground_truth"]["ground_truth_subagents"] == ["DB.SCH.SALES"]
    [profiles] = inputs.profile_eval_requests
    assert isinstance(profiles, tuple) and [model.name for model in profiles] == ["concierge"]


def test_the_project_leaves_out_a_profile_eval_where_cowork_lists_the_profile() -> None:
    profile = cowork_profile("concierge", subagent())
    resolved = concierge_eval(routed("Who sold most?", "agent:sales"), routed("Suppliers?"), profile=profile)
    inputs = InMemoryProjectInputs(
        tree=dict(LISTED),
        agent_models=(described_agent(),),
        cowork_profile_models=(profile,),
        evals=EvalCatalog((resolved,), (), diagnostics=DiagnosticBag()),
    )
    result = CompileProject(inputs).run()
    assert _evals(result.compiled) == []
    assert "SST-VAL886" in [item.code for item in result.diagnostics]


def test_an_unpublished_run_refuses_only_the_profile_evals() -> None:
    agent_eval = compiled_eval_of()
    [profile_eval] = _evals(compile_profile_eval(concierge_eval()).compiled)
    refused = unpublished_refusals((agent_eval, profile_eval))
    assert [(item.code, item.subject) for item in refused] == [("SST-ASK013", "eval:cowork_profile:concierge")]
    assert unpublished_refusals(_evals(compile_eval().compiled)) == ()
