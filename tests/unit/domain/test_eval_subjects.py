"""What an eval evaluates, an agent or a CoWork profile, and the rules each kind's eval keeps.

CoWork profiles are a Snowflake private preview: a profile's eval is keyed apart from an agent's,
expects subagents rather than tool calls, renders each expected subagent as the qualified name
its router gives it on the target, and does not compile where CoWork lists the profile.
"""

from __future__ import annotations

import json

import pytest

from snowflake_semantic_tools.domain.diagnostics import Origin
from snowflake_semantic_tools.domain.model.agent import AgentEvalFiles, AgentModel
from snowflake_semantic_tools.domain.model.cowork_profile import (
    CoworkProfileModel,
    CoworkProfileTool,
    ResolvedCoworkProfile,
    ResolvedSubagent,
)
from snowflake_semantic_tools.domain.model.eval import (
    EvalQuestion,
    EvalSubagentRef,
    EvalSubject,
    ThresholdRange,
    profile_subject_target,
)
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.render.eval import render_dataset_payload
from snowflake_semantic_tools.domain.validate.eval_subject import cowork_listed_placement, subject_rules
from tests.helpers.diagnostic_filters import codes
from tests.helpers.eval_inputs import (
    concierge_eval,
    concierge_profile,
    config,
    dataset,
    question,
    routed,
    routing_metric,
    sales_eval,
    settled,
    system_metric,
    validate,
)

ORIGIN = Origin("cowork_profiles/concierge/cowork_profile.yml")
SALES = QualifiedName.parse("DB.AGENTS.SALES")
PARTNER = QualifiedName.parse("PARTNER.AGENTS.DELIVERY")


def test_an_agent_subject_keeps_the_agent_eval_key_and_names_no_tools_of_its_own() -> None:
    files = AgentEvalFiles(Origin("agents/sales/agent.yml"), "d", "c", "agents/sales/evals")
    agent = AgentModel("Sales", Origin("agents/sales/agent.yml"), (), folder=("team",), evals=files)
    subject = EvalSubject(agent)
    assert (subject.kind, subject.is_profile, subject.name, subject.key) == ("agent", False, "Sales", "agent:sales")
    assert subject.eval_key == "eval:sales"
    assert (subject.folder, subject.evals, subject.origin) == (("team",), files, agent.origin)
    assert (subject.tool_names, subject.subagents, subject.agent) == (None, (), agent)


def test_a_profile_subject_is_keyed_apart_and_names_its_router_and_tools() -> None:
    tools = (CoworkProfileTool("web_search", None, ORIGIN), CoworkProfileTool("code_execution", "run", ORIGIN))
    profile = concierge_profile(tools=tools)
    subject = EvalSubject(profile)
    assert (subject.kind, subject.is_profile, subject.key) == ("cowork_profile", True, "cowork_profile:concierge")
    assert subject.eval_key == "eval:cowork_profile:concierge"
    assert subject.tool_names == ("router", "web_search", "run")
    assert subject.sample_questions == ("Who sold the most?",)
    assert [item.name for item in subject.subagents] == ["sales", "partner_agent"]
    assert subject.agent is None


def test_a_resolved_eval_reads_its_subject_for_name_key_and_dependency() -> None:
    profile_eval = concierge_eval()
    assert (profile_eval.name, profile_eval.key, profile_eval.agent) == (
        "concierge",
        "eval:cowork_profile:concierge",
        None,
    )
    assert profile_eval.depends_on == ("cowork_profile:concierge",)
    agent_eval = sales_eval()
    assert (agent_eval.key, agent_eval.depends_on) == ("eval:sales", ("agent:sales",))
    assert agent_eval.agent is agent_eval.evaluated


def test_a_compiled_profile_maps_each_resolved_subagent_by_its_call() -> None:
    resolved = ResolvedCoworkProfile(
        CoworkProfileModel("concierge", ORIGIN, ()),
        (
            ResolvedSubagent(SALES, "Sales", "Sells.", depends_on="agent:sales"),
            ResolvedSubagent(PARTNER, "Partner", "Delivers.", external=True, member="Partner_Agent"),
            ResolvedSubagent(QualifiedName.parse("DB.AGENTS.OTHER"), "Other", "Neither."),
        ),
    )
    target = profile_subject_target(QualifiedName.parse("DB.PROFILES.CONCIERGE"), resolved)
    assert target.target.sql == "DB.PROFILES.CONCIERGE"
    assert dict(target.subagents) == {"agent:sales": SALES, "tool:partner_agent": PARTNER}


def test_an_expected_subagent_is_compared_casefolded_by_its_call() -> None:
    assert EvalSubagentRef(ORIGIN, "tool", "Partner_Agent").reference == "tool:partner_agent"


def test_expected_subagents_render_as_their_qualified_names_and_an_empty_list_as_a_decline() -> None:
    rows = concierge_eval(routed("Who sold most?", "agent:sales", "tool:partner_agent"), routed("Suppliers?")).dataset
    payload = json.loads(render_dataset_payload(rows, {"agent:sales": SALES, "tool:partner_agent": PARTNER}))
    assert [row["ground_truth"]["ground_truth_subagents"] for row in payload] == [
        ["DB.AGENTS.SALES", "PARTNER.AGENTS.DELIVERY"],
        [],
    ]
    with pytest.raises(KeyError):
        render_dataset_payload(rows, {"agent:sales": SALES})


def test_an_agent_eval_may_write_no_routing_metric_and_no_expected_subagent() -> None:
    rows = dataset(question(), question(subagents=()), question(subagents=None))
    resolved = sales_eval(eval_dataset=rows, eval_config=config(routing_metrics=(routing_metric(),)))
    assert codes(subject_rules(resolved)) == ["SST-VAL767", "SST-VAL768"]
    assert subject_rules(sales_eval()) == ()


def test_a_row_without_ground_truth_is_skipped_by_the_routing_rules() -> None:
    blank = EvalQuestion(ORIGIN, "Q", None)
    rows = (routed("Who sold most?", "agent:sales"), blank, routed("Suppliers?"))
    assert subject_rules(concierge_eval(*rows)) == ()
    agent_rows = dataset(blank)
    assert subject_rules(sales_eval(eval_dataset=agent_rows)) == ()


def test_a_profile_eval_refuses_blind_metrics_tool_calls_and_strange_subagents() -> None:
    rows = (
        routed("Who sold most?", "agent:ops", invocations=()),
        routed("Suppliers?"),
        routed("Anything?", subagents=None),
    )
    metrics = (system_metric("tool_selection_accuracy"), system_metric("tool_execution_accuracy"))
    found = subject_rules(concierge_eval(*rows, system_metrics=metrics))
    assert codes(found) == ["SST-VAL882", "SST-VAL882", "SST-VAL883", "SST-VAL881"]


@pytest.mark.parametrize(
    ("threshold", "expected"),
    [
        (None, []),
        (ThresholdRange(0.0, 1.0), []),
        (ThresholdRange(min=-0.1), ["SST-VAL769"]),
        (ThresholdRange(max=2.0), ["SST-VAL769"]),
        (ThresholdRange(-1.0, 2.0), ["SST-VAL769", "SST-VAL769"]),
    ],
)
def test_a_routing_threshold_bound_must_lie_in_the_score_range(
    threshold: ThresholdRange | None, expected: list[str]
) -> None:
    metric = routing_metric(gate=threshold is not None, threshold=threshold, name=None if threshold is None else "x")
    assert codes(subject_rules(concierge_eval(routing_metrics=(metric,)))) == expected


@pytest.mark.parametrize(
    ("rows", "expected"),
    [
        ((), ["SST-VAL884"]),
        (("route",), ["SST-VAL885"]),
        (("decline",), []),
        (("route", "decline"), []),
    ],
)
def test_a_profile_dataset_should_route_and_decline(rows: tuple[str, ...], expected: list[str]) -> None:
    built = [routed("Q", "agent:sales") if row == "route" else routed("D") for row in rows]
    resolved = concierge_eval(*built) if built else concierge_eval(routed("Q", subagents=None))
    assert codes(subject_rules(resolved)) == expected


def test_only_a_profile_published_where_cowork_lists_it_is_left_out() -> None:
    listed = QualifiedName.parse("SNOWFLAKE_INTELLIGENCE.PROFILES.CONCIERGE")
    rehearsal = QualifiedName.parse("DB.PROFILES.CONCIERGE")
    assert codes(cowork_listed_placement(concierge_eval(), listed)) == ["SST-VAL886"]
    assert cowork_listed_placement(concierge_eval(), rehearsal) == ()
    assert cowork_listed_placement(sales_eval(), listed) == ()


def test_a_profile_eval_validates_without_tool_checks_and_gates_its_routing_metric() -> None:
    assert [item.code for item in validate(concierge_eval()) if item.blocks] == []
    ungated = routing_metric(gate=False)
    report = codes(validate(concierge_eval(routing_metrics=(ungated,))))
    assert "SST-VAL733" in report
    assert "SST-VAL708" not in codes(validate(concierge_eval(routed("Q", "agent:sales"), routed("D"))))


def test_a_routing_threshold_with_no_baseline_runs_is_reported() -> None:
    resolved = settled(concierge_eval(), baseline_runs=None)
    assert "SST-VAL735" in codes(validate(resolved))
