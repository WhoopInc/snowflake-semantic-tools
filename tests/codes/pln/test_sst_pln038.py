"""SST-PLN038: a write would publish before a dependency the selection leaves out and Snowflake lacks."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.plan_artifacts import PlanArtifacts
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action, ShowRow
from snowflake_semantic_tools.domain.model.registry import ARTIFACT_REGISTRY
from snowflake_semantic_tools.domain.plan.prune import block_unpublished_dependencies
from tests.helpers.agent_stage import staged_agent
from tests.helpers.artifact_builders import change, manifest, rendered, state, target
from tests.helpers.snowflake_fake import FakeSnowflake

DEPENDENT = change(rendered("A", depends_on=("semantic_view:b",)))


def test_sst_pln038_fires() -> None:
    blocked, [found] = block_unpublished_dependencies((DEPENDENT,), (), {"semantic_view:b"}, ARTIFACT_REGISTRY)
    assert found.severity is Severity.ERROR
    assert found.phase == "pln"
    assert found.message == "semantic_view 'a' would publish before semantic_view:b, which it depends on"
    assert found.subject == "semantic_view:a"
    assert blocked[0].action is Action.BLOCKED


def test_sst_pln038_silent() -> None:
    # The dependency exists, so publishing the dependent alone is safe.
    kept, found = block_unpublished_dependencies(
        (DEPENDENT,), {"semantic_view:b"}, {"semantic_view:b"}, ARTIFACT_REGISTRY
    )
    assert found == () and kept[0].action is Action.CREATE


def test_sst_pln038_silent_for_a_selected_agent_whose_view_is_live() -> None:
    # Planning the agent alone still observes views, so the live one it depends on is seen.
    sales = rendered("SALES")
    agent = replace(staged_agent("ANALYST"), depends_on=(sales.key,))
    port = FakeSnowflake()
    port.show(ShowRow("SALES", "DB", "SCHEMA", "OWNER", "now"))
    planned = PlanArtifacts(port).run(
        {agent.key: agent},
        manifest({sales.key: sales, agent.key: agent}),
        state(),
        target(),
        fetched_at="now",
        observation_targets=(agent.target, sales.target),
    )
    assert "SST-PLN038" not in [item.code for item in planned.diagnostics]
    assert [change.action for change in planned.changes] == [Action.CREATE]
