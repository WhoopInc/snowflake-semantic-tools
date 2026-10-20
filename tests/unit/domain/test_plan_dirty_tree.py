"""A plan that commits agent versions from uncommitted files warns once, naming some of the files."""

from __future__ import annotations

from dataclasses import replace

import pytest

from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag
from snowflake_semantic_tools.domain.model.lifecycle import Action, ChangeSet
from snowflake_semantic_tools.domain.plan.summary import plan_notices
from tests.helpers.artifact_builders import agent_and_eval
from tests.helpers.plan_codes import target


def _plan(*, temporary: bool = False, action: Action = Action.CREATE, adds_version: bool | None = None) -> ChangeSet:
    agent, evaluation = agent_and_eval(temporary=temporary)
    assert agent.rendered is not None
    agent = replace(agent, action=action, rendered=replace(agent.rendered, adds_version=adds_version))
    return ChangeSet("m", target(), (agent, evaluation), DiagnosticBag(), "now")


def _warnings(plan: ChangeSet, uncommitted: tuple[str, ...]) -> list[str]:
    return [item.message for item in plan_notices(plan, uncommitted) if item.code == "SST-PLN042"]


@pytest.mark.parametrize(
    ("count", "named"),
    [(1, "a0.md"), (5, "a0.md, a1.md, a2.md, a3.md, a4.md"), (6, "a0.md, a1.md, a2.md, a3.md, a4.md, and 1 more")],
)
def test_a_dirty_plan_names_at_most_five_paths(count: int, named: str) -> None:
    paths = tuple(f"a{index}.md" for index in range(count))
    assert _warnings(_plan(), paths) == [
        f"the project has uncommitted changes ({named}), so the 1 agent version(s) this plan commits name "
        "their commit as `git:<sha>-dirty`"
    ]


def test_only_a_change_that_commits_an_agent_version_counts() -> None:
    assert len(_warnings(_plan(action=Action.UPDATE, adds_version=True), ("a.md",))) == 1
    assert _warnings(_plan(action=Action.UPDATE, adds_version=False), ("a.md",)) == []
    assert _warnings(_plan(temporary=True), ("a.md",)) == []
    assert _warnings(_plan(), ()) == []
    # A prune carries no rendered artifact, and commits nothing.
    pruned = _plan()
    agent, evaluation = pruned.changes
    pruned = replace(pruned, changes=(replace(agent, action=Action.PRUNE, rendered=None), evaluation))
    assert _warnings(pruned, ("a.md",)) == []
