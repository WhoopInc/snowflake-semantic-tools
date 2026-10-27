"""SST-PLN042: the plan commits an agent version while the project has uncommitted changes."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag, Severity, resolve_severities
from snowflake_semantic_tools.domain.model.lifecycle import Action, ChangeReason, ChangeSet
from snowflake_semantic_tools.domain.plan.summary import plan_notices
from tests.helpers.artifact_builders import agent_and_eval
from tests.helpers.plan_codes import target

_PATHS = tuple(f"agents/sales/file_{index}.md" for index in range(7))


def _plan(*, temporary: bool = False, action: Action = Action.CREATE, adds_version: bool | None = None) -> ChangeSet:
    agent, evaluation = agent_and_eval(temporary=temporary)
    rendered = replace(agent.rendered, adds_version=adds_version) if agent.rendered is not None else None
    agent = replace(agent, action=action, reason=ChangeReason.NOT_PRESENT, rendered=rendered)
    return ChangeSet("m", target(), (agent, evaluation), DiagnosticBag(), "now")


def test_sst_pln042_fires() -> None:
    [diagnostic] = [item for item in plan_notices(_plan(), _PATHS) if item.code == "SST-PLN042"]
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == (
        "the project has uncommitted changes (agents/sales/file_0.md, agents/sales/file_1.md, "
        "agents/sales/file_2.md, agents/sales/file_3.md, agents/sales/file_4.md, and 2 more), so the 1 "
        "agent version(s) this plan commits name their commit as `git:<sha>-dirty`"
    )
    assert diagnostic.subject is None
    # An update that adds a version is counted as a create is.
    [update] = [
        item
        for item in plan_notices(_plan(action=Action.UPDATE, adds_version=True), _PATHS[:1])
        if item.code == "SST-PLN042"
    ]
    assert "(agents/sales/file_0.md), so the 1 agent" in update.message
    # Strict mode leaves it a warning: editing a project means having uncommitted files.
    promoted, count = resolve_severities(DiagnosticBag((diagnostic,)), strict=True)
    assert ([item.severity for item in promoted], count) == ([Severity.WARNING], 0)


def test_sst_pln042_silent() -> None:
    def codes(plan: ChangeSet, uncommitted: tuple[str, ...] = _PATHS) -> list[str]:
        return [item.code for item in plan_notices(plan, uncommitted) if item.code == "SST-PLN042"]

    # A clean work tree.
    assert codes(_plan(), ()) == []
    # A temporary agent commits no version, nor does an update that only moves its properties.
    assert codes(_plan(temporary=True)) == []
    assert codes(_plan(action=Action.UPDATE, adds_version=False)) == []
    # A plan without changes says nothing at all.
    assert plan_notices(ChangeSet("m", target(), (), DiagnosticBag(), "now"), _PATHS) == ()
