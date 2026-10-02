"""SST-APL900: apply's outcomes do not account for every planned change.

A property check runs random plans, dependency graphs, failures and policies through apply
and shows the count always matches; the `_fires` test feeds the check a short count.
"""

from __future__ import annotations

from collections.abc import Sequence

from hypothesis import given, settings
from hypothesis import strategies as st

from snowflake_semantic_tools.app.apply.run import _unaccounted
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import ApplyOptions, ExecResult, ExecutionError, FailurePolicy
from snowflake_semantic_tools.domain.sql import Sql
from tests.helpers.app_ports import InMemorySnowflake
from tests.helpers.apply_runs import apply_plan, codes
from tests.helpers.artifact_builders import change, changeset, rendered


class FailingNames(InMemorySnowflake):
    def __init__(self, failing: frozenset[str]) -> None:
        super().__init__()
        self.failing = failing

    def execute_script(self, statements: Sequence[Sql]) -> ExecResult:
        text = " ".join(str(statement) for statement in statements)
        if any(f"DB.SCHEMA.{name} " in text for name in self.failing):
            return ExecResult(False, error=ExecutionError("refused"))
        return super().execute_script(statements)


@st.composite
def plans(draw: st.DrawFn) -> tuple[list[tuple[str, tuple[str, ...]]], frozenset[str], FailurePolicy]:
    count = draw(st.integers(min_value=1, max_value=6))
    names = [f"V{index}" for index in range(count)]
    graph = [
        (name, tuple(sorted(draw(st.sets(st.sampled_from(names[:index])) if index else st.just(set())))))
        for index, name in enumerate(names)
    ]
    failing = frozenset(draw(st.sets(st.sampled_from(names))))
    return graph, failing, draw(st.sampled_from(list(FailurePolicy)))


def test_sst_apl900_fires() -> None:
    plan = changeset(change(rendered("A")), change(rendered("B")))
    [diagnostic] = _unaccounted(plan, ())
    assert (diagnostic.code, diagnostic.severity) == ("SST-APL900", Severity.ERROR)
    assert diagnostic.message == "applied 0 outcomes for 2 changes"


def test_sst_apl900_silent() -> None:
    assert "SST-APL900" not in codes(apply_plan(changeset(change(rendered()))))


@settings(max_examples=60, deadline=None)
@given(plans())
def test_sst_apl900_never_fires_for_any_plan_policy_or_failure(
    drawn: tuple[list[tuple[str, tuple[str, ...]]], frozenset[str], FailurePolicy],
) -> None:
    graph, failing, policy = drawn
    artifacts = {
        name: rendered(name, depends_on=tuple(f"semantic_view:{item.casefold()}" for item in deps))
        for name, deps in graph
    }
    plan = changeset(*(change(artifact) for artifact in artifacts.values()))
    result = apply_plan(plan, FailingNames(failing), options=ApplyOptions(on_failure=policy, parallelism=3))
    assert "SST-APL900" not in codes(result)
    assert sorted(outcome.key for outcome in result.outcomes) == sorted(item.key for item in plan.changes)
