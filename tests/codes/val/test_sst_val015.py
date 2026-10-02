"""SST-VAL015: a write would publish before a dependency the selection leaves out and Snowflake lacks."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action
from snowflake_semantic_tools.domain.model.registry import ARTIFACT_REGISTRY
from snowflake_semantic_tools.domain.plan.prune import block_unpublished_dependencies
from tests.helpers.artifact_builders import change, rendered

DEPENDENT = change(rendered("A", depends_on=("semantic_view:b",)))


def test_sst_val015_fires() -> None:
    blocked, [found] = block_unpublished_dependencies((DEPENDENT,), (), {"semantic_view:b"}, ARTIFACT_REGISTRY)
    assert found.severity is Severity.ERROR
    assert found.message == "semantic_view 'a' would publish before semantic_view:b, which it depends on"
    assert found.subject == "semantic_view:a"
    assert blocked[0].action is Action.BLOCKED


def test_sst_val015_silent() -> None:
    # The dependency exists, so publishing the dependent alone is safe.
    kept, found = block_unpublished_dependencies(
        (DEPENDENT,), {"semantic_view:b"}, {"semantic_view:b"}, ARTIFACT_REGISTRY
    )
    assert found == () and kept[0].action is Action.CREATE
