"""SST-MAN026: a plan summarises, per artifact type, what change detection found."""

from __future__ import annotations

from snowflake_semantic_tools.app.state import change_summary
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action
from tests.helpers.artifact_builders import change, changeset, rendered


def test_sst_man026_fires() -> None:
    plan = changeset(
        change(rendered("A")),
        change(rendered("B"), Action.UPDATE),
        change(rendered("C"), Action.NOOP),
        change(rendered("D"), Action.PRUNE),
    )
    [diagnostic] = change_summary(plan)
    assert (diagnostic.code, diagnostic.severity) == ("SST-MAN026", Severity.INFO)
    assert diagnostic.message == "semantic_view: 1 new, 1 modified, 1 unmodified, 1 orphaned"


def test_sst_man026_silent() -> None:
    assert change_summary(changeset()) == ()
