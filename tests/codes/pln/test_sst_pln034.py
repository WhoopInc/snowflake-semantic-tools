"""SST-PLN034: an artifact state records is no longer declared; its prune is reported, never executed."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import Action, ChangeSet, OwnershipMarker, RenderedArtifact
from snowflake_semantic_tools.domain.state import DEACTIVATED
from tests.helpers.artifact_builders import empty_state
from tests.helpers.diagnostic_filters import coded
from tests.helpers.lifecycle_codes import month_close, plan_skills, publish_skills
from tests.helpers.plan_codes import entry, live, plan
from tests.helpers.snowflake_fake import FakeSnowflake
from tests.helpers.sql_values import statement

MANIFEST = "a" * 64


def _planned(*, declared: bool) -> list[Diagnostic]:
    compiled = month_close()
    port = FakeSnowflake(existing=())
    published = publish_skills(port, compiled, empty_state())
    changeset = plan_skills(port, compiled if declared else {}, published, prune=True)
    if not declared:
        assert [(change.action, change.prune_executable) for change in changeset.changes] == [(Action.PRUNE, False)]
    return [item for item in changeset.diagnostics if item.code == "SST-PLN034"]


def test_sst_pln034_fires() -> None:
    [diagnostic] = _planned(declared=False)
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message.startswith("skill:month-close is no longer declared; SST never removes ")


def test_sst_pln034_silent() -> None:
    assert _planned(declared=True) == []


def _stage() -> RenderedArtifact:
    """A stage tool as apply published it; the project no longer declares it."""
    return RenderedArtifact.create(
        key="tool:exports",
        artifact_type="tool",
        target=QualifiedName.from_parts("db", "tools", "exports"),
        ddl=statement("create stage exports"),
    )


def _deleted_tool(
    *, observed: bool = True, outcome: str = "applied", prune_types: frozenset[str] | None = None
) -> ChangeSet:
    stage = _stage()
    seen = live(stage, marker=OwnershipMarker(MANIFEST, stage.fingerprint), object_type="STAGE")
    recorded = replace(entry(stage, MANIFEST), outcome=outcome)
    return plan(
        (),
        observed=(seen,) if observed else (),
        applied={stage.key: recorded},
        include_prune=True,
        prune_types=prune_types,
    )


def test_sst_pln034_fires_for_a_deleted_tool() -> None:
    # A tool is never dropped, so its prune is reported instead of left out of the plan.
    planned = _deleted_tool()
    [change] = planned.changes
    assert (change.key, change.action, change.prune_executable) == ("tool:exports", Action.PRUNE, False)
    assert change.observed is not None and change.observed.qualified_name.sql == "DB.TOOLS.EXPORTS"
    assert planned.report_only == (change,)
    [diagnostic] = coded(planned.diagnostics, "SST-PLN034")
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message.startswith("tool:exports is no longer declared; SST never removes DB.TOOLS.EXPORTS: ")
    assert "drop it by hand" in diagnostic.message
    assert coded(planned.diagnostics, "SST-PLN021") == []


def test_sst_pln034_fires_for_a_deleted_tool_dropped_by_hand() -> None:
    # State still records the tool, so it is still reported, without a live object.
    [change] = _deleted_tool(observed=False).changes
    assert (change.key, change.prune_executable, change.observed) == ("tool:exports", False, None)


def test_sst_pln034_silent_for_a_tool_outside_the_prune_or_retired() -> None:
    assert _deleted_tool(prune_types=frozenset(("semantic_view",))).changes == ()
    assert _deleted_tool(outcome=DEACTIVATED).changes == ()
    # Without --prune, nothing is planned for it at all.
    stage = _stage()
    assert plan((), applied={stage.key: entry(stage, MANIFEST)}).changes == ()
