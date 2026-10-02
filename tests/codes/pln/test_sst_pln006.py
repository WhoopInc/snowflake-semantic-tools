"""SST-PLN006: `state:modified` was asked for with no previous manifest, so the plan covers everything."""

from __future__ import annotations

from snowflake_semantic_tools.app.manifest import manifest_for
from snowflake_semantic_tools.app.plan import PlanCandidates, PlanScope, PreparePlan
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.state import Manifest
from tests.helpers.app_ports import FixedClock
from tests.helpers.compile_builders import compiled
from tests.helpers.project_inputs import EMPTY_SOURCES, InMemoryProjectInputs

MODIFIED = PlanScope(("state:modified",), None, None, None, None, False, impact=True)


def _select(previous: Manifest | None) -> PlanCandidates:
    result = compiled("SALES", "MENU")
    candidates = PreparePlan(InMemoryProjectInputs(), FixedClock()).select(
        result,
        manifest_for(result, EMPTY_SOURCES),
        MODIFIED,
        partial=False,
        strict=None,
        connected=False,
        project="the-project",
        previous_manifest=previous,
    )
    assert isinstance(candidates, PlanCandidates)
    return candidates


def test_sst_pln006_fires() -> None:
    candidates = _select(None)
    [diagnostic] = candidates.notices
    assert (diagnostic.code, diagnostic.severity) == ("SST-PLN006", Severity.WARNING)
    assert diagnostic.message == "no previous manifest; a full plan was computed"
    assert len(candidates.selected.compiled) == 2 and candidates.covers_all


def test_sst_pln006_silent() -> None:
    previous = manifest_for(compiled("SALES"), EMPTY_SOURCES)
    candidates = _select(previous)
    assert candidates.notices == ()
    assert [item.artifact_key for item in candidates.selected.compiled] == ["semantic_view:menu"]
    assert not candidates.covers_all
