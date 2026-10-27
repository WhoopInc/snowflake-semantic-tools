"""SST-MAN030: a saved plan's Snowflake observation is past its time to live, so apply ignores it."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.app.apply.observation import stale_observation
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.state import SavedPlan
from tests.helpers.artifact_builders import change, changeset, rendered


def observed_at(moment: str) -> SavedPlan:
    return SavedPlan.from_changeset(replace(changeset(change(rendered())), observation_at=moment))


def test_sst_man030_fires() -> None:
    diagnostic = stale_observation(
        observed_at("2026-10-01T08:00:00Z"), observed_at("2026-10-01T10:05:30Z"), source="plan.json"
    )
    assert diagnostic is not None
    assert (diagnostic.code, diagnostic.severity) == ("SST-MAN030", Severity.WARNING)
    assert diagnostic.message == "observation for plan.json is 2h 05m old; ignored"


def test_sst_man030_silent() -> None:
    fresh = observed_at("2026-10-01T08:00:00Z")
    assert stale_observation(fresh, observed_at("2026-10-01T08:59:00Z"), source="plan.json") is None
    assert stale_observation(observed_at("now"), fresh, source="plan.json") is None
