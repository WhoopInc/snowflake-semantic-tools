"""SST-PLN018: observing and planning took longer than the observation stays current."""

from __future__ import annotations

from snowflake_semantic_tools.app.plan import OBSERVATION_TTL_MS, stale_observation
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.project_inputs import dev_target


def test_sst_pln018_fires() -> None:
    diagnostic = stale_observation(dev_target(), OBSERVATION_TTL_MS + 1000)
    assert diagnostic is not None
    assert (diagnostic.code, diagnostic.severity) == ("SST-PLN018", Severity.ERROR)
    assert diagnostic.message == "observation for target 'dev' is 901s old, past the 900s it stays current"


def test_sst_pln018_silent() -> None:
    assert stale_observation(dev_target(), OBSERVATION_TTL_MS) is None
