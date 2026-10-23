"""SST-VAL014: an agent spec renders keys SST does not model, with `snowflake.allow_unknown_keys` true."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.shared import unmodeled_key_diagnostics

KEYS = ("spec.passthrough.budget", "tools.search.tool_spec_passthrough.mode")


def test_sst_val014_fires() -> None:
    found = unmodeled_key_diagnostics("agent", "analyst", KEYS, allow=True, subject="agent:analyst")
    assert [item.code for item in found] == ["SST-VAL014"]
    assert found[0].severity is Severity.WARNING
    assert found[0].message == "agent 'analyst': 2 unmodeled keys rendered"
    assert found[0].subject == "agent:analyst"


def test_sst_val014_silent() -> None:
    assert unmodeled_key_diagnostics("agent", "analyst", (), allow=True, subject="agent:analyst") == ()
