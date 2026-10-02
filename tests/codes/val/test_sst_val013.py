"""SST-VAL013: an agent spec renders keys SST does not model, with `snowflake.allow_unknown_keys` false."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.validate.shared import unmodelled_key_diagnostics

KEYS = ("spec.passthrough.budget", "tools.search.tool_spec_passthrough.mode")


def test_sst_val013_fires() -> None:
    found = unmodelled_key_diagnostics("agent", "analyst", KEYS, allow=False, subject="agent:analyst")
    assert [item.code for item in found] == ["SST-VAL013", "SST-VAL013"]
    assert found[0].severity is Severity.ERROR
    assert found[0].message == "agent 'analyst': 'spec.passthrough.budget' is not modelled and would be rendered as-is"
    assert found[0].subject == "agent:analyst"


def test_sst_val013_silent() -> None:
    assert unmodelled_key_diagnostics("agent", "analyst", (), allow=False, subject="agent:analyst") == ()
