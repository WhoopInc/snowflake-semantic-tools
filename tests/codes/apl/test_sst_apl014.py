"""SST-APL014: a temporary artifact took the name of a permanent object for its session."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.lifecycle import ApplyOptions
from tests.helpers.apply_runs import apply_plan
from tests.helpers.artifact_builders import change, changeset, marker, observed, rendered
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.snowflake_fake import FakeSnowflake

TEMPORARY = ApplyOptions(temporary=True)


def test_sst_apl014_fires() -> None:
    artifact = replace(rendered(), temporary=True)
    port = FakeSnowflake()
    result = apply_plan(
        changeset(change(artifact, live=observed(artifact, ownership=marker(artifact)))), port, options=TEMPORARY
    )
    diagnostic = only(result.diagnostics, "SST-APL014")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "semantic_view:v shadows a permanent object of the same name for this session"
    # The temporary object lasts for its session only: state keeps the permanent object plan saw.
    recorded = (port.remote_state or {})[artifact.key]
    assert (recorded.outcome, recorded.fingerprint) == ("observed", marker(artifact).fingerprint)


def test_sst_apl014_silent() -> None:
    artifact = replace(rendered(), temporary=True)
    assert "SST-APL014" not in codes(apply_plan(changeset(change(artifact)), options=TEMPORARY).diagnostics)
