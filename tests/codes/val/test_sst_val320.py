"""SST-VAL320: plan would compare a recorded definition that was never normalised."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag, Severity
from snowflake_semantic_tools.domain.model.lifecycle import Action
from snowflake_semantic_tools.domain.model.registry import ARTIFACT_REGISTRY
from snowflake_semantic_tools.domain.plan.classify import Decision, classify
from snowflake_semantic_tools.domain.state import AppliedEntry
from tests.helpers.artifact_builders import marker, observed, rendered

ARTIFACT = rendered("V")


def _decide(fingerprint: str) -> Decision:
    recorded = AppliedEntry(
        fingerprint=fingerprint,
        qualified_name=ARTIFACT.target.sql,
        applied_at="now",
        run_id="run",
        outcome="APPLIED",
        ddl_sha256=fingerprint,
        manifest_id="a" * 64,
    )
    return classify(
        ARTIFACT.key,
        ARTIFACT,
        ARTIFACT_REGISTRY,
        observed=observed(ARTIFACT, ownership=marker(ARTIFACT)),
        recorded=recorded,
        manifest_id="a" * 64,
        validation=DiagnosticBag(),
        composite=None,
    )


def test_sst_val320_fires() -> None:
    # A state row hand-edited to hold the DDL itself rather than its digest.
    decision = _decide("CREATE OR REPLACE SEMANTIC VIEW DB.SCHEMA.V COPY GRANTS")
    [found] = decision.diagnostics
    assert found.severity is Severity.ERROR
    assert found.message == "semantic_view:v: drift comparison compared raw DDL"
    assert decision.change.action is Action.BLOCKED


def test_sst_val320_silent() -> None:
    decision = _decide(ARTIFACT.fingerprint)
    assert [item.code for item in decision.diagnostics] == []
