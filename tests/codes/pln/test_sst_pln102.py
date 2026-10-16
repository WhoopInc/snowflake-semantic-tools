"""SST-PLN102: an otherwise unchanged agent's versions drifted from what SST published.

Fires when the agent's alias was moved off the newest version, which holds the planned
specification; the same agent with its alias on that version stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import DiagnosticBag, Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    DesiredMetadata,
    ObservedArtifact,
    OwnershipMarker,
    PublishShape,
    RenderedArtifact,
    StatementPlan,
    spec_fingerprint,
)
from snowflake_semantic_tools.domain.model.registry import GrantPreservation
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.plan_codes import entry, plan
from tests.helpers.sql_values import statement

SPEC = '{"models": {"orchestration": "auto"}}'
MANIFEST = "a" * 64


def _plan(alias_on: str) -> DiagnosticBag:
    """Plan the recorded, unchanged agent whose alias is on `alias_on`; VERSION$3 holds its spec."""
    agent = RenderedArtifact.create(
        key="agent:sales",
        artifact_type="agent",
        target=QualifiedName.parse("DB.S.SALES"),
        ddl=SPEC,
        shape=PublishShape("AGENT", render_dialect="json", grant_preservation=GrantPreservation.NONE),
        statements=StatementPlan(default=(statement("create"),), update_properties=(statement("set comment"),)),
        metadata=DesiredMetadata(alias="promoted", spec_fingerprint=spec_fingerprint(SPEC)),
    )
    marker = OwnershipMarker(MANIFEST, agent.fingerprint)
    live = ObservedArtifact(
        agent.key,
        "SALES",
        agent.target,
        "AGENT",
        "OWNER",
        "now",
        marker.text,
        marker,
        has_live_version=True,
        newest_version="VERSION$3",
        newest_spec=agent.spec_fingerprint,
        alias_versions=(("PROMOTED", alias_on),),
    )
    return plan((agent,), observed=(live,), applied={agent.key: entry(agent, MANIFEST)}).diagnostics


def test_sst_pln102_fires() -> None:
    diagnostic = only(_plan("VERSION$2"), "SST-PLN102")
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "agent:sales: alias PROMOTED is on VERSION$2, not VERSION$3; apply points it back"


def test_sst_pln102_silent() -> None:
    assert "SST-PLN102" not in codes(_plan("VERSION$3"))
