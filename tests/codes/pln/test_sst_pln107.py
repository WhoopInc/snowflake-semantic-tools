"""SST-PLN107: an otherwise unchanged agent's default follows LAST, which apply pins by name.

Fires when DESCRIBE AGENT reads the default as `LAST` while it serves the version holding the
planned specification; the same agent whose default is pinned there by name stays quiet.
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


def _plan(*, pinned: bool) -> DiagnosticBag:
    """Plan the recorded, unchanged agent whose default is VERSION$3, which holds its spec."""
    agent = RenderedArtifact.create(
        key="agent:sales",
        artifact_type="agent",
        target=QualifiedName.parse("DB.S.SALES"),
        ddl=SPEC,
        shape=PublishShape("AGENT", render_dialect="json", grant_preservation=GrantPreservation.NONE),
        statements=StatementPlan(default=(statement("create"),), update_properties=(statement("set comment"),)),
        metadata=DesiredMetadata(spec_fingerprint=spec_fingerprint(SPEC)),
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
        default_version="VERSION$3",
        default_pinned=pinned,
    )
    return plan((agent,), observed=(live,), applied={agent.key: entry(agent, MANIFEST)}).diagnostics


def test_sst_pln107_fires() -> None:
    diagnostic = only(_plan(pinned=False), "SST-PLN107")
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == "agent:sales: the default version follows LAST; apply pins it to VERSION$3 by name"
    assert diagnostic.subject == "agent:sales"


def test_sst_pln107_silent() -> None:
    assert "SST-PLN107" not in codes(_plan(pinned=True))
