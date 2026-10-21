"""CoWork profiles (a Snowflake private preview) as plan renders them, attached or not to CoWork.

Shared by the attach tests of the domain, app and per-code PLN layers. A profile here is an AGENT
with the programs a staged agent has, published where CoWork lists profiles unless a test says
otherwise, so it wants attaching to `COWORK`, the account's CoWork object.
"""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    Change,
    ChangeReason,
    DesiredMetadata,
    ObservedArtifact,
    OwnershipMarker,
    PublishShape,
    RenderedArtifact,
    StatementPlan,
    spec_fingerprint,
)
from snowflake_semantic_tools.domain.model.registry import GrantPreservation
from snowflake_semantic_tools.domain.plan.attach import AttachmentReading
from tests.helpers.sql_values import statement

# The CoWork object an enrolled account lists, as SHOW SNOWFLAKE INTELLIGENCES names it.
COWORK = "SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT"
SPEC = '{"models": {"orchestration": "auto"}}'
MANIFEST = "a" * 64


def profile(name: str = "everyone", *, schema: str = "PROFILES", wants: bool = True) -> RenderedArtifact:
    """The profile `name` in SNOWFLAKE_INTELLIGENCE.`schema`, wanting attaching unless `wants` is off."""
    artifact = RenderedArtifact.create(
        key=f"cowork_profile:{name}",
        artifact_type="cowork_profile",
        target=QualifiedName.from_parts("SNOWFLAKE_INTELLIGENCE", schema, name),
        ddl=SPEC,
        shape=PublishShape("AGENT", render_dialect="json", grant_preservation=GrantPreservation.NONE),
        statements=StatementPlan(
            default=(statement("create agent"),),
            create=(statement("create agent"), statement("commit version")),
            update=(statement("add version"),),
            update_properties=(statement("set comment"),),
            # Published, a profile's properties program leads with its marked comment.
            properties_lead=1,
        ),
        metadata=DesiredMetadata(spec_fingerprint=spec_fingerprint(SPEC)),
    )
    return replace(artifact, wants_attachment=wants, upload_path="@DB.S.SPECS/x", upload_content=b"x")


def live(artifact: RenderedArtifact) -> ObservedArtifact:
    """The profile as SST published it: marked, with a live version holding its specification."""
    marker = OwnershipMarker(MANIFEST, artifact.fingerprint)
    return ObservedArtifact(
        artifact.key,
        artifact.target.name.folded,
        artifact.target,
        "AGENT",
        "OWNER",
        "now",
        marker.text,
        marker,
        has_live_version=True,
        newest_version="VERSION$1",
        newest_spec=artifact.spec_fingerprint,
    )


def reading(*attached: RenderedArtifact, cowork_object: str | None = COWORK) -> AttachmentReading:
    """What plan reads of an account whose CoWork object has `attached` attached."""
    return AttachmentReading(cowork_object, frozenset(item.target.folded for item in attached))


def planned(artifact: RenderedArtifact, action: Action, reason: ChangeReason) -> Change:
    """The change classify decides for the profile: its statements narrowed to `action`."""
    observed = None if action is Action.CREATE else live(artifact)
    effective = artifact.for_action(action, observed)
    return Change(artifact.key, artifact.artifact_type, action, reason, effective, observed, (), 65)
