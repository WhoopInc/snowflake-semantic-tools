"""A compiled Cortex Agent and the statements that publish it, staged or temporary."""

from __future__ import annotations

import json
from dataclasses import dataclass, replace

from ....domain.model.agent import AgentModel, ResolvedAgent
from ....domain.model.identifier import Identifier, QualifiedName
from ....domain.model.lifecycle import (
    DesiredMetadata,
    OwnershipMarker,
    ProbeKind,
    PublishShape,
    RenderedArtifact,
    SmokeProbe,
    StatementPlan,
    Upload,
)
from ....domain.model.registry import GrantPreservation
from ....domain.model.sql import string_literal
from ..base import StandaloneArtifact


@dataclass(frozen=True, slots=True)
class CompiledAgent(StandaloneArtifact):
    """One agent whose tools and skills resolved, with its rendered specification.

    Attributes:
        payload: The specification JSON that is staged, or inlined for a temporary agent.
        definition_fingerprint: The SHA-256 of the whole definition -- the specification with
            the comment, profile, alias, tags, `secure`, `enabled` and `meta` -- which replaces
            the payload's own fingerprint, so a change to any of them is a change.
        stage_path: `<stage>/<agent>/<git sha>`, where the specification is staged; empty
            until `for_publication` sets it, and an agent without one has no statements.
        temporary: Publish as a TEMPORARY agent created from the inline specification,
            staging nothing.
    """

    resolved: ResolvedAgent
    target: QualifiedName
    payload: str
    definition_fingerprint: str
    stage_path: str = ""
    temporary: bool = False

    @property
    def name(self) -> str:
        return str(self.resolved.model.name)

    @property
    def artifact_key(self) -> str:
        return str(self.resolved.model.key)

    @property
    def artifact_type(self) -> str:
        return "agent"

    @property
    def source_files(self) -> tuple[str, ...]:
        return tuple(self.resolved.model.source_files)

    @property
    def rendered_artifact(self) -> RenderedArtifact:
        create, update, update_live = _agent_programs(
            self.resolved.model,
            self.target,
            self.stage_path,
            temporary=self.temporary,
            payload=self.payload,
        )
        artifact = RenderedArtifact.create(
            key=self.artifact_key,
            artifact_type="agent",
            target=self.target,
            ddl=self.payload,
            shape=PublishShape(
                "AGENT", render_dialect="json", grant_preservation=GrantPreservation.NONE, temporary=self.temporary
            ),
            statements=StatementPlan(default=create, create=create, update=update, update_live=update_live),
            upload=(
                None
                if self.temporary
                else Upload(f"@{self.stage_path.rstrip('/')}/agent_spec.yaml", self.payload.encode("utf-8"))
            ),
            metadata=DesiredMetadata(
                alias=self.resolved.model.alias, tags=tuple(name for name, _ in self.resolved.model.tags)
            ),
            depends_on=self.resolved.depends_on,
            smoke=(
                SmokeProbe(
                    f"{self.artifact_key}:describe",
                    ProbeKind.DESCRIBE,
                    f"DESCRIBE AGENT {self.target.sql}",
                ),
            ),
        )
        return replace(artifact, fingerprint=self.definition_fingerprint)

    def rendered_for_publish(self, manifest_id: str) -> RenderedArtifact:
        """Follow every statement program with the ALTERs that set the profile, marked comment, and SECURE."""
        artifact = self.rendered_artifact
        expected_marker = OwnershipMarker(manifest_id, artifact.fingerprint)
        metadata = _agent_metadata_statements(self.resolved.model, self.target, expected_marker.text)
        return replace(
            artifact,
            statements=(*artifact.statements, *metadata),
            create_statements=(*artifact.create_statements, *metadata),
            update_statements=(*artifact.update_statements, *metadata),
            update_live_statements=(*artifact.update_live_statements, *metadata),
            expected_marker=expected_marker,
        )


def for_publication(
    compiled: CompiledAgent,
    *,
    stage: QualifiedName,
    git_sha: str,
    temporary: bool = False,
) -> CompiledAgent:
    """Return the agent ready to publish: staged under `<stage>/<agent>/<git_sha>`, or temporary.

    Compile leaves an agent without a stage path, so it has no statements; plan and apply
    call this first. A temporary agent keeps the stage path but stages nothing.
    """
    return replace(
        compiled,
        stage_path=f"{stage.sql}/{compiled.name}/{git_sha}",
        temporary=temporary,
    )


def _agent_programs(
    model: AgentModel,
    target: QualifiedName,
    stage_path: str,
    *,
    temporary: bool,
    payload: str,
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    """Return the create, update, and update-live statement programs for an agent.

    A temporary agent is one CREATE OR REPLACE TEMPORARY AGENT from the inline
    specification for all three. A staged agent is created from its stage path, or gets a
    version added from it -- after COMMIT when a live version exists -- and then its alias
    and tags are set. An agent with no stage path has no statements.

    Raises:
        ValueError: a temporary agent's specification holds `$$`, which would end the
            dollar-quoted literal it is inlined in.
    """
    if temporary:
        if "$$" in payload:
            raise ValueError("temporary agent spec contains an unsupported dollar-quote delimiter")
        profile = _profile_json(model)
        statement = (
            f"CREATE OR REPLACE TEMPORARY AGENT {target.sql}"
            + (f" WITH PROFILE = {string_literal(profile)}" if profile else "")
            + f" FROM SPECIFICATION $${payload.rstrip()}$$"
        )
        return (statement,), (statement,), (statement,)
    if not stage_path:
        return (), (), ()
    create = [f"CREATE AGENT {target.sql}\n  FROM @{stage_path}/"]
    add_version = (
        f"ALTER AGENT {target.sql}\n  ADD VERSION FROM @{stage_path}/\n"
        f"  COMMENT = 'git:{stage_path.rsplit('/', 1)[-1]}'"
    )
    update = [add_version]
    update_live = [f"ALTER AGENT {target.sql} COMMIT", add_version]
    if model.alias:
        alias = f'ALTER AGENT {target.sql}\n  MODIFY VERSION "LAST" SET ALIAS = {_identifier(model.alias)}'
        create.append(alias)
        update.append(alias)
        update_live.append(alias)
    if model.tags:
        pairs = ", ".join(f"{_qualified_or_identifier(name)} = {string_literal(value)}" for name, value in model.tags)
        tag = f"ALTER AGENT {target.sql}\n  SET TAG {pairs}"
        create.append(tag)
        update.append(tag)
        update_live.append(tag)
    return tuple(create), tuple(update), tuple(update_live)


def _profile_json(model: AgentModel) -> str:
    value = {
        key: item
        for key, item in (
            ("display_name", model.profile.display_name),
            ("avatar", model.profile.avatar),
            ("color", model.profile.color),
        )
        if item is not None
    }
    return json.dumps(value, separators=(",", ":"))


def _agent_metadata_statements(
    model: AgentModel,
    target: QualifiedName,
    marker: str,
) -> tuple[str, ...]:
    statements: list[str] = []
    profile = _profile_json(model)
    statements.append(f"ALTER AGENT {target.sql} SET PROFILE = {string_literal(profile)}")
    comment = f"{marker} {model.comment}" if model.comment else marker
    statements.append(f"ALTER AGENT {target.sql} SET COMMENT = {string_literal(comment)}")
    statements.append(f"ALTER AGENT {target.sql} SET SECURE = {'TRUE' if model.secure else 'FALSE'}")
    return tuple(statements)


def _identifier(value: str) -> str:
    return Identifier.parse(value).sql


def _qualified_or_identifier(value: str) -> str:
    try:
        return QualifiedName.parse(value).sql
    except ValueError:
        return _identifier(value)
