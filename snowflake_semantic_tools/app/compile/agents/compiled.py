"""A compiled Cortex Agent and the statements that publish it, staged or temporary."""

from __future__ import annotations

from dataclasses import dataclass, replace

from snowflake_semantic_tools.app.compile.base import StandaloneArtifact
from snowflake_semantic_tools.app.compile.staged_agent import (
    AgentObjectMetadata,
    StagedAgentObject,
    external_references,
    render_agent_object,
    staged_path,
)
from snowflake_semantic_tools.domain.model.agent import ResolvedAgent
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    OwnershipMarker,
    RenderedArtifact,
    marked_comment,
    spec_fingerprint,
)
from snowflake_semantic_tools.domain.model.setting import Setting


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
        stage: The stage `stage_path` starts with; None until `for_publication` sets it.
        temporary: Publish as a TEMPORARY agent created from the inline specification,
            staging nothing.
        dirty: The project had uncommitted changes, so each version committed is labelled
            `git:<sha>-dirty`; the stage path still names the sha.
    """

    resolved: ResolvedAgent
    target: QualifiedName
    payload: str
    definition_fingerprint: str
    stage_path: str = ""
    temporary: bool = False
    stage: QualifiedName | None = None
    dirty: bool = False

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
    def settings(self) -> tuple[Setting, ...]:
        settings = self.resolved.model.settings
        return tuple(settings[key] for key in sorted(settings))

    @property
    def spec_fingerprint(self) -> str:
        """The fingerprint of the specification alone, which each version SST commits carries."""
        return spec_fingerprint(self.payload)

    @property
    def rendered_artifact(self) -> RenderedArtifact:
        return render_agent_object(self._staged, None)

    def rendered_for_publish(self, manifest_id: str) -> RenderedArtifact:
        """Render every statement program with the profile, the marked comment, and SECURE set too.

        The comment carrying the ownership marker is written first -- with CREATE AGENT, or
        before an update writes anything else -- so a write that stops part way leaves the
        marker for the definition state records it was writing, which the next plan finishes.
        """
        marker = OwnershipMarker(manifest_id, self.definition_fingerprint)
        rendered = render_agent_object(self._staged, marked_comment(self.resolved.model.comment, marker.text))
        return replace(rendered, expected_marker=marker)

    @property
    def _staged(self) -> StagedAgentObject:
        model = self.resolved.model
        return StagedAgentObject(
            key=self.artifact_key,
            artifact_type="agent",
            target=self.target,
            payload=self.payload,
            definition_fingerprint=self.definition_fingerprint,
            spec_fingerprint=self.spec_fingerprint,
            metadata=AgentObjectMetadata(
                alias=model.alias, tags=model.tags, profile=model.profile, secure=model.secure
            ),
            depends_on=self.resolved.depends_on,
            # The tool members another team owns, which the agent calls but nothing publishes.
            references=external_references([tool.member for tool in self.resolved.tools if tool.external]),
            folder=self.name,
            stage_path=self.stage_path,
            stage=self.stage,
            temporary=self.temporary,
            dirty=self.dirty,
        )


def for_publication(
    compiled: CompiledAgent,
    *,
    stage: QualifiedName,
    git_sha: str,
    temporary: bool = False,
    dirty: bool = False,
) -> CompiledAgent:
    """Return the agent ready to publish: staged under `<stage>/<agent>/<git_sha>`, or temporary.

    Compile leaves an agent without a stage path, so it has no statements; plan and apply
    call this first. A temporary agent keeps the stage path but stages nothing. A `dirty`
    agent's versions are labelled `git:<sha>-dirty`.
    """
    return replace(
        compiled,
        stage_path=staged_path(stage, compiled.name, git_sha),
        temporary=temporary,
        stage=stage,
        dirty=dirty,
    )
