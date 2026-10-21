"""A compiled Snowflake CoWork profile and the statements that publish it as a staged AGENT.

CoWork profiles are a Snowflake private preview (PrPr) feature; a profile publishes as the
preview's contract describes, on these pages:
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/set-up-cowork-profile-routing

A profile is an AGENT object, so it publishes through the agent's own programs
(`staged_agent`); it is not a `CompiledAgent`, so nothing that reads agents reads profiles. Its
versions are staged under a folder of their own, so a profile can never share an agent's. A
profile published where CoWork lists profiles, with `attach` on, also wants attaching to the
account's CoWork object, which plan adds once it has read that object (`domain.plan.attach`).
"""

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
from snowflake_semantic_tools.domain.model.cowork_profile import ResolvedCoworkProfile, listed_by_cowork
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    OwnershipMarker,
    RenderedArtifact,
    marked_comment,
    spec_fingerprint,
)
from snowflake_semantic_tools.domain.model.setting import Setting

# The stage folder every profile's versions are staged under, one subfolder per profile.
COWORK_STAGE_FOLDER = "cowork-profiles"


@dataclass(frozen=True, slots=True)
class CompiledCoworkProfile(StandaloneArtifact):
    """One CoWork profile whose subagents and skills resolved, with its rendered specification.

    Attributes:
        payload: The specification JSON that is staged.
        definition_fingerprint: The SHA-256 of the whole definition -- the specification with
            the comment, display block, alias, tags, `enabled` and `meta` -- which replaces the
            payload's own fingerprint, so a change to any of them is a change.
        stage_path: `<stage>/cowork-profiles/<profile>/<git sha>`, where the specification is
            staged; empty until `for_cowork_publication` sets it, and a profile without one has
            metadata statements only.
        stage: The stage `stage_path` starts with; None until `for_cowork_publication` sets it.
        dirty: The project had uncommitted changes, so each version committed is labelled
            `git:<sha>-dirty`; the stage path still names the sha.
    """

    resolved: ResolvedCoworkProfile
    target: QualifiedName
    payload: str
    definition_fingerprint: str
    stage_path: str = ""
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
        return "cowork_profile"

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
    def folder(self) -> str:
        """The stage folder this profile's versions are staged in: `cowork-profiles/<profile>`."""
        return f"{COWORK_STAGE_FOLDER}/{self.name}"

    @property
    def rendered_artifact(self) -> RenderedArtifact:
        return self._attached(render_agent_object(self._staged, None))

    def rendered_for_publish(self, manifest_id: str) -> RenderedArtifact:
        """Render every statement program with the display block, the marked comment, and SECURE.

        As for an agent, the comment carrying the ownership marker is written first, so a write
        that stops part way leaves the marker for the definition it was writing.
        """
        marker = OwnershipMarker(manifest_id, self.definition_fingerprint)
        rendered = render_agent_object(self._staged, marked_comment(self.resolved.model.comment, marker.text))
        return self._attached(replace(rendered, expected_marker=marker))

    @property
    def wants_attachment(self) -> bool:
        """Whether plan attaches the profile to the CoWork object: `attach` is on, and CoWork lists it.

        A rehearsal, published anywhere but SNOWFLAKE_INTELLIGENCE.PROFILES, is never attached.
        """
        # Snowflake private preview (PrPr): CoWork profiles
        return self.resolved.model.attach and listed_by_cowork(self.target)

    def _attached(self, rendered: RenderedArtifact) -> RenderedArtifact:
        return replace(rendered, wants_attachment=self.wants_attachment)

    @property
    def _staged(self) -> StagedAgentObject:
        model = self.resolved.model
        return StagedAgentObject(
            key=self.artifact_key,
            artifact_type="cowork_profile",
            target=self.target,
            payload=self.payload,
            definition_fingerprint=self.definition_fingerprint,
            spec_fingerprint=self.spec_fingerprint,
            # A profile sets no SECURE of its own, so it publishes `SECURE = FALSE`.
            metadata=AgentObjectMetadata(alias=model.alias, tags=model.tags, profile=model.display, secure=None),
            depends_on=self.resolved.depends_on,
            # The `agent` tool members another team owns, which the profile routes to.
            references=external_references(
                [subagent.member for subagent in self.resolved.subagents if subagent.external]
            ),
            folder=self.folder,
            stage_path=self.stage_path,
            stage=self.stage,
            dirty=self.dirty,
        )


def for_cowork_publication(
    compiled: CompiledCoworkProfile, *, stage: QualifiedName, git_sha: str, dirty: bool = False
) -> CompiledCoworkProfile:
    """Return the profile ready to publish, staged under `<stage>/cowork-profiles/<profile>/<git_sha>`.

    Compile leaves a profile without a stage path, so it has metadata statements only; plan
    and apply call this first. A `dirty` profile's versions are labelled `git:<sha>-dirty`.
    """
    return replace(
        compiled,
        stage_path=staged_path(stage, compiled.folder, git_sha),
        stage=stage,
        dirty=dirty,
    )
