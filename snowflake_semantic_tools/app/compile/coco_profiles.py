"""Compile Cortex Code agent profiles, and the statements that publish each as a staged AGENT.

Cortex Code agent profiles are a Snowflake private preview (PrPr) feature. A profile is an AGENT
object, so it publishes through the agent's own programs (`staged_agent`): created once from a
staged specification, a committed version and a live version per change, dropped on prune.
Its skills, its own and the shared layer's, are pinned to this project's published extensions
by the code that pins an agent's, and it publishes no alias, tags, display block, or SECURE.
Its versions are staged under a folder of their own, so it never shares an agent's.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from hashlib import sha256

from snowflake_semantic_tools.app.compile.agents.context import AgentCompileContext
from snowflake_semantic_tools.app.compile.agents.resolve_skills import resolve_skills
from snowflake_semantic_tools.app.compile.base import CompileResult, StandaloneArtifact, has_error
from snowflake_semantic_tools.app.compile.staged_agent import (
    AgentObjectMetadata,
    StagedAgentObject,
    render_agent_object,
    staged_path,
)
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.agent import AgentProfile
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.coco_profile import (
    CocoProfileCatalog,
    CocoProfileModel,
    ResolvedCocoProfile,
    owned_entries,
)
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    OwnershipMarker,
    RenderedArtifact,
    marked_comment,
    spec_fingerprint,
)
from snowflake_semantic_tools.domain.model.setting import Setting
from snowflake_semantic_tools.domain.render.coco_profile import (
    assemble_instructions,
    desired_coco_profile_definition,
    render_coco_profile_json,
    render_coco_profile_spec,
)
from snowflake_semantic_tools.domain.validate.agent import spec_size
from snowflake_semantic_tools.domain.validate.coco_profile import (
    coco_profile_diagnostics,
    default_profile_problems,
    unlisted_profile,
)

# The stage folder every profile's versions are staged under, one subfolder per profile.
COCO_STAGE_FOLDER = "coco-profiles"
# A profile carries no display block, so `SET PROFILE` writes an empty one.
_NO_DISPLAY = AgentProfile()


@dataclass(frozen=True, slots=True)
class CompiledCocoProfile(StandaloneArtifact):
    """One agent profile whose skills are pinned, with its rendered specification.

    Attributes:
        payload: The specification JSON that is staged.
        definition_fingerprint: The SHA-256 of the whole definition -- the specification with
            the comment, published name, and `enabled` -- so a change to any of them is a change.
        stage_path: `<stage>/coco-profiles/<profile>/<git sha>`, where the specification is
            staged; empty until `for_coco_publication` sets it.
        stage: The stage `stage_path` starts with; None until `for_coco_publication` sets it.
        dirty: The project had uncommitted changes, so each version committed is labeled
            `git:<sha>-dirty`.
    """

    resolved: ResolvedCocoProfile
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
        return "coco_profile"

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
        """The stage folder this profile's versions are staged in: `coco-profiles/<profile>`."""
        return f"{COCO_STAGE_FOLDER}/{self.name}"

    @property
    def rendered_artifact(self) -> RenderedArtifact:
        return render_agent_object(self._staged, None)

    def rendered_for_publish(self, manifest_id: str) -> RenderedArtifact:
        """Render every statement program with the marked comment, written first as an agent's is."""
        marker = OwnershipMarker(manifest_id, self.definition_fingerprint)
        rendered = render_agent_object(self._staged, marked_comment(self.resolved.model.comment, marker.text))
        return replace(rendered, expected_marker=marker)

    @property
    def _staged(self) -> StagedAgentObject:
        return StagedAgentObject(
            key=self.artifact_key,
            artifact_type="coco_profile",
            target=self.target,
            payload=self.payload,
            definition_fingerprint=self.definition_fingerprint,
            spec_fingerprint=self.spec_fingerprint,
            metadata=AgentObjectMetadata(alias=None, tags=(), profile=_NO_DISPLAY, secure=None),
            depends_on=self.resolved.skill_dependencies,
            references=(),
            folder=self.folder,
            stage_path=self.stage_path,
            stage=self.stage,
            dirty=self.dirty,
        )


def for_coco_publication(
    compiled: CompiledCocoProfile, *, stage: QualifiedName, git_sha: str, dirty: bool = False
) -> CompiledCocoProfile:
    """Return the profile ready to publish, staged under `<stage>/coco-profiles/<profile>/<git_sha>`."""
    return replace(compiled, stage_path=staged_path(stage, compiled.folder, git_sha), stage=stage, dirty=dirty)


def pinned_by_profiles(catalog: CocoProfileCatalog) -> frozenset[str]:
    """Return the artifact keys of the skills and plugins an enabled profile, or the shared layer, pins.

    They count as consumed when the agents check that each published extension has a consumer.
    """
    shared = catalog.shared.skills if catalog.shared is not None and catalog.profiles else ()
    return frozenset(
        (
            *(artifact_key("skill", name) for name in shared),
            *(
                artifact_key(skill.ref, skill.path)
                for model in catalog.profiles
                if model.enabled
                for skill in model.skills
            ),
        )
    )


class CompileCocoProfiles:
    """Compile each enabled profile: pin its skills, assemble its instructions, check it, render its spec.

    Each model arrives with its settings resolved and its published name set; `targets` says
    where each publishes, by artifact key. `diagnostics` are the loader's; they come first, and
    an error among them that names a profile keeps that profile back like one found here.
    """

    def __init__(
        self,
        catalog: CocoProfileCatalog,
        diagnostics: DiagnosticBag,
        context: AgentCompileContext,
        targets: dict[str, QualifiedName],
    ) -> None:
        self._catalog = catalog
        self._diagnostics = diagnostics
        self._context = context
        self._targets = targets

    def run_result(self) -> CompileResult:
        """Compile the enabled profiles in folder order; a profile compiles unless an error names it.

        Diagnostics:
            Every code `coco_profile_diagnostics` lists, for each enabled profile.
            Every code `resolve_skills` lists, for its skills and the shared layer's.
            SST-VAL511: a rendered spec is over the 100,000-byte limit.
            SST-VAL512: a rendered spec is over 80% of that limit.
            SST-VAL890: a profile compiled, relying on agent profiles, a Snowflake private preview.
            SST-VAL891: a compiled profile publishes outside CORTEX_CODE.CONFIG.
            Every code `default_profile_problems` lists, for the profiles that compiled.
        """
        diagnostics: list[Diagnostic] = list(self._diagnostics)
        compiled: list[CompiledCocoProfile] = []
        for model in self._catalog.profiles:
            if not model.enabled:
                continue
            target = self._targets[model.key]
            resolved, problems = self._resolve(model)
            diagnostics.extend(problems)
            payload = render_coco_profile_json(resolved)
            diagnostics.extend(spec_size(model, payload))
            if has_error(model.key, diagnostics):
                continue
            spec = render_coco_profile_spec(resolved)
            fingerprint = sha256(desired_coco_profile_definition(resolved.model, spec)).hexdigest()
            compiled.append(CompiledCocoProfile(resolved, target, payload, fingerprint))
            diagnostics.append(D("SST-VAL890", origin=model.origin, subject=model.key, artifact=model.name))
            diagnostics.extend(unlisted_profile(model, target))
        diagnostics.extend(default_profile_problems([(item.resolved.model, item.target) for item in compiled]))
        return CompileResult(tuple(compiled), DiagnosticBag(diagnostics))

    def _resolve(self, model: CocoProfileModel) -> tuple[ResolvedCocoProfile, tuple[Diagnostic, ...]]:
        """Pin the shared layer's skills, then the profile's own, and assemble its instructions.

        A skill the profile repeats from the shared layer is pinned once, where the shared layer
        names it.
        """
        shared = self._catalog.shared
        checks = coco_profile_diagnostics(model, shared, self._context.allowed_models)
        carried = shared.skills if shared is not None else ()
        own = tuple(skill for skill in model.skills if not (skill.ref == "skill" and skill.path in carried))
        authored = replace(model, skills=(*owned_entries("skill", tuple(dict.fromkeys(carried))), *own))
        skills, dependencies, problems = resolve_skills(authored, self._context)
        resolved = ResolvedCocoProfile(
            replace(model, skills=skills), assemble_instructions(shared, model), dependencies
        )
        return resolved, (*checks, *problems)
