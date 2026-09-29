"""Compile Desktop profiles into content-addressed stage trees and registry rows."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256

from ..domain.model.diagnostic import D, Diagnostic, DiagnosticBag, Severity
from ..domain.model.identifier import QualifiedName
from ..domain.model.lifecycle import RenderedArtifact
from ..domain.model.profile import (
    DESKTOP_REGISTRY,
    ProfileCatalog,
    ProfileRelease,
    build_profile,
    unreached_skills,
    validate_profile_catalog,
)
from ..domain.model.registry import GrantPreservation
from ..domain.model.skill import SkillCatalog
from .compile import CompileResult


@dataclass(frozen=True, slots=True)
class DesktopChannel:
    """`skills.stage`: the profile stage and the registry Desktop reads rows from."""

    stage: QualifiedName
    registry: QualifiedName
    version_prefix: str = "SST_"


@dataclass(frozen=True, slots=True)
class CompiledProfile:
    release: ProfileRelease
    channel: DesktopChannel

    @property
    def name(self) -> str:
        return self.release.name

    @property
    def artifact_key(self) -> str:
        return self.release.key

    @property
    def artifact_type(self) -> str:
        return "profile"

    @property
    def source_files(self) -> tuple[str, ...]:
        return self.release.source_files

    @property
    def member_keys(self) -> tuple[str, ...]:
        return ()

    @property
    def referenced_models(self) -> tuple[str, ...]:
        return ()

    @property
    def dbt_relations(self) -> tuple[tuple[str, str], ...]:
        return ()

    @property
    def rendered_artifact(self) -> RenderedArtifact:
        prefixes = "\n".join(tree.prefix for tree in self.release.trees)
        return RenderedArtifact.create(
            key=self.artifact_key,
            artifact_type="profile",
            target=self.channel.registry,
            ddl=self.release.document(),
            object_type="",
            render_dialect="profile_json",
            grant_preservation=GrantPreservation.NONE,
            statements=(),
            component_fingerprints=(
                ("version", self.release.version),
                ("trees", sha256(prefixes.encode("utf-8")).hexdigest()),
            ),
            physical_resources=(("STAGE", self.channel.stage), ("TABLE", self.channel.registry)),
            generic_apply_safe=False,
        )

    def rendered_for_publish(self, manifest_id: str) -> RenderedArtifact:
        del manifest_id
        return self.rendered_artifact


class CompileProfiles:
    """One registry row per profile; a profile whose skills have errors does not publish."""

    def __init__(
        self,
        catalog: ProfileCatalog,
        skills: SkillCatalog,
        channel: DesktopChannel | None,
        *,
        catalog_channel: bool,
        blocked_skills: frozenset[str] = frozenset(),
    ) -> None:
        self._catalog = catalog
        self._skills = skills
        self._channel = channel
        self._catalog_channel = catalog_channel
        self._blocked = blocked_skills

    def run_result(self) -> CompileResult:
        skills = {skill.name: skill for skill in self._skills.skills}
        diagnostics: list[Diagnostic] = list(validate_profile_catalog(self._catalog, skills))
        diagnostics.extend(unreached_skills(skills, self._catalog, catalog_channel=self._catalog_channel))
        channel = self._channel
        if channel is None:
            return CompileResult((), DiagnosticBag(diagnostics))
        if channel.registry.folded != QualifiedName.parse(DESKTOP_REGISTRY).folded:
            diagnostics.append(D("SST-VAL854", value=channel.registry.sql, expected=DESKTOP_REGISTRY))
        shared = self._catalog.shared.skills if self._catalog.shared is not None else ()
        compiled: list[CompiledProfile] = []
        for profile in self._catalog.profiles:
            broken = [name for name in (*shared, *profile.skills) if name in self._blocked]
            for name in dict.fromkeys(broken):
                diagnostics.append(
                    D("SST-VAL855", origin=profile.origin, subject=profile.key, artifact=profile.name, name=name)
                )
            subjects = {
                profile.key,
                "profile:shared",
                *(f"mcp:{name}" for name in profile.mcp_servers),
                *(f"hook:{name}" for name in profile.hooks),
            }
            if any(item.severity is Severity.ERROR and item.subject in subjects for item in diagnostics):
                continue
            release = build_profile(
                profile,
                self._catalog,
                skills,
                stage=channel.stage.sql,
                version_prefix=channel.version_prefix,
            )
            compiled.append(CompiledProfile(release, channel))
        return CompileResult(tuple(compiled), DiagnosticBag(diagnostics))
