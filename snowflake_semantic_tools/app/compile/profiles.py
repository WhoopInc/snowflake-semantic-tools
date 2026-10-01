"""Compile Desktop profiles into content-addressed stage trees and registry rows."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Mapping

from snowflake_semantic_tools.app.compile.base import CompileResult, StandaloneArtifact
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.diagnostic import D, Diagnostic, DiagnosticBag, Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    CompositeFacts,
    PublishShape,
    RenderedArtifact,
    StatementPlan,
)
from snowflake_semantic_tools.domain.model.profile import (
    DESKTOP_REGISTRY,
    DesktopProfile,
    ProfileCatalog,
    ProfileRelease,
    build_profile,
    unreached_skills,
    validate_profile_catalog,
)
from snowflake_semantic_tools.domain.model.registry import GrantPreservation
from snowflake_semantic_tools.domain.model.skill import (
    DEFAULT_VERSION_PREFIX,
    Plugin,
    Skill,
    SkillCatalog,
    build_plugin_bundle,
    flatten_skill,
)


@dataclass(frozen=True, slots=True)
class DesktopChannel:
    """`skills.stage`: the profile stage and the registry Desktop reads rows from."""

    stage: QualifiedName
    registry: QualifiedName
    version_prefix: str = DEFAULT_VERSION_PREFIX


@dataclass(frozen=True, slots=True)
class CompiledProfile(StandaloneArtifact):
    """One profile's registry row and the stage trees it points at.

    Attributes:
        contained_keys: Every input the profile carries -- the shared profile, skills,
            plugins, commands, hooks, and MCP servers -- so that `--partial` keeps the
            profile back when any of them has an error.
    """

    release: ProfileRelease
    channel: DesktopChannel
    contained_keys: tuple[str, ...] = ()

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
    def rendered_artifact(self) -> RenderedArtifact:
        prefixes = "\n".join(tree.prefix for tree in self.release.trees)
        return RenderedArtifact.create(
            key=self.artifact_key,
            artifact_type="profile",
            target=self.channel.registry,
            ddl=self.release.document(),
            shape=PublishShape("", render_dialect="profile_json", grant_preservation=GrantPreservation.NONE),
            statements=StatementPlan(default=()),
            composite=CompositeFacts(
                component_fingerprints=(
                    ("version", self.release.version),
                    ("trees", sha256(prefixes.encode("utf-8")).hexdigest()),
                ),
                physical_resources=(("STAGE", self.channel.stage), ("TABLE", self.channel.registry)),
            ),
        )


class CompileProfiles:
    """Compile one registry row per profile.

    A profile whose skills, plugins, or commands have errors does not publish.
    """

    def __init__(
        self,
        catalog: ProfileCatalog,
        skills: SkillCatalog,
        channel: DesktopChannel | None,
        *,
        catalog_channel: bool,
        blocked_skills: frozenset[str] = frozenset(),
        blocked_plugins: frozenset[str] = frozenset(),
    ) -> None:
        self._catalog = catalog
        self._skills = skills
        self._channel = channel
        self._catalog_channel = catalog_channel
        self._blocked = blocked_skills
        self._blocked_plugins = blocked_plugins

    def run_result(self) -> CompileResult:
        """Validate the catalog, then compile each profile, in authored order, that nothing blocks.

        A profile is blocked by an error on itself, the shared profile, or anything it names:
        a skill or plugin with errors (reported here as SST-VAL855), a command, a hook, or an
        MCP server. Without a channel, only the catalog's diagnostics are reported.

        Diagnostics:
            SST-VAL854: the channel's registry is not the one CoCo Desktop reads.
            SST-VAL855: a profile includes a skill or plugin with errors.
        """
        skills = {skill.name: skill for skill in self._skills.skills}
        plugins = {plugin.name: plugin for plugin in self._skills.plugins}
        diagnostics: list[Diagnostic] = list(validate_profile_catalog(self._catalog, skills, plugins))
        diagnostics.extend(
            unreached_skills(skills, self._catalog, catalog_channel=self._catalog_channel, plugins=plugins)
        )
        blocked_plugins = set(self._blocked_plugins)
        if not self._catalog_channel:
            flattened_blocked, flatten_diagnostics = self._flatten_plugins(skills, plugins)
            diagnostics.extend(flatten_diagnostics)
            blocked_plugins.update(flattened_blocked)
        channel = self._channel
        if channel is None:
            return CompileResult((), DiagnosticBag(diagnostics))
        if channel.registry.folded != QualifiedName.parse(DESKTOP_REGISTRY).folded:
            diagnostics.append(D("SST-VAL854", value=channel.registry.sql, expected=DESKTOP_REGISTRY))
        compiled: list[CompiledProfile] = []
        for profile in self._catalog.profiles:
            # The SST-VAL855 appended here is an error on the profile, so it blocks it below.
            diagnostics.extend(self._blocking_inputs(profile, blocked_plugins))
            subjects = _blocking_subjects(profile, self._shared_commands())
            if any(item.severity is Severity.ERROR and item.subject in subjects for item in diagnostics):
                continue
            compiled.append(self._compile(profile, skills, plugins, channel))
        return CompileResult(tuple(compiled), DiagnosticBag(diagnostics))

    def _flatten_plugins(
        self, skills: Mapping[str, Skill], plugins: Mapping[str, Plugin]
    ) -> tuple[set[str], list[Diagnostic]]:
        """Check each plugin a profile names, returning the plugins blocked and the problems found.

        Without the catalog channel no plugin bundle has been built yet, so the errors a
        profile's flattened copy would carry are reported here, once each: for each plugin in
        first-reference order, its member skills' flattening problems (a skill only the first
        time), then its bundle problems. A plugin is blocked when it cannot bundle or holds a
        blocked skill.
        """
        blocked: set[str] = set()
        diagnostics: list[Diagnostic] = []
        flattened: set[str] = set()
        referenced = dict.fromkeys(name for profile in self._catalog.profiles for name in profile.plugins)
        for name in referenced:
            plugin = plugins.get(name)
            if plugin is None:
                continue
            for member in dict.fromkeys(plugin.members):
                if member in skills and member not in flattened:
                    flattened.add(member)
                    diagnostics.extend(flatten_skill(skills[member])[2])
            bundle, bundle_diagnostics = build_plugin_bundle(plugin, skills)
            diagnostics.extend(bundle_diagnostics)
            if bundle is None or any(member in self._blocked for member in plugin.members):
                blocked.add(name)
        return blocked, diagnostics

    def _blocking_inputs(self, profile: DesktopProfile, blocked_plugins: set[str]) -> tuple[Diagnostic, ...]:
        """Report each skill or plugin with errors the profile includes, once each, skills first."""
        causes = [
            *(("skill", name) for name in (*self._shared_skills(), *profile.skills) if name in self._blocked),
            *(("plugin", name) for name in profile.plugins if name in blocked_plugins),
        ]
        return tuple(
            D("SST-VAL855", origin=profile.origin, subject=profile.key, artifact=profile.name, kind=kind, name=name)
            for kind, name in dict.fromkeys(causes)
        )

    def _compile(
        self,
        profile: DesktopProfile,
        skills: Mapping[str, Skill],
        plugins: Mapping[str, Plugin],
        channel: DesktopChannel,
    ) -> CompiledProfile:
        """Build the profile's release on `channel`, recording every input it carries, each once."""
        release = build_profile(
            profile,
            self._catalog,
            skills,
            stage=channel.stage.sql,
            version_prefix=channel.version_prefix,
            plugins=plugins,
        )
        shared_skills = self._shared_skills()
        shared_commands = self._shared_commands()
        contained = (
            "profile:shared",
            *(artifact_key("skill", name) for name in (*shared_skills, *profile.skills)),
            *(artifact_key("plugin", name) for name in profile.plugins),
            *(artifact_key("command", name) for name in (*shared_commands, *profile.commands)),
            *(f"hook:{name}" for name in profile.hooks),
            *(f"mcp:{name}" for name in profile.mcp_servers),
        )
        return CompiledProfile(release, channel, tuple(dict.fromkeys(contained)))

    def _shared_skills(self) -> tuple[str, ...]:
        shared = self._catalog.shared
        return shared.skills if shared is not None else ()

    def _shared_commands(self) -> tuple[str, ...]:
        shared = self._catalog.shared
        return shared.commands if shared is not None else ()


def _blocking_subjects(profile: DesktopProfile, shared_commands: tuple[str, ...]) -> set[str]:
    """The subjects whose errors keep the profile back: itself, the shared profile, its other inputs."""
    return {
        profile.key,
        "profile:shared",
        *(f"mcp:{name}" for name in profile.mcp_servers),
        *(f"hook:{name}" for name in profile.hooks),
        *(artifact_key("command", name) for name in (*shared_commands, *profile.commands)),
    }
