"""Compile skills and plugins into content-addressed Cortex Extension versions."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from hashlib import sha256
from typing import Mapping

from ...domain.model.artifact_key import artifact_key, split_artifact_key
from ...domain.model.diagnostic import D, Diagnostic, DiagnosticBag, Origin
from ...domain.model.identifier import QualifiedName
from ...domain.model.lifecycle import CompositeFacts, PublishShape, RenderedArtifact, StatementPlan
from ...domain.model.registry import GrantPreservation
from ...domain.model.skill import (
    DEFAULT_VERSION_PREFIX,
    Plugin,
    Skill,
    SkillBundle,
    SkillCatalog,
    build_plugin_bundle,
    build_skill_bundle,
    validate_skill_catalog,
)
from .base import CompileResult, StandaloneArtifact, has_error

_ALIAS_PREFIX = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


@dataclass(frozen=True, slots=True)
class CatalogChannel:
    """Where the catalog channel publishes: `skills.catalog` plus the shared `skills` keys."""

    database: str
    schema: str
    bundle_stage: QualifiedName
    version_prefix: str = DEFAULT_VERSION_PREFIX
    certified: bool = False


@dataclass(frozen=True, slots=True)
class ExtensionRelease:
    """Everything apply needs to publish one extension version, keyed by artifact."""

    key: str
    extension_type: str
    target: QualifiedName
    stage: QualifiedName
    alias: str
    comment: str
    certified: bool
    bundle: SkillBundle

    @property
    def prefix(self) -> str:
        """`@<stage>/<name>/<ALIAS>/`: immutable, because the alias is the content digest."""
        return f"@{self.stage.sql}/{self.bundle.name}/{self.alias}/"

    @property
    def paths(self) -> tuple[str, ...]:
        """Return the path of each file in the bundle, in bundle order."""
        return tuple(entry.path for entry in self.bundle.entries)


@dataclass(frozen=True, slots=True)
class CompiledExtension(StandaloneArtifact):
    """One skill or plugin as the extension version it publishes.

    Attributes:
        has_scripts: Whether the bundle carries a script, which only an agent with a
            code_execution tool can run.
        contained_keys: What the bundle carries besides itself: a plugin's member skills.
    """

    release: ExtensionRelease
    # `field()` keeps the protocol's `source_files` property from becoming this field's default.
    source_files: tuple[str, ...] = field()
    has_scripts: bool = False
    contained_keys: tuple[str, ...] = ()

    @property
    def name(self) -> str:
        return self.release.bundle.name

    @property
    def artifact_key(self) -> str:
        return self.release.key

    @property
    def artifact_type(self) -> str:
        return split_artifact_key(self.release.key)[0]

    @property
    def rendered_artifact(self) -> RenderedArtifact:
        release = self.release
        return RenderedArtifact.create(
            key=release.key,
            artifact_type=self.artifact_type,
            target=release.target,
            ddl=release.bundle.manifest(
                alias=release.alias,
                target=release.target.sql,
                comment=release.comment,
                certified=release.certified,
            ),
            shape=PublishShape("", render_dialect="bundle_json", grant_preservation=GrantPreservation.NONE),
            statements=StatementPlan(default=()),
            composite=CompositeFacts(
                component_fingerprints=(
                    ("bundle", release.bundle.digest),
                    ("alias", release.alias),
                    ("comment", sha256(release.comment.encode("utf-8")).hexdigest()),
                    ("certified", "true" if release.certified else "false"),
                ),
                physical_resources=(("CORTEX EXTENSION", release.target), ("STAGE", release.stage)),
            ),
        )


class CompileSkills:
    """One SKILL-type extension per skill and one PLUGIN-type extension per plugin."""

    def __init__(self, catalog: SkillCatalog, channel: CatalogChannel | None) -> None:
        self._catalog = catalog
        self._channel = channel

    def run_result(self) -> CompileResult:
        """Validate the catalog, then compile each skill and then each plugin; the result is sorted by key.

        Diagnostics come in that order: `validate_skill_catalog`'s, then each bundle's. Without
        a channel, or with a version prefix no alias can start with, nothing compiles. Skills
        compile first, so a plugin sees what its members reported.

        Diagnostics:
            SST-CFG008: `skills.+version_prefix` cannot start an alias.
            SST-VAL836: a plugin member has errors, so the plugin is blocked.
        """
        diagnostics: list[Diagnostic] = list(validate_skill_catalog(self._catalog))
        channel = self._channel
        if channel is None:
            return CompileResult((), DiagnosticBag(diagnostics))
        if not _ALIAS_PREFIX.fullmatch(channel.version_prefix):
            diagnostics.append(
                D(
                    "SST-CFG008",
                    origin=Origin("sst_config.yml"),
                    subject="config:skills.+version_prefix",
                    key="skills.+version_prefix",
                    found=repr(channel.version_prefix),
                    expected="a letter or underscore, then letters, digits, or underscores",
                )
            )
            return CompileResult((), DiagnosticBag(diagnostics))
        compiled: list[CompiledExtension] = []
        members = {skill.name: skill for skill in self._catalog.skills}
        for skill in self._catalog.skills:
            extension = self._compile_skill(skill, diagnostics)
            if extension is not None:
                compiled.append(extension)
        for plugin in self._catalog.plugins:
            extension = self._compile_plugin(plugin, members, diagnostics)
            if extension is not None:
                compiled.append(extension)
        return CompileResult(tuple(sorted(compiled, key=lambda item: item.artifact_key)), DiagnosticBag(diagnostics))

    def _compile_skill(self, skill: Skill, diagnostics: list[Diagnostic]) -> CompiledExtension | None:
        bundle, bundle_diagnostics = build_skill_bundle(skill)
        diagnostics.extend(bundle_diagnostics)
        if bundle is None or has_error(skill.key, diagnostics):
            return None
        release = self._release(skill.key, "SKILL", skill.extension_name, skill.description or "", bundle)
        return CompiledExtension(release, skill.source_files, has_scripts=bool(skill.scripts))

    def _compile_plugin(
        self, plugin: Plugin, members: Mapping[str, Skill], diagnostics: list[Diagnostic]
    ) -> CompiledExtension | None:
        """Bundle a plugin unless it, or a member skill, has an error; diagnostics are appended in order.

        The error check reads every diagnostic so far, the members' included, so it must run
        after the skills compile.
        """
        # A member that fails catalog validation (say, a file a stage rejects) would
        # upload inside the plugin too, so the plugin is blocked on it.
        for name in dict.fromkeys(plugin.members):
            member = members.get(name)
            if member is not None and has_error(member.key, diagnostics):
                diagnostics.append(
                    D("SST-VAL836", origin=plugin.origin, subject=plugin.key, artifact=plugin.name, name=name)
                )
        if has_error(plugin.key, diagnostics):
            return None
        bundle, bundle_diagnostics = build_plugin_bundle(plugin, members)
        diagnostics.extend(bundle_diagnostics)
        if bundle is None or has_error(plugin.key, diagnostics):
            return None
        carried = [members[name] for name in plugin.members if name in members]
        sources = tuple(
            dict.fromkeys((plugin.manifest_file, *(path for skill in carried for path in skill.source_files)))
        )
        release = self._release(plugin.key, "PLUGIN", plugin.extension_name, plugin.description or "", bundle)
        return CompiledExtension(
            release,
            sources,
            has_scripts=any(skill.scripts for skill in carried),
            contained_keys=tuple(artifact_key("skill", name) for name in dict.fromkeys(plugin.members)),
        )

    def _release(self, key: str, kind: str, name: str, comment: str, bundle: SkillBundle) -> ExtensionRelease:
        channel = self._channel
        assert channel is not None
        return ExtensionRelease(
            key=key,
            extension_type=kind,
            target=QualifiedName.from_parts(channel.database, channel.schema, name),
            stage=channel.bundle_stage,
            alias=bundle.alias(channel.version_prefix),
            comment=comment,
            certified=channel.certified,
            bundle=bundle,
        )
