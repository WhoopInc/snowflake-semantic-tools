"""Compile skills and plugins into content-addressed Cortex Extension versions."""

from __future__ import annotations

import re
from dataclasses import dataclass
from hashlib import sha256

from ..domain.model.diagnostic import D, Diagnostic, DiagnosticBag, Origin, Severity
from ..domain.model.identifier import QualifiedName
from ..domain.model.lifecycle import RenderedArtifact
from ..domain.model.registry import GrantPreservation
from ..domain.model.skill import (
    SkillBundle,
    SkillCatalog,
    build_plugin_bundle,
    build_skill_bundle,
    validate_skill_catalog,
)
from .compile import CompileResult

_ALIAS_PREFIX = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


@dataclass(frozen=True, slots=True)
class CatalogChannel:
    """Where the catalog channel publishes: `skills.catalog` plus the shared `skills` keys."""

    database: str
    schema: str
    bundle_stage: QualifiedName
    version_prefix: str = "SST_"
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
        return tuple(entry.path for entry in self.bundle.entries)


@dataclass(frozen=True, slots=True)
class CompiledExtension:
    release: ExtensionRelease
    source_files: tuple[str, ...]
    has_scripts: bool = False

    @property
    def name(self) -> str:
        return self.release.bundle.name

    @property
    def artifact_key(self) -> str:
        return self.release.key

    @property
    def artifact_type(self) -> str:
        return self.release.key.split(":", 1)[0]

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
            object_type="",
            render_dialect="bundle_json",
            grant_preservation=GrantPreservation.NONE,
            statements=(),
            component_fingerprints=(
                ("bundle", release.bundle.digest),
                ("alias", release.alias),
                ("comment", sha256(release.comment.encode("utf-8")).hexdigest()),
                ("certified", "true" if release.certified else "false"),
            ),
            physical_resources=(("CORTEX EXTENSION", release.target), ("STAGE", release.stage)),
            generic_apply_safe=False,
        )

    def rendered_for_publish(self, manifest_id: str) -> RenderedArtifact:
        del manifest_id
        return self.rendered_artifact


class CompileSkills:
    """One SKILL-type extension per skill and one PLUGIN-type extension per plugin."""

    def __init__(self, catalog: SkillCatalog, channel: CatalogChannel | None) -> None:
        self._catalog = catalog
        self._channel = channel

    def run_result(self) -> CompileResult:
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
            bundle, bundle_diagnostics = build_skill_bundle(skill)
            diagnostics.extend(bundle_diagnostics)
            if bundle is None or _blocked(diagnostics, skill.key):
                continue
            compiled.append(
                CompiledExtension(
                    self._release(skill.key, "SKILL", skill.extension_name, skill.description or "", bundle),
                    skill.source_files,
                    has_scripts=bool(skill.scripts),
                )
            )
        for plugin in self._catalog.plugins:
            bundle, bundle_diagnostics = build_plugin_bundle(plugin, members)
            diagnostics.extend(bundle_diagnostics)
            if bundle is None or _blocked(diagnostics, plugin.key):
                continue
            sources = tuple(
                dict.fromkeys(
                    (
                        plugin.manifest_file,
                        *(path for name in plugin.members if name in members for path in members[name].source_files),
                    )
                )
            )
            compiled.append(
                CompiledExtension(
                    self._release(plugin.key, "PLUGIN", plugin.extension_name, plugin.description or "", bundle),
                    sources,
                    has_scripts=any(members[name].scripts for name in plugin.members if name in members),
                )
            )
        return CompileResult(tuple(sorted(compiled, key=lambda item: item.artifact_key)), DiagnosticBag(diagnostics))

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


def _blocked(diagnostics: list[Diagnostic], subject: str) -> bool:
    return any(item.severity is Severity.ERROR and item.subject == subject for item in diagnostics)
