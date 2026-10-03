"""Compile skills and plugins into content-addressed Cortex Extension versions.

`CompileSkills` compiles the catalog; `extension_pins` and `unpublished_reasons` project its
result for the agents that reference the extensions.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from hashlib import sha256

from snowflake_semantic_tools.app.compile.agents import ExtensionPin
from snowflake_semantic_tools.app.compile.base import CompileResult, StandaloneArtifact, has_error
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key, split_artifact_key
from snowflake_semantic_tools.domain.model.config_schema import CONFIG_FILE
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    CompositeFacts,
    PublishShape,
    RenderedArtifact,
    StatementPlan,
)
from snowflake_semantic_tools.domain.model.registry import GrantPreservation
from snowflake_semantic_tools.domain.model.skill import DEFAULT_VERSION_PREFIX, Plugin, Skill, SkillBundle, SkillCatalog
from snowflake_semantic_tools.domain.render.skill_bundle import build_plugin_bundle, build_skill_bundle
from snowflake_semantic_tools.domain.sql import Sql, stage_path
from snowflake_semantic_tools.domain.validate.publication import (
    MintedVersion,
    put_target_diagnostics,
    schema_diagnostics,
    version_collision_diagnostics,
    version_diagnostics,
)
from snowflake_semantic_tools.domain.validate.skill import extension_name_diagnostics, validate_skill_catalog

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
    def location(self) -> Sql:
        """`prefix` as the stage location ADD VERSION ... FROM names.

        Raises:
            ValueError: the bundle name or the alias is not a safe stage path segment.
        """
        return stage_path(self.stage, f"{self.bundle.name}/{self.alias}/")

    @property
    def paths(self) -> tuple[str, ...]:
        """Return the path of each file in the bundle, in bundle order."""
        return tuple(entry.path for entry in self.bundle.entries)

    @property
    def minted(self) -> MintedVersion:
        """Return the version this release mints, as the publication guards read it."""
        return MintedVersion(self.key, self.bundle.name, self.target, self.alias, self.bundle.digest)


@dataclass(frozen=True, slots=True)
class CompiledExtension(StandaloneArtifact):
    """One skill or plugin as the extension version it publishes.

    Attributes:
        scripts: Each script the bundle carries, as the skill that ships it and the path
            inside that skill; only an agent with a code_execution tool can run one.
        contained_keys: What the bundle carries besides itself: a plugin's member skills.
    """

    release: ExtensionRelease
    # `field()` keeps the protocol's `source_files` property from becoming this field's default.
    source_files: tuple[str, ...] = field()
    scripts: tuple[tuple[str, str], ...] = ()
    contained_keys: tuple[str, ...] = ()

    @property
    def has_scripts(self) -> bool:
        """Report whether the bundle carries any script."""
        return bool(self.scripts)

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

    def __init__(
        self, catalog: SkillCatalog, channel: CatalogChannel | None, *, config_file: str = CONFIG_FILE
    ) -> None:
        self._catalog = catalog
        self._channel = channel
        self._config_file = config_file

    def run_result(self) -> CompileResult:
        """Validate the catalog, then compile each skill and then each plugin; the result is sorted by key.

        Diagnostics come in that order: `validate_skill_catalog`'s, then each bundle's and its
        extension name's. Without a channel, or with a version prefix no alias can start with,
        nothing compiles. Skills compile first, so a plugin sees what its members reported.

        Diagnostics:
            SST-CFG008: `skills.+version_prefix` cannot start an alias.
            SST-VAL801: a skill or plugin's extension name starts with a digit, so it cannot
                name a Snowflake object; it does not compile.
            SST-VAL836: a plugin member has errors, so the plugin is blocked.
            SST-VAL831: each compiled skill and plugin publishes through the CORTEX EXTENSION
                statements (info).
            SST-VAL803, SST-VAL806, SST-VAL820: as `_release_checks` reports them.
            SST-VAL825: two releases name one extension's version alike for other content;
                the later one does not compile.
        """
        diagnostics: list[Diagnostic] = list(validate_skill_catalog(self._catalog))
        channel = self._channel
        if channel is None:
            return CompileResult((), DiagnosticBag(diagnostics))
        if not _ALIAS_PREFIX.fullmatch(channel.version_prefix):
            diagnostics.append(
                D(
                    "SST-CFG008",
                    origin=Origin(self._config_file),
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
        collisions = version_collision_diagnostics(item.release.minted for item in compiled)
        diagnostics.extend(collisions)
        collided = {item.subject for item in collisions}
        kept = sorted(
            (item for item in compiled if item.artifact_key not in collided), key=lambda item: item.artifact_key
        )
        return CompileResult(tuple(kept), DiagnosticBag(diagnostics))

    def _compile_skill(self, skill: Skill, diagnostics: list[Diagnostic]) -> CompiledExtension | None:
        bundle, bundle_diagnostics = build_skill_bundle(skill)
        diagnostics.extend(bundle_diagnostics)
        if bundle is None or has_error(skill.key, diagnostics):
            return None
        unnamed = extension_name_diagnostics(skill.key, "folder", skill.name, skill.extension_name, skill.origin)
        if unnamed:
            diagnostics.extend(unnamed)
            return None
        release = self._release(skill.key, "SKILL", skill.extension_name, skill.description or "", bundle)
        if self._release_checks(release, diagnostics):
            return None
        diagnostics.append(_extension_surface(release, skill.name, skill.origin))
        return CompiledExtension(release, skill.source_files, scripts=_scripts_of((skill,)))

    def _compile_plugin(
        self, plugin: Plugin, members: Mapping[str, Skill], diagnostics: list[Diagnostic]
    ) -> CompiledExtension | None:
        """Bundle a plugin unless it, or a member skill, has an error; diagnostics are appended in order.

        The error check reads every diagnostic so far, the members' included, so it must run
        after the skills compile.
        """
        # SST-VAL836 stays here, not in `domain.validate`: it is a blocking decision read off
        # the diagnostics this compile has gathered so far, not a check of authored input.
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
        unnamed = extension_name_diagnostics(plugin.key, "plugin", plugin.name, plugin.extension_name, plugin.origin)
        if unnamed:
            diagnostics.extend(unnamed)
            return None
        carried = [members[name] for name in plugin.members if name in members]
        sources = tuple(
            dict.fromkeys((plugin.manifest_file, *(path for skill in carried for path in skill.source_files)))
        )
        release = self._release(plugin.key, "PLUGIN", plugin.extension_name, plugin.description or "", bundle)
        if self._release_checks(release, diagnostics):
            return None
        diagnostics.append(_extension_surface(release, plugin.name, plugin.origin))
        return CompiledExtension(
            release,
            sources,
            scripts=_scripts_of(carried),
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

    def _release_checks(self, release: ExtensionRelease, diagnostics: list[Diagnostic]) -> bool:
        """Append what the publication guards report for a release; True when one is an error.

        `_release` names the version by its digest, in the channel's schema, under a prefix that
        ends in a separator; these report a release that is not so, which then does not compile.

        Diagnostics:
            SST-VAL803: the alias is not the version prefix and the bundle digest.
            SST-VAL806: the target is outside the catalog channel's schema (a warning).
            SST-VAL820: the bundle's stage prefix does not end in `/`.
        """
        channel = self._channel
        assert channel is not None
        version = release.minted
        catalog = QualifiedName.from_parts(channel.database, channel.schema, "X").folded[:2]
        found = (
            *version_diagnostics(version, channel.version_prefix),
            *schema_diagnostics(version, catalog),
            *put_target_diagnostics(release.key, release.bundle.name, (release.prefix,)),
        )
        diagnostics.extend(found)
        return any(item.blocks for item in found)


def _extension_surface(release: ExtensionRelease, name: str, origin: Origin) -> Diagnostic:
    """SST-VAL831: the statements a release publishes through, which the public SQL reference omits."""
    kind = release.extension_type.lower()
    value = f"publishes as a {kind} Cortex Extension version, a statement surface the public SQL reference omits"
    return D("SST-VAL831", origin=origin, subject=release.key, artifact=name, value=value)


def _scripts_of(skills: Iterable[Skill]) -> tuple[tuple[str, str], ...]:
    """Each script the skills ship, as the skill and its path, skills first-carried first."""
    unique = {skill.name: skill for skill in skills}
    return tuple((skill.name, script.path) for skill in unique.values() for script in skill.scripts)


def unpublished_reasons(catalog: SkillCatalog, skills: CompileResult, channel_problem: str | None) -> dict[str, str]:
    """Say why each declared skill and plugin produced no extension version, by artifact key.

    One an error names "has errors"; any other is held back by `channel_problem`, or, when the
    channel is configured, by the channel itself.

    Returns:
        The reason by artifact key, the skills in catalog order and then the plugins; a skill
        or plugin that compiled has no entry.
    """
    compiled = {item.artifact_key for item in skills.compiled}
    failed = {item.subject for item in skills.diagnostics if item.blocks}
    return {
        key: "it has errors" if key in failed else channel_problem or "the catalog channel cannot publish it"
        for key in (*(item.key for item in catalog.skills), *(item.key for item in catalog.plugins))
        if key not in compiled
    }


def extension_pins(skills: CompileResult) -> tuple[dict[str, ExtensionPin], dict[str, ExtensionPin], frozenset[str]]:
    """Return what `skill()` and `plugin()` references pin to, and the skills plugins carry.

    A plugin's members are the skill folders its bundle carries, in bundle order.

    Returns:
        The skill pins and the plugin pins, each by extension name, and the artifact key of
        every skill some plugin carries.
    """
    skill_pins: dict[str, ExtensionPin] = {}
    plugin_pins: dict[str, ExtensionPin] = {}
    consumed: set[str] = set()
    for item in skills.compiled:
        if not isinstance(item, CompiledExtension):
            continue
        release = item.release
        if item.artifact_type == "skill":
            skill_pins[item.name] = ExtensionPin(
                item.artifact_key, release.target, release.alias, (item.name,), item.scripts
            )
            continue
        members = tuple(
            dict.fromkeys(
                entry.path.split("/")[1] for entry in release.bundle.entries if entry.path.startswith("skills/")
            )
        )
        plugin_pins[item.name] = ExtensionPin(item.artifact_key, release.target, release.alias, members, item.scripts)
        consumed.update(artifact_key("skill", member) for member in members)
    return skill_pins, plugin_pins, frozenset(consumed)
