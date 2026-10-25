"""Compile a whole project: run every typed compiler over the project's inputs and merge the results.

`CompileProject` reads the project only through a `ProjectInputs` port, so it touches no
file. It resolves each compiler's settings from `sst_config.yml` and the dbt target, runs
the compilers in a fixed order -- which is also the order a broken input is reported in --
and merges their results in DDL order.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from types import MappingProxyType

from snowflake_semantic_tools.app.compile import CompileArtifacts, CompileSemanticViews
from snowflake_semantic_tools.app.compile.agents import AgentCompileContext, CompileAgents, CompiledAgent
from snowflake_semantic_tools.app.compile.agents.cross import cross_artifact_result
from snowflake_semantic_tools.app.compile.agents.settled import agent_environment, settled_agent
from snowflake_semantic_tools.app.compile.base import CompileResult
from snowflake_semantic_tools.app.compile.coco_desktop_profiles import CompileCocoDesktopProfiles, DesktopChannel
from snowflake_semantic_tools.app.compile.cowork_profiles import (
    CompiledCoworkProfile,
    compile_cowork_profiles,
    pinned_extensions,
)
from snowflake_semantic_tools.app.compile.evals import CompileEvals
from snowflake_semantic_tools.app.compile.skills import (
    CatalogChannel,
    CompileSkills,
    extension_pins,
    unpublished_reasons,
)
from snowflake_semantic_tools.app.compile.tools import CompileTools, ToolPlacement
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.diagnostics.policy import apply_overrides
from snowflake_semantic_tools.domain.model.agent import AgentModel
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key, split_artifact_key, view_key
from snowflake_semantic_tools.domain.model.coco_desktop_profile import (
    DESKTOP_REGISTRY,
    NAME_SUFFIX_PATTERN,
    CocoDesktopProfile,
    CocoDesktopProfileCatalog,
)
from snowflake_semantic_tools.domain.model.config_schema import (
    CONFIG_FILE,
    KeyKind,
    Setting,
    block_settings,
    config_block,
    config_bool,
    config_int,
    configured_dir,
    resolve_settings,
    setting_value,
    target_text,
    with_defaults,
)
from snowflake_semantic_tools.domain.model.cowork_profile import CoworkProfileModel
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from snowflake_semantic_tools.domain.model.eval import EvalCatalog, profile_subject_target
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.model.setting import CONFIG_SOURCE, ROOT_SOURCE, ROUTE_PREFIX
from snowflake_semantic_tools.domain.model.skill import DEFAULT_VERSION_PREFIX, Plugin, Skill, extension_identifier
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolMember
from snowflake_semantic_tools.domain.ports.project import ProjectInputs
from snowflake_semantic_tools.domain.resolve.stage_path import stage_member
from snowflake_semantic_tools.domain.resolve.tags import declared_tags
from snowflake_semantic_tools.domain.validate.config import (
    NAME_KINDS,
    config_tool_calls,
    config_tool_references,
    severity_overrides,
    unreferenced_tool_members,
    unusable_name,
)
from snowflake_semantic_tools.domain.validate.shared import namespace_collisions

# Where each artifact type sits in the merged stream, which is also the order apply publishes in.
POSITIONS: Mapping[str, int] = MappingProxyType(
    {name: value.ddl_position for name, value in SEMANTIC_REGISTRY.artifacts.items()}
)


@dataclass(frozen=True, slots=True)
class Publishing:
    """What the publishing channels compiled, and what the agents need to know of it.

    Attributes:
        skills: The skills and plugins, as extension versions.
        profiles: The Desktop profiles; empty when no profile has a stage.
        desktop_consumed: The artifact keys of the skills and plugins a profile carries, which
            have a consumer even when no agent references them.
        unpublished: Why each declared skill or plugin has no version, by artifact key.
    """

    skills: CompileResult
    profiles: CompileResult
    desktop_consumed: frozenset[str]
    unpublished: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class _Settings:
    """`sst_config.yml` read against the dbt target: where every compiler's settings come from.

    Attributes:
        problems: Each location that rendered to no Snowflake name, collected as the compilers
            read their settings; the target's own value stands in for it.
    """

    tree: Mapping[str, object]
    target: TargetIdentity
    file: str = CONFIG_FILE
    problems: list[Diagnostic] = field(default_factory=list)

    def block(self, name: str) -> dict[str, object]:
        """Return one top-level block; empty when it is absent or not a mapping."""
        return config_block(self.tree.get(name))

    def directory(self, key: str, default: str) -> str:
        """Return the `project.<key>` root, else `default`."""
        return configured_dir(self.tree, key, default)

    def location(self, name: str) -> tuple[str, str]:
        """Return block `name`'s `+database` and `+schema`, each defaulting to the target's own.

        Diagnostics:
            SST-CFG008: a part renders to no Snowflake identifier; the target's own is used.
        """
        block = self.block(name)
        return (
            self.name(block.get("+database"), self.target.database.folded, f"{name}.+database", None),
            self.name(block.get("+schema"), self.target.schema.folded, f"{name}.+schema", None),
        )

    def place(self, resolved: Mapping[str, Setting], block: str, subject: str) -> tuple[str, str]:
        """Return the `database` and `schema` an item's settings name, each else the target's own.

        Diagnostics:
            SST-CFG008: a part renders to no Snowflake identifier; the target's own is used.
        """
        return (
            self.setting(resolved, "database", self.target.database.folded, block, subject),
            self.setting(resolved, "schema", self.target.schema.folded, block, subject),
        )

    def setting(
        self,
        resolved: Mapping[str, Setting],
        key: str,
        default: str,
        block: str,
        subject: str,
        kind: KeyKind = KeyKind.IDENTIFIER,
    ) -> str:
        """Render an item's location setting `key`, else `default`, as `name` checks it.

        Diagnostics:
            SST-CFG008: the setting renders to no Snowflake name; `default` is used.
        """
        setting = resolved.get(key)
        if setting is None:
            return default
        # A setting from the item's own `config:` is reported against the item, not this file.
        own = subject if setting.source == CONFIG_SOURCE else None
        return self.name(setting.value, default, _setting_key(block, setting), own, kind)

    def name(
        self, value: object, default: str, key: str, subject: str | None, kind: KeyKind = KeyKind.IDENTIFIER
    ) -> str:
        """Render one location value, else `default`; one that renders to no Snowflake name is refused.

        A value that is itself no name was refused where it was checked, as the configuration file
        or the item's `config:` was read, so only one that `{{ target.* }}` made unusable is
        reported here. Either way `default` stands in, so nothing downstream parses it.

        Args:
            subject: The item whose `config:` wrote the value; None for this file's own key.

        Diagnostics:
            SST-CFG008: the value renders to no Snowflake name.
        """
        text = target_text(value, self.target, default) or default
        if value is None or text == default or not unusable_name(text, kind):
            return text
        if not (isinstance(value, str) and unusable_name(value, kind)):
            origin = Origin(self.file) if subject is None else None
            self.problems.append(
                D(
                    "SST-CFG008",
                    origin=origin,
                    subject=subject or f"config:{key}",
                    key=key,
                    found=repr(text),
                    expected=NAME_KINDS[kind],
                )
            )
        return default


class CompileProject:
    """Compile every artifact a project declares into one result, reading it through its inputs.

    A problem in what was authored is a diagnostic in the result, never an exception. An input
    the port cannot read raises out of `run`, at the step that reads it.
    """

    def __init__(self, inputs: ProjectInputs) -> None:
        self._inputs = inputs

    def run(self) -> CompileResult:
        """Compile the project's artifacts and merge them in DDL order, then by key.

        Steps, in order:

        1. Read `sst_config.yml` and the dbt target.
        2. Resolve the extensions agents consume, from `extensions:`.
        3. Compile skills and plugins, then Desktop profiles, each where its settings place it.
        4. With a dbt project, compile the semantic views, the tools, the agents, the CoWork
           profiles (a Snowflake private preview), and then the evals; each reads what the
           compilers before it produced.
        5. Merge the results. The diagnostics of the configuration, the target, and
           `extensions:` come first, then each compiler's in the order it ran.

        Diagnostics:
            SST-CFG036: an `extensions:` entry cannot be qualified.
            SST-VAL002: a skill or plugin this project publishes takes the name of an extension
                it consumes.
            Each typed compiler's, as it documents them.
        """
        config = self._inputs.config()
        target = self._inputs.target()
        settings = _Settings(config.tree, target.identity, config.file)
        consumed, consumed_diagnostics = consumed_extensions(config.tree, target.identity, file=config.file)
        publishing = self._publishing(settings)
        # A reference to an entry that cannot be qualified names that cause, not "undeclared".
        unpublished = {
            **publishing.unpublished,
            **{
                f"extension:{str(item.context['name']).casefold()}": "its extensions: entry cannot be qualified"
                for item in consumed_diagnostics
                if item.code == "SST-CFG036"
            },
        }
        results = [
            CompileResult(
                (),
                DiagnosticBag(
                    (
                        *config.diagnostics,
                        *target.diagnostics,
                        *consumed_diagnostics,
                        *_consumed_collisions(publishing.skills, consumed),
                    )
                ),
            ),
            publishing.skills,
            publishing.profiles,
        ]
        if config.has_dbt_project:
            results.extend(self._dbt_artifacts(settings, publishing, consumed, unpublished))
        results.append(CompileResult((), DiagnosticBag(settings.problems)))
        merged = CompileArtifacts.merge(results, POSITIONS)
        # Each code reports at the severity the project sets for it from here on.
        return replace(merged, diagnostics=apply_overrides(merged.diagnostics, severity_overrides(config.tree)))

    def _publishing(self, settings: _Settings) -> Publishing:
        """Compile skills, plugins, and Desktop profiles, each where its resolved settings place it.

        A skill or plugin publishes where its `+bundle_stage` resolves, and a profile where its
        `+stage` does. The catalogs are read whatever is configured, so every declared skill and
        plugin can be reported as unpublished with its reason. When no block of the three is
        written and no item places itself, nothing compiles; profiles compile when the
        `coco_desktop_profiles:` block is written or a profile has a stage.
        """
        catalog = self._inputs.skill_catalog(
            skills_dir=settings.directory("skills_dir", "skills"),
            plugins_dir=settings.directory("plugins_dir", "plugins"),
        )
        profiles_catalog = self._inputs.coco_desktop_profile_catalog(
            coco_desktop_profiles_dir=settings.directory("coco_desktop_profiles_dir", "coco_desktop_profiles"),
            hooks_dir=settings.directory("hooks_dir", "hooks"),
            mcp_servers_dir=settings.directory("mcp_servers_dir", "mcp-servers"),
            commands_dir=settings.directory("commands_dir", "commands"),
        )
        channels = {
            item.key: channel
            for block, items in (("skills", catalog.skills), ("plugins", catalog.plugins))
            for item in items
            if (channel := _catalog_channel(settings, block, item)) is not None
        }
        desktop = {
            profile.key: place
            for profile in profiles_catalog.profiles
            if (place := _desktop_channel(settings, profile)) is not None
        }
        written = {name for name in ("skills", "plugins", "coco_desktop_profiles") if name in settings.tree}
        if not (written or channels or desktop):
            empty = CompileResult(())
            return Publishing(empty, empty, frozenset(), unpublished_reasons(catalog, empty, frozenset()))
        skills = CompileSkills(catalog, channels, config_file=settings.file).run_result()
        unpublished = unpublished_reasons(catalog, skills, frozenset(channels))
        if not desktop and "coco_desktop_profiles" not in written:
            return Publishing(skills, CompileResult(()), frozenset(), unpublished)
        profiles = CompileCocoDesktopProfiles(
            profiles_catalog,
            catalog,
            desktop,
            catalog_published=frozenset(channels),
            blocked_skills=_blocked(skills, "skill"),
            blocked_plugins=_blocked(skills, "plugin"),
        ).run_result()
        return Publishing(skills, profiles, _carried_by_profiles(profiles_catalog), unpublished)

    def _dbt_artifacts(
        self,
        settings: _Settings,
        publishing: Publishing,
        consumed: Mapping[str, QualifiedName],
        unpublished: Mapping[str, str],
    ) -> tuple[CompileResult, ...]:
        """Compile the semantic views, tools, agents, CoWork profiles, and evals of a dbt project, in order.

        Tools read the dbt relations, agents the views and tools that compiled, CoWork profiles
        the agents and what they compiled against, and evals the tools each agent resolved. The
        agents' and the profiles' load diagnostics are reported once, by their own compilers.
        The checks that span the five follow them, as a result that compiles nothing; then
        what the configuration, the agents, and the profiles say about the tools.

        Diagnostics:
            SST-REF015: a configuration value's `tool()` does not take exactly one name.
            SST-CFG017: a configuration value's `tool()` names no declared member.
            SST-CFG018: a declared tool member is referenced by no agent, no CoWork profile, and
                no configuration value.
        """
        semantic = CompileSemanticViews(self._inputs).run_result()
        # A view the project declares disabled is named as such, not as undeclared, by a tool.
        unpublished = {
            **unpublished,
            **{view_key(name): "it is disabled" for name in semantic.disabled},
        }
        dbt = self._inputs.dbt_catalog()
        tool_catalog = self._inputs.tool_catalog()
        tools = _compile_tools(settings, tool_catalog, dbt)
        loaded, agent_diagnostics = self._inputs.agents(agents_dir=settings.directory("agents_dir", "agents"))
        profiles, profile_diagnostics = self._inputs.cowork_profiles(
            cowork_profiles_dir=settings.directory("cowork_profiles_dir", "cowork_profiles")
        )
        # Loading resolves each agent's settings, `enabled` among them; the rest are applied here.
        tags = declared_tags(settings.tree.get("tags"), settings.target)
        settled = tuple(settled_agent(model, settings.target, tags) for model in loaded)
        agent_models = tuple(model for model, _ in settled)
        agent_diagnostics = DiagnosticBag((*agent_diagnostics, *(item for _, found in settled for item in found)))
        enabled = tuple(model for model in agent_models if model.enabled)
        context = self._agent_context(
            settings, enabled, semantic, tool_catalog, consumed, publishing, unpublished, pinned_extensions(profiles)
        )
        contexts: dict[str, AgentCompileContext] = {}
        for model in enabled:
            # The first of two agents with one name keeps it, as its published location does.
            contexts.setdefault(model.key, _agent_environment(settings, context, model))
        agents = CompileAgents(enabled, agent_diagnostics, context, contexts=contexts).run_result()
        cowork = compile_cowork_profiles(
            profiles,
            profile_diagnostics,
            enabled,
            context,
            target=settings.target,
            tags=tags,
            place=lambda models: _published_names(settings, models, "cowork_profiles"),
        )
        eval_catalog, evals = self._evals(enabled, profiles, agents, cowork, context)
        compiled = (semantic, tools, agents, cowork, evals)
        # Every agent and profile counts as a consumer, enabled or not: disabling one does not
        # orphan its tools. So does a configuration value's `tool()`.
        calls = (
            *(tool.member_reference for model in agent_models for tool in model.tools if tool.member_reference),
            *(
                stage
                for model in agent_models
                for tool in model.tools
                if tool.stage_path and (stage := stage_member(tool.stage_path))
            ),
            *(item.name for model in profiles for item in model.subagents if item.reference == "tool"),
            *(stage for member in tool_catalog.members for path in member.imports if (stage := stage_member(path))),
            *config_tool_calls(settings.tree),
        )
        tool_config = CompileResult(
            (),
            DiagnosticBag(
                (
                    *config_tool_references(settings.tree, tool_catalog),
                    *unreferenced_tool_members(tool_catalog, calls),
                )
            ),
        )
        return (*compiled, cross_artifact_result(compiled, eval_catalog, POSITIONS), tool_config)

    def _evals(
        self,
        enabled: tuple[AgentModel, ...],
        profiles: tuple[CoworkProfileModel, ...],
        agents: CompileResult,
        cowork: CompileResult,
        context: AgentCompileContext,
    ) -> tuple[EvalCatalog, CompileResult]:
        """Load the evals of the enabled agents and CoWork profiles, then compile them.

        An agent's eval checks its expected tools against the tools the agent resolved. A
        profile's eval, a Snowflake private preview (PrPr), compiles against the profile as it
        compiled, with the subagents its router names on this target.
        """
        resolved_tools = {
            item.resolved.model.name.casefold(): tuple(sorted(item.resolved.agent_facing_tool_names))
            for item in agents.compiled
            if isinstance(item, CompiledAgent)
        }
        catalog = self._inputs.eval_catalog(
            enabled,
            agent_tool_names=resolved_tools,
            cowork_profiles=tuple(model for model in profiles if model.enabled),
        )
        profile_targets = {
            item.resolved.model.name.casefold(): profile_subject_target(item.target, item.resolved)
            for item in cowork.compiled
            if isinstance(item, CompiledCoworkProfile)
        }
        evals = CompileEvals(catalog, agent_targets=dict(context.agents), profile_targets=profile_targets)
        return catalog, evals.run_result()

    def _agent_context(
        self,
        settings: _Settings,
        enabled: tuple[AgentModel, ...],
        semantic: CompileResult,
        tool_catalog: ToolCatalog,
        consumed: Mapping[str, QualifiedName],
        publishing: Publishing,
        unpublished: Mapping[str, str],
        profile_pins: frozenset[str],
    ) -> AgentCompileContext:
        """Gather what compiling the agents resolves against, with the `agents:` block root's environment.

        Each agent's published name is in the location its settings resolve to; the first of
        two agents with one name keeps it. The commit is read last, once everything else
        resolved: it becomes `sha_version`.

        Args:
            profile_pins: The artifact keys of the skills and plugins a CoWork profile pins,
                which have a consumer even when no agent references them.
        """
        defaults = settings.block("agents")
        raw_models = settings.block("snowflake").get("orchestration_models")
        allowed_models = (
            frozenset(str(value) for value in raw_models) if isinstance(raw_models, list) else frozenset(("auto",))
        )
        skill_pins, plugin_pins, plugin_members = extension_pins(publishing.skills)
        semantic_targets = {item.name.casefold(): item.rendered_artifact.target for item in semantic.compiled}
        database, schema = settings.location("agents")
        return AgentCompileContext(
            semantic_targets,
            tool_catalog,
            _published_names(settings, enabled, "agents"),
            consumed,
            _skill_variables(settings.tree, self._inputs.git_sha()),
            database,
            schema,
            target_text(defaults.get("+warehouse"), settings.target, settings.target.warehouse),
            config_int(defaults.get("+query_timeout")),
            allowed_models=allowed_models,
            skills=skill_pins,
            plugins=plugin_pins,
            consumed=plugin_members | publishing.desktop_consumed | profile_pins,
            unpublished=MappingProxyType(dict(unpublished)),
            allow_unknown_keys=settings.block("snowflake").get("allow_unknown_keys") is not False,
            avatar_allowlist=_avatar_allowlist(settings.block("snowflake")),
            extra_tool_types=_extra_tool_types(settings.block("snowflake")),
            tool_locations=MappingProxyType(
                {
                    name: (place.database, place.schema)
                    for name, place in _tool_placements(settings, tool_catalog).items()
                }
            ),
        )


def _skill_variables(tree: Mapping[str, object], sha: str) -> dict[str, str]:
    """Return what `{{ var('<name>') }}` in a skill's version reads: `vars:`, and `sha_version`.

    `vars:` is read as configuration templates read it; `sha_version` is supplied by SST, and
    `vars:` may not declare it. A variable declared with no value resolves to nothing, so a
    version naming it is SST-VAL538.
    """
    declared = tree.get("vars")
    entries = declared.items() if isinstance(declared, Mapping) else ()
    variables = {str(key): str(value) for key, value in entries if value is not None}
    return {**variables, "sha_version": sha}


def _agent_environment(settings: _Settings, context: AgentCompileContext, model: AgentModel) -> AgentCompileContext:
    """Return the context an agent compiles against: the shared one, with its tools' execution environment."""
    warehouse, query_timeout = agent_environment(model, settings.target)
    return replace(context, warehouse=warehouse, query_timeout=query_timeout)


def _published_names(
    settings: _Settings, models: tuple[AgentModel, ...] | tuple[CoworkProfileModel, ...], block: str
) -> dict[str, QualifiedName]:
    """Return where each agent, or each CoWork profile, publishes, by casefolded name.

    Each is in the location its `block` settings resolve to; the first of two with one name
    keeps it.

    Diagnostics:
        SST-CFG008: a part of a location renders to no Snowflake identifier; the target's own is used.
    """
    targets: dict[str, QualifiedName] = {}
    for model in models:
        location = settings.place(model.settings, block, model.key)
        targets.setdefault(model.name.casefold(), QualifiedName.from_parts(*location, model.name))
    return targets


def _extra_tool_types(snowflake: Mapping[str, object]) -> frozenset[str]:
    """Read `snowflake.tool_types`: the tool types the project adds to the shipped ones."""
    value = snowflake.get("tool_types")
    return frozenset(str(item) for item in value if isinstance(item, str)) if isinstance(value, list) else frozenset()


def _consumed_collisions(skills: CompileResult, consumed: Mapping[str, QualifiedName]) -> tuple[Diagnostic, ...]:
    """Report each published skill or plugin whose name an extension under `extensions:` holds."""
    return namespace_collisions(
        ((item.artifact_key, item.artifact_type, item.name) for item in skills.compiled),
        {name: f"extension '{location.sql}'" for name, location in consumed.items()},
    )


def _avatar_allowlist(snowflake: Mapping[str, object]) -> frozenset[str] | None:
    """Return `snowflake.profile.avatar_allowlist`; None when it is not configured as a list."""
    allowlist = config_block(snowflake.get("profile")).get("avatar_allowlist")
    return frozenset(str(value) for value in allowlist) if isinstance(allowlist, list) else None


def consumed_extensions(
    config: Mapping[str, object], target: TargetIdentity, *, file: str = CONFIG_FILE
) -> tuple[dict[str, QualifiedName], tuple[Diagnostic, ...]]:
    """Resolve `extensions:`: the extensions agents consume and this project does not publish.

    An entry resolves to its `fqn:`, else to `default_prefix` plus its name as an identifier;
    both may name `{{ target.* }}`.

    Returns:
        The resolved names by casefolded entry name, and one diagnostic per entry that does
        not resolve, in entry order.

    Diagnostics:
        SST-CFG036: an entry's name does not parse, the block sets no `default_prefix`, or
            the entry's key is not a single identifier.
    """
    block = config_block(config.get("extensions"))
    prefix = target_text(block.get("default_prefix"), target, None)
    resolved: dict[str, QualifiedName] = {}
    diagnostics: list[Diagnostic] = []
    for name, entry in block.items():
        if name == "default_prefix":
            continue
        fqn = target_text(entry.get("fqn"), target, None) if isinstance(entry, dict) else None
        try:
            if fqn:
                resolved[name.casefold()] = QualifiedName.parse(fqn)
                continue
            if prefix and "." not in name:
                resolved[name.casefold()] = QualifiedName.parse(f"{prefix}.{extension_identifier(name)}")
                continue
        except ValueError as exc:
            diagnostics.append(_unqualified_extension(name, f"its name does not parse ({exc})", file))
            continue
        reason = "the block sets no default_prefix" if not prefix else "the key is not a single identifier"
        diagnostics.append(_unqualified_extension(name, reason, file))
    return resolved, tuple(diagnostics)


def _unqualified_extension(name: str, reason: str, file: str) -> Diagnostic:
    return D(
        "SST-CFG036",
        origin=Origin(file),
        subject=f"config:extensions.{name}",
        block="extensions",
        name=name,
        reason=reason,
    )


def _object_in(name: str, database: str, schema: str) -> QualifiedName:
    """Qualify an object name in the block's database and schema, unless it is qualified already.

    `name` is one `_Settings.setting` checked as an `OBJECT_NAME`, so it parses.
    """
    return QualifiedName.parse(name) if "." in name else QualifiedName.from_parts(database, schema, name)


def _setting_key(block: str, setting: Setting) -> str:
    """Name where a setting was written: `<block>.+<key>`, a route's dotted path, or `config.<key>`."""
    if setting.source == ROOT_SOURCE:
        return f"{block}.+{setting.key}"
    if setting.source.startswith(ROUTE_PREFIX):
        route = setting.source.removeprefix(ROUTE_PREFIX).replace("/", ".")
        return f"{block}.{route}.+{setting.key}"
    return f"config.{setting.key}"


def _rendered(
    block: str, settings: Mapping[str, Setting], target: TargetIdentity, defaults: Mapping[str, object]
) -> tuple[Setting, ...]:
    """Return an item's settings rendered for the target, with each built-in default it took."""
    keys = block_settings(block)
    own = {key: item for key, item in settings.items() if key in keys}
    rendered = {
        key: Setting(key, target_text(item.value, target, "") or "", item.source)
        for key, item in own.items()
        if isinstance(item.value, str)
    }
    return with_defaults({**own, **rendered}, defaults)


def _catalog_channel(settings: _Settings, block: str, item: Skill | Plugin) -> CatalogChannel | None:
    """Return where a skill or plugin publishes, as its settings resolve; None without a `+bundle_stage`.

    Its settings resolve through the one precedence chain: its own `config:`, then the closest
    folder route over its folder, then `block`'s root. Its database and schema are its settings',
    else the target's; its version prefix and certification likewise, else the defaults. Its
    block's root location is its home.
    """
    resolved = resolve_settings(settings.tree.get(block), item.folder, item.config)
    if not isinstance(setting_value(resolved, "bundle_stage"), str):
        return None
    bundle_stage = settings.setting(resolved, "bundle_stage", "", block, item.key, KeyKind.OBJECT_NAME)
    if not bundle_stage:
        # Empty, or no Snowflake name and reported: either way the item has nowhere to publish.
        return None
    target = settings.target
    database, schema = settings.place(resolved, block, item.key)
    prefix = target_text(setting_value(resolved, "version_prefix"), target, DEFAULT_VERSION_PREFIX)
    prefix_setting = resolved.get("version_prefix")
    defaults: dict[str, object] = {
        "database": database,
        "schema": schema,
        "certified": False,
        "version_prefix": DEFAULT_VERSION_PREFIX,
    }
    return CatalogChannel(
        database=database,
        schema=schema,
        bundle_stage=_object_in(bundle_stage, database, schema),
        version_prefix=prefix or DEFAULT_VERSION_PREFIX,
        certified=config_bool(setting_value(resolved, "certified")) is True,
        home=settings.location(block),
        prefix_key=_setting_key(block, prefix_setting) if prefix_setting else f"{block}.+version_prefix",
        settings=_rendered(block, resolved, target, defaults),
    )


def _desktop_channel(settings: _Settings, profile: CocoDesktopProfile) -> DesktopChannel | None:
    """Return where a profile publishes, from the settings it resolves; None without a `+stage`.

    Its settings resolve over the `coco_desktop_profiles:` route of its folder and the block root, below its
    own `config:`. The stage is qualified in its database and schema unless written qualified;
    the registry defaults to the one Desktop reads, and a bare name is qualified the same way.

    Diagnostics:
        SST-CFG008: `+name_suffix` holds more than letters, digits, `_`, and `-`; no suffix is used.
    """
    resolved = resolve_settings(settings.tree.get("coco_desktop_profiles"), (profile.name,), profile.config)
    if not isinstance(setting_value(resolved, "stage"), str):
        return None
    stage = settings.setting(resolved, "stage", "", "coco_desktop_profiles", profile.key, KeyKind.OBJECT_NAME)
    if not stage:
        return None
    target = settings.target
    database, schema = settings.place(resolved, "coco_desktop_profiles", profile.key)
    registry = settings.setting(
        resolved, "registry_table", DESKTOP_REGISTRY, "coco_desktop_profiles", profile.key, KeyKind.OBJECT_NAME
    )
    defaults: dict[str, object] = {
        "database": database,
        "schema": schema,
        "registry_table": DESKTOP_REGISTRY,
        "name_suffix": "",
    }
    return DesktopChannel(
        stage=_object_in(stage, database, schema),
        registry=_object_in(registry, database, schema),
        name_suffix=_name_suffix(settings, resolved, profile),
        settings=_rendered("coco_desktop_profiles", resolved, target, defaults),
    )


def _name_suffix(settings: _Settings, resolved: Mapping[str, Setting], profile: CocoDesktopProfile) -> str:
    setting = resolved.get("name_suffix")
    if setting is None:
        return ""
    suffix = target_text(setting.value, settings.target, "") or ""
    if re.fullmatch(NAME_SUFFIX_PATTERN, suffix):
        return suffix
    own = setting.source == CONFIG_SOURCE
    key = _setting_key("coco_desktop_profiles", setting)
    settings.problems.append(
        D(
            "SST-CFG008",
            origin=None if own else Origin(settings.file),
            subject=profile.key if own else f"config:{key}",
            key=key,
            found=repr(suffix),
            expected="up to 64 letters, digits, underscores, or hyphens",
        )
    )
    return ""


def _blocked(skills: CompileResult, kind: str) -> frozenset[str]:
    """Name the skills, or the plugins, that an error keeps from compiling."""
    prefix = artifact_key(kind, "")
    return frozenset(
        split_artifact_key(str(item.subject))[1]
        for item in skills.diagnostics
        if item.blocks and str(item.subject or "").startswith(prefix)
    )


def _carried_by_profiles(catalog: CocoDesktopProfileCatalog) -> frozenset[str]:
    """Return the artifact keys of the skills and plugins some profile, or the shared profile, carries."""
    skills = {
        *(catalog.shared.skills if catalog.shared is not None else ()),
        *(name for item in catalog.profiles for name in item.skills),
    }
    plugins = {name for item in catalog.profiles for name in item.plugins}
    return frozenset(
        (*(artifact_key("skill", name) for name in skills), *(artifact_key("plugin", name) for name in plugins))
    )


def _tool_placements(settings: _Settings, catalog: ToolCatalog) -> dict[str, ToolPlacement]:
    """Return where each defined member publishes and what its DDL renders, by casefolded name.

    A member's settings resolve through the one precedence chain: its own `config:`, then the
    closest `tools:` folder route over its file, then the block root. Its database and schema
    are its settings', else the target's; its warehouse likewise, else the target's; and a
    procedure runs as its caller unless a setting says otherwise. The first of two members
    with one name keeps the placement, as SST-VAL001 reports the second.
    """
    placements: dict[str, ToolPlacement] = {}
    for member in catalog.managed:
        placements.setdefault(member.name.casefold(), _tool_placement(settings, member))
    return placements


def _tool_placement(settings: _Settings, member: ToolMember) -> ToolPlacement:
    resolved = resolve_settings(settings.tree.get("tools"), member.folder, member.config)
    target = settings.target
    database, schema = settings.place(resolved, "tools", artifact_key("tool", member.name.casefold()))
    warehouse = target_text(setting_value(resolved, "warehouse"), target, target.warehouse)
    defaults: dict[str, object] = {
        "database": database,
        "schema": schema,
        "warehouse": target.warehouse,
        "execute_as": "caller",
        "immutable": False,
    }
    return ToolPlacement(
        database=database,
        schema=schema,
        warehouse=warehouse or target.warehouse,
        target_lag=target_text(setting_value(resolved, "target_lag"), target, None),
        embedding_model=target_text(setting_value(resolved, "embedding_model"), target, None),
        execute_as=target_text(setting_value(resolved, "execute_as"), target, "caller"),
        settings=_rendered("tools", resolved, target, defaults),
    )


def _compile_tools(settings: _Settings, catalog: ToolCatalog, dbt: DbtCatalog) -> CompileResult:
    """Compile the tools, each defined member where its own resolved settings place it."""
    target = settings.target
    return CompileTools(
        catalog,
        default=ToolPlacement(target.database.folded, target.schema.folded, target.warehouse, None, None, "caller"),
        placements=_tool_placements(settings, catalog),
        dbt_relations={model.name: model.relation_name for model in dbt.models},
        dbt_materializations={model.name: model.materialized for model in dbt.models},
    ).run_result()
