"""Compile a whole project: run every typed compiler over the project's inputs and merge the results.

`CompileProject` reads the project only through a `ProjectInputs` port, so it touches no
file. It resolves each compiler's settings from `sst_config.yml` and the dbt target, runs
the compilers in a fixed order -- which is also the order a broken input is reported in --
and merges their results in DDL order.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from types import MappingProxyType

from snowflake_semantic_tools.app.compile import CompileArtifacts, CompileSemanticViews
from snowflake_semantic_tools.app.compile.agents import AgentCompileContext, CompileAgents, CompiledAgent
from snowflake_semantic_tools.app.compile.base import CompileResult
from snowflake_semantic_tools.app.compile.evals import CompileEvals
from snowflake_semantic_tools.app.compile.profiles import CompileProfiles, DesktopChannel
from snowflake_semantic_tools.app.compile.skills import (
    CatalogChannel,
    CompileSkills,
    extension_pins,
    unpublished_reasons,
)
from snowflake_semantic_tools.app.compile.tools import CompileTools
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Origin, Severity
from snowflake_semantic_tools.domain.model.agent import AgentModel
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key, split_artifact_key
from snowflake_semantic_tools.domain.model.config_schema import (
    CONFIG_FILE,
    config_block,
    config_bool,
    config_int,
    config_text,
    configured_dir,
    skills_configured,
    target_text,
)
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.model.profile import ProfileCatalog
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.model.skill import DEFAULT_VERSION_PREFIX, extension_identifier
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolGroup, ToolOwnership
from snowflake_semantic_tools.domain.ports.project import ProjectInputs
from snowflake_semantic_tools.domain.validate.config import config_tool_references, unreferenced_tool_members

# Where each artifact type sits in the merged stream, which is also the order apply publishes in.
POSITIONS: Mapping[str, int] = MappingProxyType(
    {name: value.ddl_position for name, value in SEMANTIC_REGISTRY.artifacts.items()}
)


@dataclass(frozen=True, slots=True)
class Publishing:
    """What the publishing channels compiled, and what the agents need to know of it.

    Attributes:
        skills: The skills and plugins, as extension versions.
        profiles: The Desktop profiles; empty without the stage channel.
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
    """`sst_config.yml` read against the dbt target: where every compiler's settings come from."""

    tree: Mapping[str, object]
    target: TargetIdentity
    file: str = CONFIG_FILE

    def block(self, name: str) -> dict[str, object]:
        """Return one top-level block; empty when it is absent or not a mapping."""
        return config_block(self.tree.get(name))

    def directory(self, key: str, default: str) -> str:
        """Return the `project.<key>` root, else `default`."""
        return configured_dir(self.tree, key, default)

    def location(self, block: Mapping[str, object]) -> tuple[str, str]:
        """Return the block's `+database` and `+schema`, each defaulting to the target's own."""
        database = self.target.database.folded
        schema = self.target.schema.folded
        return (
            target_text(block.get("+database"), self.target, database) or database,
            target_text(block.get("+schema"), self.target, schema) or schema,
        )


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
        2. Resolve the extensions agents consume, from `skills.extensions`.
        3. Compile skills and plugins, then Desktop profiles, as far as the channels allow.
        4. With a dbt project, compile the semantic views, the tools, the agents, and then the
           evals; each reads what the compilers before it produced.
        5. Merge the results. The diagnostics of the configuration, the target, and
           `skills.extensions` come first, then each compiler's in the order it ran.

        Diagnostics:
            SST-CFG036: a `skills.extensions` entry cannot be qualified.
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
                f"extension:{str(item.context['name']).casefold()}": "its skills.extensions entry cannot be qualified"
                for item in consumed_diagnostics
                if item.code == "SST-CFG036"
            },
        }
        results = [
            CompileResult((), DiagnosticBag((*config.diagnostics, *target.diagnostics, *consumed_diagnostics))),
            publishing.skills,
            publishing.profiles,
        ]
        if config.has_dbt_project:
            results.extend(self._dbt_artifacts(settings, publishing, consumed, unpublished))
        return CompileArtifacts.merge(results, POSITIONS)

    def _publishing(self, settings: _Settings) -> Publishing:
        """Compile skills, plugins, and Desktop profiles, as far as the configured channels allow.

        The catalog is read whatever is configured, so every declared skill and plugin can be
        reported as unpublished with its reason. Without a `skills.catalog` or `skills.stage`
        block nothing compiles; profiles compile only with the stage channel.
        """
        catalog = self._inputs.skill_catalog(
            skills_dir=settings.directory("skills_dir", "skills"),
            plugins_dir=settings.directory("plugins_dir", "plugins"),
        )
        if not skills_configured(settings.tree):
            empty = CompileResult(())
            return Publishing(empty, empty, frozenset(), unpublished_reasons(catalog, empty, _NO_CATALOG))
        skills_config = settings.block("skills")
        catalog_config = skills_config.get("catalog")
        channel = _catalog_channel(settings, skills_config)
        skills = CompileSkills(catalog, channel, config_file=settings.file).run_result()
        unpublished = unpublished_reasons(catalog, skills, _channel_problem(catalog_config, channel))
        stage_config = skills_config.get("stage")
        if not isinstance(stage_config, dict):
            return Publishing(skills, CompileResult(()), frozenset(), unpublished)
        profiles_catalog = self._inputs.profile_catalog(
            profiles_dir=settings.directory("profiles_dir", "profiles"),
            hooks_dir=settings.directory("hooks_dir", "hooks"),
            mcp_servers_dir=settings.directory("mcp_servers_dir", "mcp-servers"),
            commands_dir=settings.directory("commands_dir", "commands"),
        )
        profiles = CompileProfiles(
            profiles_catalog,
            catalog,
            _desktop_channel(settings, skills_config, stage_config),
            catalog_channel=channel is not None,
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
        """Compile the semantic views, tools, agents, and evals of a dbt project, in that order.

        Tools read the dbt relations, agents the views and tools that compiled, and evals the
        tools each agent resolved. The agents' load diagnostics are reported once, by the agents.
        Then what the configuration and the agents say about the tools is reported.

        Diagnostics:
            SST-CFG017: a configuration value's `tool()` names no declared member.
            SST-CFG018: a declared tool member is referenced by no agent.
        """
        semantic = CompileSemanticViews(self._inputs).run_result()
        dbt = self._inputs.dbt_catalog()
        tool_catalog = self._inputs.tool_catalog()
        tools = _compile_tools(settings, tool_catalog, dbt)
        agent_models, agent_diagnostics = self._inputs.agents(agents_dir=settings.directory("agents_dir", "agents"))
        defaults = settings.block("agents")
        enabled = tuple(
            model for model in agent_models if model.enabled and defaults.get("+enabled", True) is not False
        )
        context = self._agent_context(settings, enabled, semantic, tool_catalog, consumed, publishing, unpublished)
        agents = CompileAgents(enabled, agent_diagnostics, context).run_result()
        resolved_tools = {
            item.resolved.model.name.casefold(): tuple(sorted(item.resolved.agent_facing_tool_names))
            for item in agents.compiled
            if isinstance(item, CompiledAgent)
        }
        evals = CompileEvals(
            self._inputs.eval_catalog(enabled, agent_tool_names=resolved_tools),
            agent_targets=dict(context.agents),
        ).run_result()
        # Every agent counts as a consumer, enabled or not: disabling one does not orphan its tools.
        calls = tuple(tool.backing for model in agent_models for tool in model.tools if tool.backing)
        tool_config = CompileResult(
            (),
            DiagnosticBag(
                (
                    *config_tool_references(settings.tree, tool_catalog),
                    *unreferenced_tool_members(tool_catalog, calls),
                )
            ),
        )
        return semantic, tools, agents, evals, tool_config

    def _agent_context(
        self,
        settings: _Settings,
        enabled: tuple[AgentModel, ...],
        semantic: CompileResult,
        tool_catalog: ToolCatalog,
        consumed: Mapping[str, QualifiedName],
        publishing: Publishing,
        unpublished: Mapping[str, str],
    ) -> AgentCompileContext:
        """Gather what compiling the agents resolves against, with the `agents:` defaults.

        The commit is read last, once everything else resolved: it becomes `sha_version`.
        """
        defaults = settings.block("agents")
        raw_models = settings.block("snowflake").get("orchestration_models")
        allowed_models = (
            frozenset(str(value) for value in raw_models) if isinstance(raw_models, list) else frozenset(("auto",))
        )
        skill_pins, plugin_pins, plugin_members = extension_pins(publishing.skills)
        semantic_targets = {item.name.casefold(): item.rendered_artifact.target for item in semantic.compiled}
        database, schema = settings.location(defaults)
        agent_targets = {
            model.name.casefold(): QualifiedName.from_parts(database, schema, model.name) for model in enabled
        }
        target = settings.target
        return AgentCompileContext(
            semantic_targets,
            tool_catalog,
            agent_targets,
            consumed,
            {"sha_version": self._inputs.git_sha()},
            database,
            schema,
            target_text(defaults.get("+warehouse"), target, target.warehouse),
            config_int(defaults.get("+query_timeout")),
            config_text(defaults.get("+orchestration_model"), "auto") or "auto",
            config_int(defaults.get("+budget_seconds")),
            config_int(defaults.get("+budget_tokens")),
            config_text(defaults.get("+tool_not_accessible"), None),
            config_bool(defaults.get("+analytical_search")),
            config_text(defaults.get("+alias"), None),
            allowed_models,
            skills=skill_pins,
            plugins=plugin_pins,
            consumed=plugin_members | publishing.desktop_consumed,
            unpublished=MappingProxyType(dict(unpublished)),
        )


def consumed_extensions(
    config: Mapping[str, object], target: TargetIdentity, *, file: str = CONFIG_FILE
) -> tuple[dict[str, QualifiedName], tuple[Diagnostic, ...]]:
    """Resolve `skills.extensions`: the extensions agents consume and this project does not publish.

    An entry resolves to its `fqn:`, else to `default_prefix` plus its name as an identifier;
    both may name `{{ target.* }}`.

    Returns:
        The resolved names by casefolded entry name, and one diagnostic per entry that does
        not resolve, in entry order.

    Diagnostics:
        SST-CFG036: an entry's name does not parse, the block sets no `default_prefix`, or
            the entry's key is not a single identifier.
    """
    block = config_block(config_block(config.get("skills")).get("extensions"))
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


_NO_CATALOG = "skills.catalog is not configured"


def _unqualified_extension(name: str, reason: str, file: str) -> Diagnostic:
    return D(
        "SST-CFG036",
        origin=Origin(file),
        subject=f"config:skills.extensions.{name}",
        block="skills.extensions",
        name=name,
        reason=reason,
    )


def _version_prefix(skills_config: Mapping[str, object]) -> str:
    return config_text(skills_config.get("+version_prefix"), DEFAULT_VERSION_PREFIX) or DEFAULT_VERSION_PREFIX


def _object_in(name: str, database: str, schema: str) -> QualifiedName:
    """Qualify an object name in the block's database and schema, unless it is qualified already."""
    return QualifiedName.parse(name) if "." in name else QualifiedName.from_parts(database, schema, name)


def _catalog_channel(settings: _Settings, skills_config: Mapping[str, object]) -> CatalogChannel | None:
    """Return the catalog channel `skills.catalog` configures; None without a `+bundle_stage`."""
    catalog_config = skills_config.get("catalog")
    if not isinstance(catalog_config, dict) or not isinstance(catalog_config.get("+bundle_stage"), str):
        return None
    database, schema = settings.location(catalog_config)
    return CatalogChannel(
        database=database,
        schema=schema,
        bundle_stage=_object_in(str(catalog_config["+bundle_stage"]), database, schema),
        version_prefix=_version_prefix(skills_config),
        certified=config_bool(skills_config.get("+certified")) or False,
    )


def _channel_problem(catalog_config: object, channel: CatalogChannel | None) -> str | None:
    """Say why the catalog channel cannot publish anything; None when it can."""
    if not isinstance(catalog_config, dict):
        return _NO_CATALOG
    if channel is None:
        return "skills.catalog sets no +bundle_stage"
    return None


def _desktop_channel(
    settings: _Settings, skills_config: Mapping[str, object], stage_config: Mapping[str, object]
) -> DesktopChannel | None:
    """Return the stage channel `skills.stage` configures; None without a `+stage`."""
    if not isinstance(stage_config.get("+stage"), str):
        return None
    database, schema = settings.location(stage_config)
    return DesktopChannel(
        stage=_object_in(str(stage_config["+stage"]), database, schema),
        registry=_object_in(str(stage_config.get("+registry_table") or "PROFILE_REGISTRY"), database, schema),
        version_prefix=_version_prefix(skills_config),
    )


def _blocked(skills: CompileResult, kind: str) -> frozenset[str]:
    """Name the skills, or the plugins, that an error keeps from compiling."""
    prefix = artifact_key(kind, "")
    return frozenset(
        split_artifact_key(str(item.subject))[1]
        for item in skills.diagnostics
        if item.severity is Severity.ERROR and str(item.subject or "").startswith(prefix)
    )


def _carried_by_profiles(catalog: ProfileCatalog) -> frozenset[str]:
    """Return the artifact keys of the skills and plugins some profile, or the shared profile, carries."""
    skills = {
        *(catalog.shared.skills if catalog.shared is not None else ()),
        *(name for item in catalog.profiles for name in item.skills),
    }
    plugins = {name for item in catalog.profiles for name in item.plugins}
    return frozenset(
        (*(artifact_key("skill", name) for name in skills), *(artifact_key("plugin", name) for name in plugins))
    )


def _compile_tools(settings: _Settings, catalog: ToolCatalog, dbt: DbtCatalog) -> CompileResult:
    """Compile the tools with the `tools:` defaults, a group's own `tools.<group>` block overriding them.

    Each overridden group compiles on its own, with the catalog's diagnostics about its members,
    so every diagnostic is reported once.

    Diagnostics:
        SST-CFG020: a `tools.<group>` override names no declared group, or one with no `define:`.
    """
    defaults = settings.block("tools")
    by_name = {group.name.casefold(): group for group in catalog.groups}
    overrides: dict[str, dict[str, object]] = {}
    problems: list[Diagnostic] = []
    for name, block in defaults.items():
        if name.startswith("+") or not isinstance(block, dict):
            continue
        group = by_name.get(name.casefold())
        reason = (
            "is not declared"
            if group is None
            else "has no define: members, so the override would change nothing"
            if not any(member.ownership is ToolOwnership.DEFINE for member in group.members)
            else None
        )
        if reason is not None:
            problems.append(
                D(
                    "SST-CFG020",
                    origin=Origin(settings.file),
                    subject=f"config:tools.{name}",
                    group=name,
                    reason=reason,
                )
            )
        else:
            overrides[name.casefold()] = {**defaults, **config_block(block)}

    def keys(groups: tuple[ToolGroup, ...]) -> frozenset[str]:
        return frozenset(f"tool:{member.name.casefold()}" for group in groups for member in group.members)

    separate = keys(tuple(by_name[name] for name in overrides))
    shared = tuple(group for group in catalog.groups if group.name.casefold() not in overrides)
    results = [
        _tool_result(
            settings,
            replace(
                catalog,
                groups=shared,
                diagnostics=DiagnosticBag(d for d in catalog.diagnostics if d.subject not in separate),
            ),
            defaults,
            dbt,
        )
    ]
    for name, merged in overrides.items():
        own = keys((by_name[name],))
        alone = replace(
            catalog,
            groups=(by_name[name],),
            diagnostics=DiagnosticBag(d for d in catalog.diagnostics if d.subject in own),
        )
        results.append(_tool_result(settings, alone, merged, dbt))
    return CompileResult(
        tuple(item for result in results for item in result.compiled),
        DiagnosticBag((*problems, *(item for result in results for item in result.diagnostics))),
    )


def _tool_result(
    settings: _Settings, catalog: ToolCatalog, defaults: Mapping[str, object], dbt: DbtCatalog
) -> CompileResult:
    """Compile one part of the tool catalog with `defaults`, resolving each dbt model to its relation."""
    database, schema = settings.location(defaults)
    return CompileTools(
        catalog,
        database=database,
        schema=schema,
        warehouse=target_text(defaults.get("+warehouse"), settings.target, settings.target.warehouse),
        target_lag=config_text(defaults.get("+target_lag"), None),
        embedding_model=config_text(defaults.get("+embedding_model"), None),
        execute_as=config_text(defaults.get("+execute_as"), "caller"),
        dbt_relations={model.name: model.relation_name for model in dbt.models},
    ).run_result()
