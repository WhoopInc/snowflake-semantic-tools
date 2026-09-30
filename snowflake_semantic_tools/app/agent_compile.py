"""Resolve and validate authored Cortex Agents, then render complete specs."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from hashlib import sha256
from types import MappingProxyType
from typing import Mapping

from ..domain.model.agent import (
    KNOWN_AGENT_TOOL_TYPES,
    RESERVED_AGENT_ALIASES,
    AgentModel,
    AgentSkill,
    AgentTool,
    ResolvedAgent,
    ResolvedAgentTool,
)
from ..domain.model.diagnostic import D, Diagnostic, DiagnosticBag, Severity
from ..domain.model.identifier import QualifiedName
from ..domain.model.lifecycle import OwnershipMarker, ProbeKind, RenderedArtifact, SmokeProbe
from ..domain.model.registry import GrantPreservation
from ..domain.model.tool import ToolCatalog, ToolKind, ToolMember
from ..domain.render.agent import desired_agent_definition, render_agent_json, render_agent_spec
from .compile import CompileResult


@dataclass(frozen=True, slots=True)
class AgentCompileContext:
    semantic_views: Mapping[str, QualifiedName]
    tools: ToolCatalog
    agents: Mapping[str, QualifiedName]
    extensions: Mapping[str, QualifiedName]
    variables: Mapping[str, str]
    database: str
    schema: str
    warehouse: str | None
    query_timeout: int | None
    orchestration_model: str
    budget_seconds: int | None
    budget_tokens: int | None
    tool_not_accessible: str | None
    analytical_search: bool | None
    alias: str | None
    allowed_models: frozenset[str]
    skills: Mapping[str, ExtensionPin] = field(default_factory=lambda: MappingProxyType({}))
    plugins: Mapping[str, ExtensionPin] = field(default_factory=lambda: MappingProxyType({}))
    # Skills a plugin or profile already consumes, so SST-VAL804 does not report them.
    consumed: frozenset[str] = frozenset()
    # Declared extensions with no version to pin -- `skill:<name>`, `plugin:<name>`,
    # or `extension:<name>` -- mapped to the reason, so a reference to one names
    # the cause instead of reporting the name as undeclared.
    unpublished: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))


@dataclass(frozen=True, slots=True)
class ExtensionPin:
    """An extension this project publishes, as an agent reference resolves it."""

    key: str
    target: QualifiedName
    alias: str
    members: tuple[str, ...] = ()
    has_scripts: bool = False


@dataclass(frozen=True, slots=True)
class CompiledAgent:
    resolved: ResolvedAgent
    target: QualifiedName
    payload: str
    definition_fingerprint: str
    stage_path: str = ""
    temporary: bool = False

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
        create, update, update_live = _agent_programs(
            self.resolved.model,
            self.target,
            self.stage_path,
            temporary=self.temporary,
            payload=self.payload,
        )
        artifact = RenderedArtifact.create(
            key=self.artifact_key,
            artifact_type="agent",
            target=self.target,
            ddl=self.payload,
            object_type="AGENT",
            render_dialect="json",
            grant_preservation=GrantPreservation.NONE,
            statements=create,
            upload_path=(None if self.temporary else f"@{self.stage_path.rstrip('/')}/agent_spec.yaml"),
            upload_content=(None if self.temporary else self.payload.encode("utf-8")),
            temporary=self.temporary,
            create_statements=create,
            update_statements=update,
            update_live_statements=update_live,
            desired_alias=self.resolved.model.alias,
            desired_tags=tuple(name for name, _ in self.resolved.model.tags),
            depends_on=self.resolved.depends_on,
            smoke=(
                SmokeProbe(
                    f"{self.artifact_key}:describe",
                    ProbeKind.DESCRIBE,
                    f"DESCRIBE AGENT {self.target.sql}",
                ),
            ),
        )
        return replace(artifact, fingerprint=self.definition_fingerprint)

    def rendered_for_publish(self, manifest_id: str) -> RenderedArtifact:
        artifact = self.rendered_artifact
        expected_marker = OwnershipMarker(manifest_id, artifact.fingerprint)
        metadata = _agent_metadata_statements(self.resolved.model, self.target, expected_marker.text)
        return replace(
            artifact,
            statements=(*artifact.statements, *metadata),
            create_statements=(*artifact.create_statements, *metadata),
            update_statements=(*artifact.update_statements, *metadata),
            update_live_statements=(*artifact.update_live_statements, *metadata),
            expected_marker=expected_marker,
        )


class CompileAgents:
    def __init__(
        self, models: tuple[AgentModel, ...], diagnostics: DiagnosticBag, context: AgentCompileContext
    ) -> None:
        self._models = models
        self._diagnostics = diagnostics
        self._context = context

    def run_result(self) -> CompileResult:
        diagnostics: list[Diagnostic] = list(self._diagnostics)
        names: dict[str, AgentModel] = {}
        display_names: dict[str, AgentModel] = {}
        compiled: list[CompiledAgent] = []
        resolved_agents: list[ResolvedAgent] = []
        graph: dict[str, tuple[str, ...]] = {}
        for model in self._models:
            folded = model.name.casefold()
            if folded in names:
                diagnostics.append(D("SST-VAL001", type="agent", name=model.name, origin=model.origin))
            names[folded] = model
            if model.profile.display_name:
                other = display_names.get(model.profile.display_name.casefold())
                if other is not None:
                    diagnostics.append(
                        D(
                            "SST-VAL549",
                            artifact=model.name,
                            value=model.profile.display_name,
                            other=other.name,
                            origin=model.origin,
                        )
                    )
                display_names[model.profile.display_name.casefold()] = model
            effective = _inherit(model, self._context)
            resolved, agent_diagnostics = _resolve_agent(effective, self._context)
            resolved_agents.append(resolved)
            diagnostics.extend(agent_diagnostics)
            graph[folded] = tuple(
                str(tool.resources.get("identifier", "")).casefold() for tool in resolved.tools if tool.type == "agent"
            )
            payload = render_agent_json(resolved.model, resolved.tools)
            diagnostics.extend(_validate_rendered(resolved, payload, self._context))
            if any(
                diagnostic.severity is Severity.ERROR and diagnostic.subject == model.key for diagnostic in diagnostics
            ):
                continue
            spec = render_agent_spec(resolved.model, resolved.tools)
            definition_fingerprint = sha256(desired_agent_definition(resolved.model, spec)).hexdigest()
            target = QualifiedName.from_parts(self._context.database, self._context.schema, model.name)
            compiled.append(CompiledAgent(resolved, target, payload, definition_fingerprint))
        cycle = _cycle(graph, names)
        if cycle:
            diagnostics.append(D("SST-REF022", cycle=" -> ".join(cycle)))
            compiled = []
        if self._models:
            referenced = {dependency for agent in resolved_agents for dependency in agent.skill_dependencies}
            for pin in (*self._context.skills.values(), *self._context.plugins.values()):
                if pin.key not in referenced and pin.key not in self._context.consumed:
                    diagnostics.append(D("SST-VAL804", artifact=pin.key, value=pin.alias, subject=pin.key))
        return CompileResult(tuple(compiled), DiagnosticBag(diagnostics))


def _inherit(model: AgentModel, context: AgentCompileContext) -> AgentModel:
    return replace(
        model,
        orchestration_model=(
            model.orchestration_model if model.orchestration_model != "auto" else context.orchestration_model
        ),
        budget_seconds=model.budget_seconds or context.budget_seconds,
        budget_tokens=model.budget_tokens or context.budget_tokens,
        tool_not_accessible=model.tool_not_accessible or context.tool_not_accessible,
        analytical_search=(
            model.analytical_search if model.analytical_search is not None else context.analytical_search
        ),
        alias=model.alias or context.alias,
    )


def _resolve_agent(model: AgentModel, context: AgentCompileContext) -> tuple[ResolvedAgent, tuple[Diagnostic, ...]]:
    diagnostics: list[Diagnostic] = []
    tools: list[ResolvedAgentTool] = []
    for authored in model.tools:
        resolved, problems = _resolve_tool(model, authored, context)
        diagnostics.extend(problems)
        if resolved is not None:
            tools.append(resolved)
    names: dict[str, ResolvedAgentTool] = {}
    for tool in tools:
        if tool.name in names:
            diagnostics.append(D("SST-VAL514", artifact=model.name, name=tool.name, subject=model.key))
        for other in names.values():
            if other.name.casefold() == tool.name.casefold() and other.name != tool.name:
                diagnostics.append(D("SST-VAL515", artifact=model.name, a=other.name, b=tool.name, subject=model.key))
        names[tool.name] = tool
    if model.alias and model.alias.upper() in RESERVED_AGENT_ALIASES:
        diagnostics.append(D("SST-PRS025", artifact=model.name, value=model.alias, subject=model.key))
    if model.orchestration_model not in context.allowed_models:
        diagnostics.append(D("SST-VAL543", artifact=model.name, found=model.orchestration_model, subject=model.key))
    if model.tool_not_accessible not in (None, "accept", "reject", "legacy"):
        diagnostics.append(
            D("SST-VAL545", artifact=model.name, detail=f"is {model.tool_not_accessible!r}", subject=model.key)
        )
    if model.analytical_search and not any(tool.type == "cortex_search" for tool in tools):
        diagnostics.append(D("SST-VAL546", artifact=model.name, subject=model.key))
    skills, dependencies, skill_diagnostics = _resolve_skills(model, context)
    diagnostics.extend(skill_diagnostics)
    agent = ResolvedAgent(
        replace(model, skills=skills),
        tuple(tools),
        DiagnosticBag(diagnostics),
        skill_dependencies=dependencies,
    )
    return agent, tuple(diagnostics)


def _resolve_skills(
    model: AgentModel,
    context: AgentCompileContext,
) -> tuple[tuple[AgentSkill, ...], tuple[str, ...], tuple[Diagnostic, ...]]:
    """Pin owned extensions to their published alias; check consumed ones are pinned."""
    diagnostics: list[Diagnostic] = []
    resolved: list[AgentSkill] = []
    dependencies: list[str] = []
    executes_code = any(tool.type == "code_execution" for tool in model.tools)
    for skill in model.skills:
        label = skill.name or skill.path
        if skill.source_type == "STAGE":
            diagnostics.append(D("SST-VAL539", artifact=model.name, name=label, subject=model.key))
            resolved.append(skill)
            continue
        if skill.version_var == "sha_version":
            diagnostics.append(D("SST-VAL839", artifact=model.name, name=label, subject=model.key))
            continue
        if skill.ref in ("skill", "plugin"):
            pins = context.skills if skill.ref == "skill" else context.plugins
            pin = pins.get(skill.path)
            if pin is None:
                reason = context.unpublished.get(f"{skill.ref}:{skill.path}")
                if reason is not None:
                    diagnostics.append(
                        D(
                            "SST-VAL856",
                            artifact=model.name,
                            kind=skill.ref,
                            name=skill.path,
                            reason=reason,
                            subject=model.key,
                            origin=model.origin,
                        )
                    )
                else:
                    code = "SST-REF032" if skill.ref == "skill" else "SST-REF036"
                    diagnostics.append(D(code, name=skill.path, subject=model.key, origin=model.origin))
                continue
            if skill.version or skill.version_var:
                diagnostics.append(
                    D("SST-VAL838", artifact=model.name, name=label, kind=skill.ref, path=skill.path, subject=model.key)
                )
                continue
            if skill.ref == "skill" and not skill.name:
                diagnostics.append(D("SST-VAL540", artifact=model.name, path=skill.path, subject=model.key))
                continue
            if skill.name and skill.name not in pin.members:
                expected = (
                    f"'{skill.path}'"
                    if skill.ref == "skill"
                    else "one of the plugin's members: " + ", ".join(pin.members)
                )
                diagnostics.append(
                    D("SST-VAL840", artifact=model.name, name=skill.name, expected=expected, subject=model.key)
                )
                continue
            if pin.has_scripts and not executes_code:
                diagnostics.append(
                    D("SST-VAL814", artifact=model.name, name=f"{skill.ref}('{skill.path}')", subject=model.key)
                )
            resolved.append(replace(skill, path=pin.target.sql, version=pin.alias))
            dependencies.append(pin.key)
            continue
        owned = next(
            (
                kind
                for kind, pins in (("skill", context.skills), ("plugin", context.plugins))
                if skill.path in pins or f"{kind}:{skill.path}" in context.unpublished
            ),
            None,
        )
        if owned is not None:
            diagnostics.append(D("SST-REF037", artifact=model.name, name=skill.path, kind=owned, subject=model.key))
            continue
        target = context.extensions.get(skill.path.casefold())
        if target is None:
            reason = context.unpublished.get(f"extension:{skill.path.casefold()}")
            if reason is not None:
                diagnostics.append(
                    D(
                        "SST-VAL856",
                        artifact=model.name,
                        kind="extension",
                        name=skill.path,
                        reason=reason,
                        subject=model.key,
                        origin=model.origin,
                    )
                )
            else:
                diagnostics.append(D("SST-REF013", name=skill.path or label, subject=model.key))
            continue
        version = context.variables.get(skill.version_var, "") if skill.version_var else skill.version
        if not version or version.upper() == "LIVE":
            diagnostics.append(D("SST-VAL538", artifact=model.name, name=label, subject=model.key))
            continue
        resolved.append(replace(skill, path=target.sql, version=version))
    return tuple(resolved), tuple(dict.fromkeys(dependencies)), tuple(diagnostics)


def _resolve_tool(
    agent: AgentModel,
    authored: AgentTool,
    context: AgentCompileContext,
) -> tuple[ResolvedAgentTool | None, tuple[Diagnostic, ...]]:
    diagnostics: list[Diagnostic] = []
    if authored.type not in KNOWN_AGENT_TOOL_TYPES:
        diagnostics.append(
            D("SST-RND012", artifact=agent.name, found=authored.type, subject=agent.key, origin=authored.origin)
        )
        return None, tuple(diagnostics)
    name = authored.name or ""
    resources: dict[str, object] = {}
    depends_on: tuple[str, ...] = ()
    description = authored.description or ""
    warehouse = authored.warehouse
    if authored.type == "cortex_analyst_text_to_sql":
        if not authored.semantic_view or authored.name:
            diagnostics.append(
                D(
                    "SST-VAL520",
                    artifact=agent.name,
                    name=authored.name or "",
                    count=int(bool(authored.semantic_view)),
                    subject=agent.key,
                )
            )
            return None, tuple(diagnostics)
        target = context.semantic_views.get(authored.semantic_view.casefold())
        if target is None:
            diagnostics.append(D("SST-REF011", name=authored.semantic_view, subject=agent.key))
            return None, tuple(diagnostics)
        name = target.name.folded
        resources["semantic_view"] = target.sql
        resources.update(
            _execution_environment(
                authored.warehouse or context.warehouse,
                authored.query_timeout or context.query_timeout,
            )
        )
        depends_on = (f"semantic_view:{authored.semantic_view.casefold()}",)
    elif authored.type in ("cortex_search", "generic"):
        if not authored.backing or not name:
            diagnostics.append(
                D(
                    "SST-VAL521",
                    artifact=agent.name,
                    name=name,
                    field="search_service" if authored.type == "cortex_search" else "identifier",
                    subject=agent.key,
                )
            )
            return None, tuple(diagnostics)
        backing, problems = context.tools.resolve(*authored.backing)
        diagnostics.extend(problems)
        if backing is None:
            return None, tuple(diagnostics)
        expected = (
            (ToolKind.CORTEX_SEARCH_SERVICE.value,)
            if authored.type == "cortex_search"
            else (ToolKind.PROCEDURE.value, ToolKind.FUNCTION.value)
        )
        if backing.type not in expected:
            diagnostics.append(
                D(
                    "SST-REF020",
                    ref_function="tool",
                    name=backing.name,
                    found=backing.type,
                    expected=" or ".join(expected),
                    subject=agent.key,
                )
            )
            return None, tuple(diagnostics)
        if not description:
            description = backing.description or ""
        warehouse = warehouse or backing.warehouse or context.warehouse
        if authored.type == "cortex_search":
            search_relation: QualifiedName | None = QualifiedName.from_parts(
                context.database, context.schema, backing.name
            )
            if backing.ownership.value == "reference":
                search_relation, relation_problems = context.tools.relation(backing)
                diagnostics.extend(relation_problems)
            if search_relation is None:
                return None, tuple(diagnostics)
            resources = _search_resources(authored, backing, search_relation, warehouse, context.query_timeout)
        else:
            routine_relation, relation_problems = context.tools.relation(backing)
            diagnostics.extend(relation_problems)
            if routine_relation is None:
                routine_relation = QualifiedName.from_parts(context.database, context.schema, backing.name)
            resources = {
                "identifier": routine_relation.sql,
                "type": backing.type,
                **_execution_environment(warehouse, authored.query_timeout or context.query_timeout),
                **dict(authored.passthrough),
            }
            if not authored.input_schema or authored.input_schema.get("type") != "object":
                diagnostics.append(
                    D(
                        "SST-VAL526",
                        artifact=agent.name,
                        name=name,
                        found=authored.input_schema.get("type"),
                        subject=agent.key,
                    )
                )
            diagnostics.extend(_input_schema_diagnostics(agent, name, authored.input_schema))
            if not warehouse:
                diagnostics.append(D("SST-VAL527", artifact=agent.name, name=name, subject=agent.key))
        if backing.artifact_key:
            depends_on = (backing.artifact_key,)
    elif authored.type == "agent":
        agent_backing: ToolMember | None = None
        if authored.agent_ref:
            target = context.agents.get(authored.agent_ref.casefold())
            if target is not None:
                name = name or target.artifact_name
                resources = {"identifier": target.sql, "type": "agent"}
                depends_on = (f"agent:{authored.agent_ref.casefold()}",)
            else:
                diagnostics.append(D("SST-REF012", name=authored.agent_ref, subject=agent.key))
        elif name:
            agent_backing, problems = context.tools.resolve(name)
            diagnostics.extend(problems)
            if agent_backing is not None:
                agent_relation, relation_problems = context.tools.relation(agent_backing)
                diagnostics.extend(relation_problems)
                if agent_relation is not None:
                    resources = {"identifier": agent_relation.sql, "type": "agent"}
        if not resources:
            return None, tuple(diagnostics)
        diagnostics.append(D("SST-VAL528", artifact=agent.name, subject=agent.key))
    elif authored.type == "mcp":
        resources = dict(authored.passthrough)
    else:
        name = name or authored.type
    if not name or not 1 <= len(name) <= 64:
        diagnostics.append(D("SST-VAL513", artifact=agent.name, name=name, size=len(name), subject=agent.key))
    if authored.type == "web_search" and name != "web_search":
        diagnostics.append(D("SST-VAL517", artifact=agent.name, name=name, subject=agent.key))
    if not description:
        diagnostics.append(D("SST-VAL518", artifact=agent.name, name=name, subject=agent.key))
    if authored.query_timeout is not None and authored.query_timeout <= 0:
        diagnostics.append(
            D(
                "SST-PRS016",
                artifact=agent.name,
                field="query_timeout",
                found=authored.query_timeout,
                expected="> 0",
                subject=agent.key,
            )
        )
    return (
        ResolvedAgentTool(
            authored.type,
            name,
            description,
            MappingProxyType(resources),
            authored.input_schema,
            depends_on,
            authored.tool_spec_passthrough,
        ),
        tuple(diagnostics),
    )


def _execution_environment(warehouse: str | None, query_timeout: int | None) -> dict[str, object]:
    if not warehouse and query_timeout is None:
        return {}
    environment: dict[str, object] = {"type": "warehouse"}
    if warehouse:
        environment["warehouse"] = warehouse
    if query_timeout is not None:
        environment["query_timeout"] = query_timeout
    return {"execution_environment": environment}


def _search_resources(
    authored: AgentTool,
    backing: ToolMember,
    relation: QualifiedName,
    warehouse: str | None,
    query_timeout: int | None,
) -> dict[str, object]:
    columns = authored.columns_and_descriptions or MappingProxyType(
        {
            column.name.upper(): {
                "description": column.description,
                "type": column.type,
                "searchable": column.searchable,
                "filterable": column.filterable,
            }
            for column in backing.columns
            if column.searchable or column.filterable
        }
    )
    resources: dict[str, object] = {"search_service": relation.sql}
    if authored.max_results is not None:
        resources["max_results"] = authored.max_results
    for key, value in (
        ("id_column", authored.id_column or backing.id_column or _default_column(backing, "id")),
        ("title_column", authored.title_column or backing.title_column or _default_column(backing, "name")),
    ):
        if value:
            resources[key] = value.upper()
    if columns:
        resources["columns_and_descriptions"] = dict(columns)
    resources.update(_execution_environment(warehouse, authored.query_timeout or query_timeout))
    resources.update(authored.passthrough)
    return resources


def _default_column(backing: ToolMember, suffix: str) -> str | None:
    return next((column.name for column in backing.columns if column.name.casefold().endswith(f"_{suffix}")), None)


def _input_schema_diagnostics(agent: AgentModel, name: str, schema: Mapping[str, object]) -> tuple[Diagnostic, ...]:
    diagnostics: list[Diagnostic] = []
    properties = schema.get("properties")
    properties = properties if isinstance(properties, dict) else {}
    for field, value in properties.items():
        type_name = value.get("type") if isinstance(value, dict) else None
        if type_name not in {"string", "number", "integer", "boolean", "array"}:
            diagnostics.append(D("SST-PRS032", artifact=agent.name, field=field, found=type_name, subject=agent.key))
    required = schema.get("required")
    for field in required if isinstance(required, list) else []:
        if field not in properties:
            diagnostics.append(D("SST-PRS033", artifact=agent.name, field=field, subject=agent.key))
    return tuple(diagnostics)


def _validate_rendered(resolved: ResolvedAgent, payload: str, context: AgentCompileContext) -> tuple[Diagnostic, ...]:
    diagnostics: list[Diagnostic] = []
    size = len(payload.encode("utf-8"))
    if size >= 100_000:
        diagnostics.append(D("SST-VAL511", artifact=resolved.model.name, size=size, subject=resolved.model.key))
    elif size >= 80_000:
        diagnostics.append(D("SST-VAL512", artifact=resolved.model.name, size=size, subject=resolved.model.key))
    return tuple(diagnostics)


def _cycle(graph: Mapping[str, tuple[str, ...]], agents: Mapping[str, AgentModel]) -> tuple[str, ...]:
    names_by_target = {f"{name}".casefold(): name for name in agents}
    normalized: dict[str, tuple[str, ...]] = {}
    for name, targets in graph.items():
        normalized[name] = tuple(
            other
            for target in targets
            for other in names_by_target
            if target.endswith(f".{other}".casefold()) or target == other
        )
    visiting: list[str] = []
    complete: set[str] = set()

    def walk(name: str) -> tuple[str, ...]:
        if name in visiting:
            index = visiting.index(name)
            return tuple(visiting[index:] + [name])
        if name in complete:
            return ()
        visiting.append(name)
        for target in normalized.get(name, ()):
            cycle = walk(target)
            if cycle:
                return cycle
        visiting.pop()
        complete.add(name)
        return ()

    for name in sorted(normalized):
        cycle = walk(name)
        if cycle:
            return cycle
    return ()


def for_publication(
    compiled: CompiledAgent,
    *,
    stage: QualifiedName,
    git_sha: str,
    temporary: bool = False,
) -> CompiledAgent:
    return replace(
        compiled,
        stage_path=f"{stage.sql}/{compiled.name}/{git_sha}",
        temporary=temporary,
    )


def _agent_programs(
    model: AgentModel,
    target: QualifiedName,
    stage_path: str,
    *,
    temporary: bool,
    payload: str,
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    if temporary:
        if "$$" in payload:
            raise ValueError("temporary agent spec contains an unsupported dollar-quote delimiter")
        profile = _profile_json(model)
        statement = (
            f"CREATE OR REPLACE TEMPORARY AGENT {target.sql}"
            + (f" WITH PROFILE = {_sql_string(profile)}" if profile else "")
            + f" FROM SPECIFICATION $${payload.rstrip()}$$"
        )
        return (statement,), (statement,), (statement,)
    if not stage_path:
        return (), (), ()
    create = [f"CREATE AGENT {target.sql}\n  FROM @{stage_path}/"]
    add_version = (
        f"ALTER AGENT {target.sql}\n  ADD VERSION FROM @{stage_path}/\n"
        f"  COMMENT = 'git:{stage_path.rsplit('/', 1)[-1]}'"
    )
    update = [add_version]
    update_live = [f"ALTER AGENT {target.sql} COMMIT", add_version]
    if model.alias:
        alias = f'ALTER AGENT {target.sql}\n  MODIFY VERSION "LAST" SET ALIAS = {_identifier(model.alias)}'
        create.append(alias)
        update.append(alias)
        update_live.append(alias)
    if model.tags:
        pairs = ", ".join(f"{_qualified_or_identifier(name)} = {_sql_string(value)}" for name, value in model.tags)
        tag = f"ALTER AGENT {target.sql}\n  SET TAG {pairs}"
        create.append(tag)
        update.append(tag)
        update_live.append(tag)
    return tuple(create), tuple(update), tuple(update_live)


def _profile_json(model: AgentModel) -> str:
    import json

    value = {
        key: item
        for key, item in (
            ("display_name", model.profile.display_name),
            ("avatar", model.profile.avatar),
            ("color", model.profile.color),
        )
        if item is not None
    }
    return json.dumps(value, separators=(",", ":"))


def _agent_metadata_statements(
    model: AgentModel,
    target: QualifiedName,
    marker: str,
) -> tuple[str, ...]:
    statements: list[str] = []
    profile = _profile_json(model)
    statements.append(f"ALTER AGENT {target.sql} SET PROFILE = {_sql_string(profile)}")
    comment = f"{marker} {model.comment}" if model.comment else marker
    statements.append(f"ALTER AGENT {target.sql} SET COMMENT = {_sql_string(comment)}")
    statements.append(f"ALTER AGENT {target.sql} SET SECURE = {'TRUE' if model.secure else 'FALSE'}")
    return tuple(statements)


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _identifier(value: str) -> str:
    from ..domain.model.identifier import Identifier

    return Identifier.parse(value).sql


def _qualified_or_identifier(value: str) -> str:
    try:
        return QualifiedName.parse(value).sql
    except ValueError:
        return _identifier(value)
