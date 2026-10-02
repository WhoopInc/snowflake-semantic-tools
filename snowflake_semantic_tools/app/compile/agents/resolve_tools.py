"""Resolve one authored agent tool into the entry its specification carries.

Each modeled tool type has a resolver in `_RESOLVERS`: a Cortex Analyst tool names a
semantic view, a Cortex Search or generic tool names a tool member, an agent tool names
another agent, and an MCP tool passes its keys through. Every other known type is a
built-in, such as data_to_chart. A resolver appends what it finds to the diagnostics it
is given, in order, and returns None for a tool that cannot be rendered; `_finish` then
checks what every resolved tool shares and builds its entry.

Its diagnostics are reported as each entry resolves against the `AgentCompileContext`, so
they stay with resolution here rather than in `domain.validate`.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from snowflake_semantic_tools.app.compile.agents.context import AgentCompileContext
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.agent import KNOWN_AGENT_TOOL_TYPES, AgentModel, AgentTool, ResolvedAgentTool
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.tool import ToolKind, ToolMember

_SEARCH_SERVICES = (ToolKind.CORTEX_SEARCH_SERVICE.value,)
_ROUTINES = (ToolKind.PROCEDURE.value, ToolKind.FUNCTION.value)
_INPUT_TYPES = frozenset(("string", "number", "integer", "boolean", "array"))

# What an agent tool's name may hold: letters, digits, `_` and `-`.
_TOOL_IDENTIFIER = re.compile(r"[A-Za-z0-9_-]+")


@dataclass(frozen=True, slots=True)
class _Resolution:
    """What a resolver found for one tool, from which `_finish` builds the tool's entry."""

    name: str
    description: str
    resources: dict[str, object] = field(default_factory=dict)
    depends_on: tuple[str, ...] = ()


_Resolver = Callable[[AgentModel, AgentTool, AgentCompileContext, list[Diagnostic]], _Resolution | None]


def resolve_tool(
    agent: AgentModel,
    authored: AgentTool,
    context: AgentCompileContext,
) -> tuple[ResolvedAgentTool | None, tuple[Diagnostic, ...]]:
    """Resolve one authored tool of `agent`, returning None for a tool that cannot be rendered.

    A type the renderer does not know resolves to nothing. Otherwise the type's resolver
    runs, and a tool it resolves is checked for its name, description, and query timeout.
    Every diagnostic reported here names the agent as its subject.

    Returns:
        The resolved tool, or None, and every diagnostic in the order it was found.

    Diagnostics:
        SST-RND012: the tool type is unknown to the renderer.
        SST-VAL520: an Analyst tool names no semantic view, or is given a name as well.
        SST-REF011: an Analyst tool's semantic view is not one that compiled.
        SST-VAL521: a Cortex Search or generic tool has no name, or names no tool member.
        SST-REF010: the tool member a tool names does not resolve (from the tool catalog).
        SST-REF020: the tool member is of a type the tool cannot use.
        SST-REF018: a referenced tool member has no relation for the target (from the catalog).
        SST-REF019: a referenced tool member's relation does not parse (from the catalog).
        SST-VAL526: a generic tool's input schema is not an object.
        SST-PRS032: an input schema property has a type a tool input cannot carry.
        SST-PRS033: an input schema requires a property it does not declare.
        SST-VAL527: a generic tool resolves no warehouse.
        SST-REF012: an agent tool's `agent()` names no enabled agent.
        SST-VAL528: an agent tool resolved, so this spec no longer fixes the tool surface.
        SST-VAL513: the resolved name is empty or longer than 64 characters.
        SST-PRS009: the resolved name holds a character a tool identifier may not.
        SST-VAL517: a web_search tool is not named web_search.
        SST-VAL518: the resolved tool has no description.
        SST-PRS016: the tool's `query_timeout` is not positive.
    """
    diagnostics: list[Diagnostic] = []
    if authored.type not in KNOWN_AGENT_TOOL_TYPES:
        diagnostics.append(
            D("SST-RND012", artifact=agent.name, found=authored.type, subject=agent.key, origin=authored.origin)
        )
        return None, tuple(diagnostics)
    # A known type without a resolver of its own is a built-in.
    resolution = _RESOLVERS.get(authored.type, _builtin)(agent, authored, context, diagnostics)
    if resolution is None:
        return None, tuple(diagnostics)
    tool = _finish(agent, authored, resolution, diagnostics)
    return tool, tuple(diagnostics)


def _analyst(
    agent: AgentModel, authored: AgentTool, context: AgentCompileContext, diagnostics: list[Diagnostic]
) -> _Resolution | None:
    """A Cortex Analyst tool: named after the compiled semantic view it queries."""
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
        return None
    target = context.semantic_views.get(authored.semantic_view.casefold())
    if target is None:
        diagnostics.append(D("SST-REF011", name=authored.semantic_view, subject=agent.key))
        return None
    environment = _execution_environment(
        authored.warehouse or context.warehouse,
        authored.query_timeout or context.query_timeout,
    )
    return _Resolution(
        target.name.folded,
        authored.description or "",
        {"semantic_view": target.sql, **environment},
        (artifact_key("semantic_view", authored.semantic_view.casefold()),),
    )


def _search(
    agent: AgentModel, authored: AgentTool, context: AgentCompileContext, diagnostics: list[Diagnostic]
) -> _Resolution | None:
    """A Cortex Search tool over a search service member, defined here or referenced."""
    backing = _backing(agent, authored, context, diagnostics, authored_key="search_service", expected=_SEARCH_SERVICES)
    if backing is None:
        return None
    warehouse = authored.warehouse or backing.warehouse or context.warehouse
    # A defined member is taken to be in the agents' schema; a referenced one resolves per target.
    relation: QualifiedName | None = QualifiedName.from_parts(context.database, context.schema, backing.name)
    if backing.ownership.value == "reference":
        relation, problems = context.tools.relation(backing)
        diagnostics.extend(problems)
    if relation is None:
        return None
    return _Resolution(
        authored.name or "",
        authored.description or backing.description or "",
        _search_resources(authored, backing, relation, warehouse, context.query_timeout),
        _dependency(backing),
    )


def _generic(
    agent: AgentModel, authored: AgentTool, context: AgentCompileContext, diagnostics: list[Diagnostic]
) -> _Resolution | None:
    """A generic tool over a procedure or function member, called with an object input schema."""
    backing = _backing(agent, authored, context, diagnostics, authored_key="identifier", expected=_ROUTINES)
    if backing is None:
        return None
    name = authored.name or ""
    warehouse = authored.warehouse or backing.warehouse or context.warehouse
    relation, problems = context.tools.relation(backing)
    diagnostics.extend(problems)
    if relation is None:
        relation = QualifiedName.from_parts(context.database, context.schema, backing.name)
    resources: dict[str, object] = {
        "identifier": relation.sql,
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
    return _Resolution(name, authored.description or backing.description or "", resources, _dependency(backing))


def _backing(
    agent: AgentModel,
    authored: AgentTool,
    context: AgentCompileContext,
    diagnostics: list[Diagnostic],
    *,
    authored_key: str,
    expected: tuple[str, ...],
) -> ToolMember | None:
    """Resolve the tool member a Cortex Search or generic tool names, if its type is `expected`."""
    name = authored.name or ""
    if not authored.backing or not name:
        diagnostics.append(D("SST-VAL521", artifact=agent.name, name=name, field=authored_key, subject=agent.key))
        return None
    backing, problems = context.tools.resolve(*authored.backing)
    diagnostics.extend(problems)
    if backing is None:
        return None
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
        return None
    return backing


def _delegate(
    agent: AgentModel, authored: AgentTool, context: AgentCompileContext, diagnostics: list[Diagnostic]
) -> _Resolution | None:
    """An agent tool: another agent of this project by `agent()`, or a tool member by its name.

    An agent reached by `agent()` must publish first, so it is a dependency. A tool member
    that resolves to no relation drops the tool; for a defined member nothing reports it.
    """
    name = authored.name or ""
    resolution: _Resolution | None = None
    if authored.agent_ref:
        target = context.agents.get(authored.agent_ref.casefold())
        if target is None:
            diagnostics.append(D("SST-REF012", name=authored.agent_ref, subject=agent.key))
        else:
            resolution = _Resolution(
                name or target.artifact_name,
                authored.description or "",
                {"identifier": target.sql, "type": "agent"},
                (artifact_key("agent", authored.agent_ref.casefold()),),
            )
    elif name:
        relation = _member_relation(name, context, diagnostics)
        if relation is not None:
            resolution = _Resolution(name, authored.description or "", {"identifier": relation.sql, "type": "agent"})
    if resolution is not None:
        diagnostics.append(D("SST-VAL528", artifact=agent.name, subject=agent.key))
    return resolution


def _mcp(
    agent: AgentModel, authored: AgentTool, context: AgentCompileContext, diagnostics: list[Diagnostic]
) -> _Resolution:
    """An MCP tool, whose `passthrough` keys are its resources."""
    return _Resolution(authored.name or "", authored.description or "", dict(authored.passthrough))


def _builtin(
    agent: AgentModel, authored: AgentTool, context: AgentCompileContext, diagnostics: list[Diagnostic]
) -> _Resolution:
    """A built-in tool, such as data_to_chart: no resources, and named after its type unless named."""
    return _Resolution(authored.name or authored.type, authored.description or "")


_RESOLVERS: Mapping[str, _Resolver] = MappingProxyType(
    {
        "cortex_analyst_text_to_sql": _analyst,
        "cortex_search": _search,
        "generic": _generic,
        "agent": _delegate,
        "mcp": _mcp,
    }
)


def _finish(
    agent: AgentModel, authored: AgentTool, resolution: _Resolution, diagnostics: list[Diagnostic]
) -> ResolvedAgentTool:
    """Check what every resolved tool shares -- name, description, timeout -- then build its entry."""
    name = resolution.name
    if not name or not 1 <= len(name) <= 64:
        diagnostics.append(D("SST-VAL513", artifact=agent.name, name=name, size=len(name), subject=agent.key))
    elif not _TOOL_IDENTIFIER.fullmatch(name):
        diagnostics.append(D("SST-PRS009", name=name, subject=agent.key))
    if authored.type == "web_search" and name != "web_search":
        diagnostics.append(D("SST-VAL517", artifact=agent.name, name=name, subject=agent.key))
    if not resolution.description:
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
    return ResolvedAgentTool(
        authored.type,
        name,
        resolution.description,
        MappingProxyType(resolution.resources),
        authored.input_schema,
        resolution.depends_on,
        authored.tool_spec_passthrough,
    )


def _member_relation(name: str, context: AgentCompileContext, diagnostics: list[Diagnostic]) -> QualifiedName | None:
    backing, problems = context.tools.resolve(name)
    diagnostics.extend(problems)
    if backing is None:
        return None
    relation, relation_problems = context.tools.relation(backing)
    diagnostics.extend(relation_problems)
    return relation


def _dependency(backing: ToolMember) -> tuple[str, ...]:
    # A referenced member has no artifact key: SST never publishes it.
    return (backing.artifact_key,) if backing.artifact_key else ()


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
    """Build a search tool's resources: the service, its columns, and its execution environment.

    Authored columns replace the member's searchable and filterable ones, and an id or
    title column falls back to the member's, then to its first column ending `_id` or
    `_name`. The authored `passthrough` keys come last, so they override any of these.
    """
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
    for key, value in properties.items():
        type_name = value.get("type") if isinstance(value, dict) else None
        if type_name not in _INPUT_TYPES:
            diagnostics.append(D("SST-PRS032", artifact=agent.name, field=key, found=type_name, subject=agent.key))
    required = schema.get("required")
    for key in required if isinstance(required, list) else []:
        if key not in properties:
            diagnostics.append(D("SST-PRS033", artifact=agent.name, field=key, subject=agent.key))
    return tuple(diagnostics)
