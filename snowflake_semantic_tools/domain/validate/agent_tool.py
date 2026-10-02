"""Check one agent tool: the keys its type takes, a search tool's columns, and its backing member.

The tool resolver in `app/compile/agents/resolve_tools` runs these as it resolves each tool;
each reads only the values it is given and names the agent as its subject.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.agent import BUILTIN_AGENT_TOOLS, AgentModel, AgentTool
from snowflake_semantic_tools.domain.model.tool import ToolMember

# The keys every tool type takes; `name` on an Analyst tool is SST-VAL520's to report.
_COMMON = frozenset(("type", "name", "description", "tool_spec_passthrough"))
_ENVIRONMENT = frozenset(("warehouse", "query_timeout"))
TOOL_KEYS: Mapping[str, frozenset[str]] = {
    "cortex_analyst_text_to_sql": _COMMON | _ENVIRONMENT | {"semantic_view"},
    "cortex_search": _COMMON
    | _ENVIRONMENT
    | {
        "search_service",
        "max_results",
        "title_column",
        "id_column",
        "stage_path",
        "relative_path_column",
        "filter",
        "columns_and_descriptions",
        "passthrough",
    },
    "generic": _COMMON | _ENVIRONMENT | {"identifier", "input_schema", "passthrough"},
    "agent": _COMMON | {"agent"},
    "mcp": _COMMON | {"passthrough"},
    **dict.fromkeys(BUILTIN_AGENT_TOOLS, _COMMON),
}
# Every key some type takes. A key outside it is not a misplaced key but an unknown one.
_KNOWN_KEYS = frozenset(key for keys in TOOL_KEYS.values() for key in keys)
_COLUMN_TYPES = frozenset(("string", "datetime"))
# JSON Schema input types, by the Snowflake type families each may carry.
_SQL_TO_INPUT: Mapping[str, frozenset[str]] = {
    "VARCHAR": frozenset(("string",)),
    "STRING": frozenset(("string",)),
    "TEXT": frozenset(("string",)),
    "CHAR": frozenset(("string",)),
    "DATE": frozenset(("string",)),
    "TIMESTAMP": frozenset(("string",)),
    "NUMBER": frozenset(("number", "integer")),
    "DECIMAL": frozenset(("number", "integer")),
    "NUMERIC": frozenset(("number", "integer")),
    "INT": frozenset(("integer",)),
    "INTEGER": frozenset(("integer",)),
    "BIGINT": frozenset(("integer",)),
    "SMALLINT": frozenset(("integer",)),
    "FLOAT": frozenset(("number",)),
    "DOUBLE": frozenset(("number",)),
    "REAL": frozenset(("number",)),
    "BOOLEAN": frozenset(("boolean",)),
    "ARRAY": frozenset(("array",)),
}


def misplaced_keys(agent: AgentModel, authored: AgentTool, name: str) -> list[Diagnostic]:
    """Report each key the tool writes that some other tool type takes and its own type does not.

    A built-in tool's `passthrough` is SST-VAL529's to report, since it would emit resources.

    Diagnostics:
        SST-VAL516: a key belongs to another tool type.
    """
    allowed = TOOL_KEYS.get(authored.type, _COMMON)
    if authored.type in BUILTIN_AGENT_TOOLS:
        allowed = allowed | {"passthrough"}
    return [
        D("SST-VAL516", artifact=agent.name, name=name, found=authored.type, key=key, subject=agent.key)
        for key in authored.declared_keys
        if key in _KNOWN_KEYS and key not in allowed
    ]


def search_tool_problems(
    agent: AgentModel, authored: AgentTool, name: str, columns: Mapping[str, object]
) -> list[Diagnostic]:
    """Check a Cortex Search tool's document-preview pair, column descriptors, and filter.

    Args:
        columns: The `columns_and_descriptions` the tool renders: its own, else its member's.

    Diagnostics:
        SST-VAL522: one of `stage_path` and `relative_path_column` is declared without the other.
        SST-VAL524: a column descriptor is not a mapping, or its type, `searchable` or
            `filterable` is missing or of the wrong kind.
        SST-VAL523: the filter names a column not marked filterable, once per column.
    """
    found: list[Diagnostic] = []
    pair = (("stage_path", authored.stage_path), ("relative_path_column", authored.relative_path_column))
    for (field, value), (other, other_value) in (pair, pair[::-1]):
        if value and not other_value:
            found.append(D("SST-VAL522", artifact=agent.name, name=name, field=field, other=other, subject=agent.key))
    for column, descriptor in columns.items():
        detail = _descriptor_problem(descriptor)
        if detail is not None:
            found.append(
                D("SST-VAL524", artifact=agent.name, name=name, column=column, detail=detail, subject=agent.key)
            )
    filterable = {
        str(column).casefold()
        for column, descriptor in columns.items()
        if isinstance(descriptor, Mapping) and descriptor.get("filterable") is True
    }
    for column in dict.fromkeys(filter_columns(authored.filter)):
        if column.casefold() not in filterable:
            found.append(D("SST-VAL523", artifact=agent.name, name=name, column=column, subject=agent.key))
    return found


def _descriptor_problem(descriptor: object) -> str | None:
    if not isinstance(descriptor, Mapping):
        return "the descriptor is not a mapping"
    kind = descriptor.get("type")
    if not isinstance(kind, str) or kind.casefold() not in _COLUMN_TYPES:
        return f"type is {kind!r}, not string or datetime"
    for flag in ("searchable", "filterable"):
        if not isinstance(descriptor.get(flag), bool):
            return f"{flag} is {descriptor.get(flag)!r}, not a boolean"
    return None


def filter_columns(node: object) -> Iterator[str]:
    """Yield each column a Cortex Search filter names, in the order it is written.

    An operator is a key beginning `@`. A leaf operator such as `@eq` maps columns to values;
    a compound one such as `@and` or `@not` holds further filters, as a list or a mapping.
    """
    if isinstance(node, list):
        for item in node:
            yield from filter_columns(item)
        return
    if not isinstance(node, Mapping):
        return
    for key, value in node.items():
        if not str(key).startswith("@"):
            yield str(key)
        elif isinstance(value, Mapping) and not any(str(inner).startswith("@") for inner in value):
            yield from (str(column) for column in value)
        else:
            yield from filter_columns(value)


def signature_mismatch(
    agent: AgentModel, name: str, backing: ToolMember, input_schema: Mapping[str, object]
) -> Diagnostic | None:
    """Compare a member's declared signature with the input schema of the tool that calls it.

    Parameters match properties in number, by name ignoring case, and by type: a Snowflake
    type's family against the JSON Schema type. A member that declares no signature, or a
    schema with no properties mapping, is not compared.

    Diagnostics:
        SST-VAL607: the arity, a name, or a type differs.
    """
    properties = input_schema.get("properties")
    if not backing.signature or not isinstance(properties, Mapping):
        return None
    declared = {str(key).casefold(): value for key, value in properties.items()}
    matches = len(declared) == len(backing.signature) and all(
        input_type_matches(parameter.type, declared.get(parameter.name.casefold())) for parameter in backing.signature
    )
    if matches:
        return None
    found = "(" + ", ".join(f"{parameter.name} {parameter.type}" for parameter in backing.signature) + ")"
    expected = "(" + ", ".join(f"{key} {schema_type(value)}" for key, value in properties.items()) + ")"
    return D("SST-VAL607", name=backing.name, found=found, expected=expected, subject=agent.key)


def schema_type(schema: object) -> str:
    """Name an input-schema property's JSON type, or show the value when it is not a mapping."""
    return str(schema.get("type")) if isinstance(schema, Mapping) else repr(schema)


def input_type_matches(sql_type: str, schema: object) -> bool:
    """Report whether an input-schema property can carry a Snowflake type: its JSON type is in the family.

    A type outside the families SST maps, such as VARIANT, matches any property.
    """
    if not isinstance(schema, Mapping):
        return False
    family = sql_type.strip().upper().split("(")[0].split()[0] if sql_type.strip() else ""
    accepted = _SQL_TO_INPUT.get(family)
    return accepted is None or schema.get("type") in accepted


def overrides(agent: AgentModel, authored: AgentTool, name: str, backing: ToolMember) -> list[Diagnostic]:
    """Report each value the tool sets over one its backing member declares, field by field.

    The description is not among them: a member's describes the object, and is its comment;
    the tool's routes the agent to it, and is always the tool's own.

    Diagnostics:
        SST-VAL615: the tool's warehouse, id column, title column, or columns replace the
            member's.
    """
    pairs = (
        ("warehouse", authored.warehouse, backing.warehouse),
        ("id_column", authored.id_column, backing.id_column),
        ("title_column", authored.title_column, backing.title_column),
    )
    found = [
        D("SST-VAL615", artifact=agent.name, name=name, value=field, subject=agent.key)
        for field, ours, theirs in pairs
        if ours and theirs and ours != theirs
    ]
    if authored.columns_and_descriptions and backing.columns:
        found.append(
            D("SST-VAL615", artifact=agent.name, name=name, value="columns_and_descriptions", subject=agent.key)
        )
    return found
