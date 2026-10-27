"""Check one agent tool: the keys its type takes, a search tool's columns, and its backing member.

The tool resolver in `app/compile/agents/resolve_tools` runs these as it resolves each tool;
each reads only the values it is given and names the agent as its subject.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import replace

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.agent import BUILTIN_AGENT_TOOLS, AgentModel, AgentTool
from snowflake_semantic_tools.domain.model.identifier import Identifier
from snowflake_semantic_tools.domain.model.tool import ToolMember, ToolOwnership
from snowflake_semantic_tools.domain.resolve.tool_column import ColumnSite, tool_column
from snowflake_semantic_tools.domain.sql import ident

# The keys every tool type takes; an Analyst tool's `name` defaults to its view's name.
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
        "columns",
        "passthrough",
    },
    "generic": _COMMON | _ENVIRONMENT | {"identifier", "input_schema", "passthrough"},
    "agent_toolset": _COMMON | {"agent", "identifier"},
    **dict.fromkeys(BUILTIN_AGENT_TOOLS, _COMMON),
}
# Every key some type takes. A key outside it is not a misplaced key but an unknown one.
_KNOWN_KEYS = frozenset(key for keys in TOOL_KEYS.values() for key in keys)
_COLUMN_TYPES = frozenset(("string", "datetime"))
# Parameter type families Snowflake's tool runtime cannot pass a generic tool's argument as.
_UNCALLABLE_TYPES = frozenset(("ARRAY", "OBJECT", "VARIANT"))
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
    type's family against the JSON Schema type. Every parameter is required, since none renders
    a DEFAULT, so the schema's `required` must list each one. A member that declares no
    signature, or a schema with no properties mapping, is not compared.

    Diagnostics:
        SST-VAL607: the arity, a name, or a type differs, or `required` leaves out a parameter.
    """
    properties = input_schema.get("properties")
    if not backing.signature or not isinstance(properties, Mapping):
        return None
    declared = {str(key).casefold(): value for key, value in properties.items()}
    matches = len(declared) == len(backing.signature) and all(
        input_type_matches(parameter.type, declared.get(parameter.name.casefold())) for parameter in backing.signature
    )
    required = input_schema.get("required")
    listed = {str(key).casefold() for key in required} if isinstance(required, list) else set()
    optional = [parameter.name for parameter in backing.signature if parameter.name.casefold() not in listed]
    if matches and not optional:
        return None
    found = "(" + ", ".join(f"{parameter.name} {parameter.type}" for parameter in backing.signature) + ")"
    expected = "(" + ", ".join(f"{key} {schema_type(value)}" for key, value in properties.items()) + ")"
    if matches:
        found, expected = f"{found} requiring each", f"{expected} not requiring {', '.join(optional)}"
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
    accepted = _SQL_TO_INPUT.get(_family(sql_type))
    return accepted is None or schema.get("type") in accepted


def _family(sql_type: str) -> str:
    """Name a Snowflake type's family: its first word, upper-cased, before any `(`; "" when blank."""
    return sql_type.strip().upper().split("(")[0].split()[0] if sql_type.strip() else ""


def generic_callable(agent: AgentModel, name: str, backing: ToolMember) -> tuple[Diagnostic, ...]:
    """Report what in a routine's declared signature a generic tool calling it cannot pass or read.

    Snowflake's tool runtime calls the routine on a warehouse with each argument named unquoted,
    and reads a single value back. A routine an agent does not call this way is not affected.
    A parameter whose name does not parse is left to SST-PRS005.

    Diagnostics:
        SST-VAL623: the routine returns a table.
        SST-VAL624: a parameter's name renders quoted: it is a reserved word, or written quoted.
        SST-VAL625: a parameter's type is ARRAY, OBJECT or VARIANT.
    """
    found: list[Diagnostic] = []
    if _family(backing.returns or "") == "TABLE":
        found.append(D("SST-VAL623", artifact=agent.name, name=name, member=backing.name, subject=agent.key))
    for parameter in backing.signature:
        if _needs_quotes(parameter.name):
            found.append(
                D(
                    "SST-VAL624",
                    artifact=agent.name,
                    name=name,
                    member=backing.name,
                    field=parameter.name,
                    subject=agent.key,
                )
            )
        if _family(parameter.type) in _UNCALLABLE_TYPES:
            found.append(
                D(
                    "SST-VAL625",
                    artifact=agent.name,
                    name=name,
                    member=backing.name,
                    field=parameter.name,
                    found=parameter.type,
                    subject=agent.key,
                )
            )
    return tuple(found)


def _needs_quotes(name: str) -> bool:
    """Report whether a parameter name renders other than as its bare name upper-cased."""
    try:
        rendered = str(ident(Identifier.parse(name)))
    except ValueError:
        return False
    return rendered != name.strip().upper()


def overrides(agent: AgentModel, authored: AgentTool, name: str, backing: ToolMember) -> list[Diagnostic]:
    """Report each value the tool sets over one its backing member declares, field by field.

    The description is not among them: a member's describes the object, and is its comment;
    the tool's routes the agent to it, and is always the tool's own.

    Diagnostics:
        SST-VAL615: the tool's warehouse, id column, title column, or columns replace the
            member's.
    """
    pairs = (
        ("warehouse", authored.warehouse, backing.configured_warehouse),
        ("id_column", authored.id_column, backing.id_column),
        ("title_column", authored.title_column, backing.title_column),
    )
    found = [
        D("SST-VAL615", artifact=agent.name, name=name, value=field, subject=agent.key)
        for field, ours, theirs in pairs
        if ours and theirs and ours != theirs
    ]
    if authored.columns and backing.columns:
        found.append(D("SST-VAL615", artifact=agent.name, name=name, value="columns", subject=agent.key))
    return found


def search_tool_columns(
    agent: AgentModel, authored: AgentTool, name: str, backing: ToolMember
) -> tuple[AgentTool, dict[str, object], list[Diagnostic]]:
    """Read a search tool's own columns against its member, as the member reads its own.

    Over a defined member each is a `{{ ref('<on model>', '<column>') }}`; over a referenced
    one, a bare column. A column that does not resolve reads as unset, and an entry whose column
    does not resolve is left out.

    Returns:
        The tool with its id, title and relative path columns resolved; its own columns, each
        upper-cased column mapped to the rest of its entry, in written order, which replace the
        member's when there are any; and what reading them reported.

    Diagnostics:
        SST-VAL524: a `columns:` entry is not a mapping, or names no `column`.
        Every code `tool_column` lists, for each column.
    """
    found: list[Diagnostic] = []
    referenced = backing.ownership is ToolOwnership.REFERENCE

    def column(field: str, value: str | None) -> str | None:
        if not value:
            return None
        site = ColumnSite(backing.name, f"tools.{name}.{field}", agent.name, agent.key, authored.origin)
        resolved, problem = tool_column(value, site, referenced=referenced, on_model=backing.on_model)
        if problem is not None:
            found.append(problem)
        return resolved

    columns: dict[str, object] = {}
    for index, entry in enumerate(authored.columns):
        raw = entry.get("column") if isinstance(entry, Mapping) else None
        if not isinstance(entry, Mapping) or not isinstance(raw, str) or not raw.strip():
            detail = "the entry is not a mapping" if not isinstance(entry, Mapping) else "it names no column"
            found.append(
                D(
                    "SST-VAL524",
                    artifact=agent.name,
                    name=name,
                    column=f"columns[{index}]",
                    detail=detail,
                    subject=agent.key,
                )
            )
            continue
        resolved = column(f"columns[{index}].column", raw.strip())
        if resolved is not None:
            columns[resolved.upper()] = {str(key): value for key, value in entry.items() if key != "column"}
    tool = replace(
        authored,
        id_column=column("id_column", authored.id_column),
        title_column=column("title_column", authored.title_column),
        relative_path_column=column("relative_path_column", authored.relative_path_column),
    )
    return tool, columns, found
