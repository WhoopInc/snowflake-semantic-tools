"""The key table of a tool member, and the name sets the tool loader checks members against."""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from snowflake_semantic_tools.domain.model.authored_schema.field_types import (
    Field,
    FieldStatus,
    FileKind,
    Node,
    ValueKind,
)
from snowflake_semantic_tools.domain.model.registry import ARTIFACT_REGISTRY
from snowflake_semantic_tools.domain.model.tool import REFRESH_MODES, ROUTINE_LANGUAGES, ToolKind

_STRING, _TEMPLATE, _LIST, _MAPPING = ValueKind.STRING, ValueKind.TEMPLATE, ValueKind.LIST, ValueKind.MAPPING
_BOOLEAN = ValueKind.BOOLEAN


def _moved(name: str, reason: str) -> Field:
    return Field(name, ValueKind.ANY, f"No longer read: {reason}.", status=FieldStatus.REMOVED, replacement=reason)


_ON = Node(
    (
        Field("table", _TEMPLATE, "The model the service's query selects from, as a `ref()` call."),
        Field("search_column", _TEMPLATE, "The column the service indexes."),
        Field("attributes", _LIST, "Attribute columns the service exposes for filtering.", items=_TEMPLATE),
    ),
    open=True,
    shorthand=_TEMPLATE,
)
# The keys only a member SST creates may write; a reference to an existing object refuses them.
_CREATION = (
    Field(
        "on", _MAPPING, "The model a defined search service's query reads, as a `ref()` call or a mapping.", node=_ON
    ),
    Field("where", _TEMPLATE, "A predicate appended to a defined search service's query."),
    Field(
        "attribute_columns",
        _LIST,
        "Attribute columns written beside `on:`; they win over `on.attributes`.",
        items=_TEMPLATE,
    ),
    Field(
        "refresh_mode",
        ValueKind.ENUM,
        "How a defined search service refreshes, in any case; unset leaves it to Snowflake.",
        choices=REFRESH_MODES,
    ),
    Field("language", ValueKind.ENUM, "The routine's language, in any case.", choices=ROUTINE_LANGUAGES),
    Field("runtime_version", _STRING, "The runtime version of a Java, Python or Scala routine."),
    Field("handler", _STRING, "The entry point a Java, Python or Scala routine runs."),
    Field("body", _TEMPLATE, "The routine's body, as a `file()` call relative to the tool file."),
    Field("returns", _STRING, "The routine's return type: a SQL type, or `TABLE(...)`."),
    Field("packages", _LIST, "Packages the routine imports.", items=_STRING),
    Field("imports", _LIST, "Staged files the routine imports.", items=_TEMPLATE),
    Field("external_access_integrations", _LIST, "External access integrations the routine may use.", items=_STRING),
    Field(
        "secrets", _MAPPING, "Secret variable names mapped to the three-part names of the secrets the routine reads."
    ),
)
_COLUMN = Node(
    (
        Field("column", _TEMPLATE, "The column the entry adds and describes.", required=True),
        Field("description", _STRING, "What the column holds, as search tools describe it."),
        Field("type", _STRING, "The column's search type, `string` or `datetime`."),
        Field("searchable", _BOOLEAN, "Whether search tools search the column."),
        Field("filterable", _BOOLEAN, "Whether search tools may filter on the column."),
    )
)
_PARAMETER = Node(
    (
        Field("name", _STRING, "The parameter's name.", required=True),
        Field("type", _STRING, "The parameter's SQL data type, rendered as written.", required=True),
        Field(
            "required",
            ValueKind.ANY,
            "Refused: every parameter is required.",
            status=FieldStatus.REFUSED,
            code="SST-PRS130",
        ),
    ),
    open=True,
)
_TOOL = ARTIFACT_REGISTRY.artifacts["tool"]
assert _TOOL.root_key is not None and _TOOL.singular_root is not None
TOOL_MEMBER = FileKind(
    "tool",
    "A tool member: a search service, procedure, function or stage SST creates, or an object it references.",
    "a `tool:` file, or an entry of a `tools:` list, in the tools directory",
    Node(
        (
            Field("name", _STRING, "The member's name, which `tool()` references use.", required=True),
            Field(
                "type",
                ValueKind.ENUM,
                "What object the member is.",
                required=True,
                choices=tuple(kind.value for kind in ToolKind),
            ),
            Field("description", _STRING, "The object's comment, and the description of a tool that sets none."),
            *_CREATION,
            Field("search_column", _TEMPLATE, "The column a search service indexes; wins over `on.search_column`."),
            Field("title_column", _TEMPLATE, "The default title column of search tools over the member."),
            Field("id_column", _TEMPLATE, "The default document id column of search tools over the member."),
            Field("columns", _LIST, "Columns the service's query adds, with how search tools use them.", items=_COLUMN),
            Field("relative_path_column", _TEMPLATE, "The column holding each document's stage-relative path."),
            Field("signature", _LIST, "The routine's parameters, in order.", items=_PARAMETER),
            Field(
                "relations",
                _MAPPING,
                "Makes the member a reference: the existing object's three-part name on each target.",
            ),
            Field("meta", _MAPPING, "Free-form metadata SST never reads; `meta.owner` names the owner."),
            Field(
                "config",
                _MAPPING,
                "The member's own `tools:` settings, which win over folder routes.",
                config_block="tools",
            ),
            _moved("warehouse", "a tool member sets it under config:, as config.warehouse"),
            _moved("target_lag", "a tool member sets it under config:, as config.target_lag"),
            _moved("embedding_model", "a tool member sets it under config:, as config.embedding_model"),
            _moved("execute_as", "a tool member sets it under config:, as config.execute_as"),
            _moved(
                "immutable", "a tool member sets it under config:, as config.immutable, or a folder route as +immutable"
            ),
            _moved("owner", "a tool member names its owner under meta:, as meta.owner"),
            _moved(
                "body_file", "a routine names its body as body: \"{{ file('<path>') }}\", relative to the tool file"
            ),
            _moved(
                "columns_and_descriptions",
                "a search service lists its columns under columns:, each column a {{ ref('<model>', '<column>') }}",
            ),
            _moved("group", "tools have no groups: a member's folder under the tools directory routes it"),
            _moved("define", "tools have no groups: write each member as a tool: file or a tools: list entry"),
            _moved("reference", "tools have no groups: a member that writes relations: is a reference"),
        )
    ),
    (_TOOL.singular_root, _TOOL.root_key),
)

CREATION_KEYS = frozenset(item.name for item in _CREATION)
# The keys a member may write. YAML 1.1 reads an unquoted `on:` as the boolean `true`, which a
# member's loader reads as `on:`.
TOOL_MEMBER_KEYS = TOOL_MEMBER.node.read_keys()
# The keys a search service member's `columns:` entry may write.
TOOL_COLUMN_KEYS = _COLUMN.read_keys()
# Keys a member, or an entry of a `tools:` list, wrote before tools lost their groups, with where
# each is written now. Each is an error, so a file still writing one learns it does nothing.
MOVED_TOOL_KEYS: Mapping[str, str] = MappingProxyType(
    {item.name: str(item.replacement) for item in TOOL_MEMBER.node.fields if item.status is FieldStatus.REMOVED}
)
