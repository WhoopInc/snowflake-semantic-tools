"""The key tables of an agent file and of a CoWork profile file, which share most of their nodes.

An agent's `spec.tools` entries are discriminated by `type`: each type adds its keys to the
ones every tool takes, which is where `TOOL_KEYS` comes from.
"""

from __future__ import annotations

from types import MappingProxyType

from snowflake_semantic_tools.domain.model.agent import BUILTIN_AGENT_TOOLS, KNOWN_AGENT_TOOL_TYPES, SEARCH_COLUMN_TYPES
from snowflake_semantic_tools.domain.model.authored_schema.field_types import (
    Field,
    FieldStatus,
    FileKind,
    Node,
    ValueKind,
)
from snowflake_semantic_tools.domain.model.authored_schema.semantic import TAGGED_CONFIG
from snowflake_semantic_tools.domain.model.cowork_profile import COWORK_PROFILE_TOOLS

_STRING, _TEMPLATE, _LIST, _MAPPING = ValueKind.STRING, ValueKind.TEMPLATE, ValueKind.LIST, ValueKind.MAPPING
_BOOLEAN, _INTEGER = ValueKind.BOOLEAN, ValueKind.INTEGER


def _moved(name: str, reason: str) -> Field:
    return Field(name, ValueKind.ANY, f"No longer read: {reason}.", status=FieldStatus.REMOVED, replacement=reason)


def _free(name: str, summary: str) -> Field:
    return Field(name, _MAPPING, summary)


_DISPLAY = Node(
    (
        Field("display_name", _STRING, "The name people see; two agents with one display name is a warning."),
        Field("avatar", _STRING, "The avatar image name, from the account's avatar allowlist when it sets one."),
        Field("color", _STRING, "The display color: a plain color name, or a CSS variable such as `var(--token)`."),
    )
)
_SAMPLE_QUESTION = Node(
    (Field("question", _STRING, "One sample question, published as written.", required=True),), open=True
)
_INSTRUCTIONS = Node(
    (
        Field(
            "orchestration",
            _TEMPLATE,
            "How the agent plans and chooses tools: inline text, or one `file()` call.",
        ),
        Field("response", _TEMPLATE, "How the agent writes its answers: inline text, or one `file()` call."),
        Field("sample_questions", _LIST, "Example questions shown to users.", items=_SAMPLE_QUESTION),
    )
)
_SOURCE = Node(
    (
        Field(
            "type", _STRING, "The kind of source the skill is loaded from, such as `CORTEX_EXTENSION`.", required=True
        ),
        Field("path", _TEMPLATE, "The project skill or plugin, or the consumed extension, the entry pins."),
        Field(
            "version",
            _TEMPLATE,
            "The version a consumed extension pins, or a `var()` call; a project skill or plugin is pinned for you.",
        ),
    )
)
_SKILL = Node(
    (
        Field("name", _TEMPLATE, "Which member of a plugin the entry loads; every member when unset."),
        Field("source", _MAPPING, "Where the skill comes from: its source type, path and version.", node=_SOURCE),
    )
)
_EVALS = Node(
    (
        Field(
            "dataset", _TEMPLATE, "The eval dataset in the `evals/` folder, as an `eval_dataset()` call.", required=True
        ),
        Field(
            "config", _TEMPLATE, "The eval config in the `evals/` folder, as an `eval_config()` call.", required=True
        ),
    )
)


def _skills() -> Field:
    return Field("skills", _LIST, "The skills loaded, each pinned to a published extension version.", items=_SKILL)


def _common(subject: str, display: str, display_summary: str) -> tuple[Field, ...]:
    """The keys an agent and a CoWork profile both write at their root, with the display block's name."""
    return (
        Field(
            "name", _STRING, f"The {subject}'s name, which keys it and names the object it publishes as.", required=True
        ),
        Field("comment", _STRING, f"Published as the {subject}'s comment, before SST's ownership marker."),
        Field(display, _MAPPING, display_summary, node=_DISPLAY),
    )


def _closing(subject: str, block: str) -> tuple[Field, ...]:
    """The keys after `spec:` an agent and a CoWork profile both write."""
    return (
        Field(
            "config",
            _MAPPING,
            f"The {subject}'s own `{block}:` settings, which win over folder routes and the block root.",
            node=TAGGED_CONFIG,
            config_block=block,
        ),
        Field("deprecated", _BOOLEAN, f"Retires the {subject}; one that still has an alias is an error."),
        _free("meta", "Free-form metadata, never published, but counted in the fingerprint."),
        Field("evals", _MAPPING, f"The eval dataset and eval config in the {subject}'s `evals/` folder.", node=_EVALS),
    )


_ENVIRONMENT = (
    Field("warehouse", _STRING, "The warehouse the tool's queries run on."),
    Field("query_timeout", _INTEGER, "Seconds a tool query may run."),
)
_SEARCH_COLUMN = Node(
    (
        Field("column", _TEMPLATE, "The search service column the entry describes.", required=True),
        Field("description", _STRING, "What the column holds, as the agent reads it."),
        Field(
            "type",
            ValueKind.ENUM,
            "The column's search type.",
            required=True,
            choices=tuple(sorted(SEARCH_COLUMN_TYPES)),
        ),
        Field("searchable", _BOOLEAN, "Whether queries search the column's text.", required=True),
        Field("filterable", _BOOLEAN, "Whether the tool's filter may name the column.", required=True),
    ),
    open=True,
)
_PASSTHROUGH = _free("passthrough", "Keys added last to the tool's resources, overriding the computed ones.")
_SEARCH = (
    *_ENVIRONMENT,
    Field("search_service", _TEMPLATE, "The search service tool member the tool queries, as a `tool()` call."),
    Field("max_results", _INTEGER, "The most results one search returns."),
    Field("title_column", _TEMPLATE, "The column a result's title is; defaults to the member's."),
    Field("id_column", _TEMPLATE, "The column that identifies a document; defaults to the member's."),
    Field("stage_path", _TEMPLATE, "The stage documents are previewed from; written with `relative_path_column`."),
    Field("relative_path_column", _TEMPLATE, "The column holding each document's path below `stage_path`."),
    _free("filter", "A fixed search filter applied to every query the tool runs."),
    Field("columns", _LIST, "The tool's own column descriptions, replacing its member's.", items=_SEARCH_COLUMN),
    _PASSTHROUGH,
)
# One `spec.tools` entry of an agent: the keys every tool takes, then those its `type` adds.
AGENT_TOOL = Node(
    (
        Field(
            "type",
            ValueKind.ENUM,
            "The tool's kind, which decides the other keys it takes and how it renders.",
            required=True,
            choices=tuple(sorted(KNOWN_AGENT_TOOL_TYPES)),
            open_choices=True,
        ),
        Field("name", _STRING, "The name the agent calls the tool by, unique within the agent."),
        Field("description", _STRING, "What the tool answers and what it does not, as the orchestrator reads it."),
        _free("tool_spec_passthrough", "Keys added last to the rendered tool spec, overriding the computed ones."),
        _moved(
            "columns_and_descriptions",
            "a search tool lists its columns under columns:, each column as its member names its own",
        ),
    ),
    discriminator="type",
    variants=MappingProxyType(
        {
            "cortex_analyst_text_to_sql": (
                *_ENVIRONMENT,
                Field("semantic_view", _TEMPLATE, "The semantic view the tool queries, as a `semantic_view()` call."),
            ),
            "cortex_search": _SEARCH,
            "generic": (
                *_ENVIRONMENT,
                Field(
                    "identifier", _TEMPLATE, "The procedure or function tool member the tool calls, as a `tool()` call."
                ),
                _free("input_schema", "A JSON Schema object describing the arguments the tool passes."),
                _PASSTHROUGH,
            ),
            "agent_toolset": (
                Field("agent", _TEMPLATE, "The project agent whose tools the toolset adds, as an `agent()` call."),
                Field(
                    "identifier", _TEMPLATE, "The agent tool member whose tools the toolset adds, as a `tool()` call."
                ),
            ),
            **dict.fromkeys(sorted(BUILTIN_AGENT_TOOLS), ()),
        }
    ),
)
AGENT = FileKind(
    "agent",
    "A Cortex Agent: its instructions, the tools it calls, and the skills it loads.",
    "an `agent.yml` or `agent.yaml` file in a folder of the agents directory",
    Node(
        (
            *_common("agent", "profile", "The display name, avatar and color of the published agent."),
            Field(
                "spec",
                _MAPPING,
                "The agent specification: instructions, tools, skills and passthrough keys.",
                node=Node(
                    (
                        Field(
                            "instructions",
                            _MAPPING,
                            "The agent's instructions and sample questions.",
                            node=_INSTRUCTIONS,
                        ),
                        Field("tools", _LIST, "The tools the agent can call.", items=AGENT_TOOL),
                        _skills(),
                        _free("passthrough", "Keys added unvalidated to the rendered specification, last."),
                        _moved("models", "an agent sets its model under config:, as config.orchestration_model"),
                        _moved(
                            "orchestration",
                            "an agent sets these under config:, as config.budget_seconds, config.budget_tokens, "
                            "config.tool_not_accessible and config.analytical_search",
                        ),
                    )
                ),
            ),
            *_closing("agent", "agents"),
            _moved("alias", "an agent sets it under config:, as config.alias"),
            _moved("secure", "an agent sets it under config:, as config.secure"),
            _moved("enabled", "an agent sets it under config:, as config.enabled"),
            _moved("tags", "an agent sets it under config:, as config.tags"),
        )
    ),
)
_SUBAGENT = Node(
    (
        Field(
            "agent",
            _TEMPLATE,
            "A project agent, as an `agent()` call, or an agent tool member, as a `tool()` call.",
            required=True,
        ),
        Field("display_name", _STRING, "The subagent's name as the router shows it; defaults to the agent's own."),
        Field("description", _STRING, "What the subagent is for, which the router reads to choose it."),
        Field(
            "sample_questions",
            _LIST,
            "The subagent's example questions; an empty list offers none, and leaving it out keeps the agent's.",
            items=_SAMPLE_QUESTION,
        ),
    )
)
_PROFILE_TOOL = Node(
    (
        Field(
            "type",
            ValueKind.ENUM,
            "The built-in tool the profile carries.",
            required=True,
            choices=tuple(sorted(COWORK_PROFILE_TOOLS)),
        ),
        Field("name", _STRING, "The tool's name in the profile's specification."),
    )
)
COWORK_PROFILE = FileKind(
    "cowork_profile",
    "A CoWork profile: a router over subagents, with the built-in tools and skills it carries itself.",
    "a `cowork_profile.yml` or `cowork_profile.yaml` file in a folder of the CoWork profiles directory",
    Node(
        (
            *_common("profile", "display", "The display name, avatar and color of the profile."),
            Field(
                "spec",
                _MAPPING,
                "Routing instructions, subagents, built-in tools, skills and passthrough keys.",
                node=Node(
                    (
                        Field(
                            "instructions",
                            _MAPPING,
                            "The profile's instructions and sample questions.",
                            node=_INSTRUCTIONS,
                        ),
                        Field(
                            "subagents",
                            _LIST,
                            "The agents the profile's router may hand a question to.",
                            items=_SUBAGENT,
                        ),
                        Field("tools", _LIST, "Built-in tools the profile answers with itself.", items=_PROFILE_TOOL),
                        _skills(),
                        _free("passthrough", "Keys copied into the specification unread."),
                    )
                ),
            ),
            *_closing("profile", "cowork_profiles"),
        )
    ),
)
