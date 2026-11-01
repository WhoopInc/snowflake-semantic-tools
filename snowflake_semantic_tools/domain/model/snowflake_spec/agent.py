"""The specification SST writes for a Cortex Agent: each key, its source, and its documentation.

`AGENT_SPEC` is the document `CREATE AGENT ... FROM SPECIFICATION` takes; `TOOL_RESOURCES` is
each tool type's `tool_resources` entry, with `EXECUTION_ENVIRONMENT` shared by the types that
run queries. A built-in tool has no resources. The renderer, the importer and the passthrough
check are held to these tables by `tests/unit/domain/test_snowflake_spec.py`.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from snowflake_semantic_tools.domain.model.authored_schema import ValueKind
from snowflake_semantic_tools.domain.model.snowflake_spec import docs
from snowflake_semantic_tools.domain.model.snowflake_spec.model import (
    Authored,
    Derived,
    Documented,
    Fixed,
    Measured,
    Setting,
    SpecKey,
    SpecNode,
)

_STRING, _INTEGER, _BOOLEAN = ValueKind.STRING, ValueKind.INTEGER, ValueKind.BOOLEAN
_MAPPING, _LIST, _ANY = ValueKind.MAPPING, ValueKind.LIST, ValueKind.ANY


def _agent(path: str) -> Authored:
    return Authored("agent", path)


def _tool(key: str) -> Authored:
    return _agent(f"spec.tools.{key}")


EXECUTION_ENVIRONMENT = SpecKey(
    "execution_environment",
    _MAPPING,
    "Where the tool's queries run; left out when neither a warehouse nor a timeout is set.",
    node=SpecNode(
        (
            SpecKey("type", _STRING, "The kind of environment.", (Fixed("warehouse"),)),
            SpecKey(
                "warehouse",
                _STRING,
                "The warehouse the queries run on.",
                (
                    _tool("warehouse"),
                    Derived("the backing member's configured warehouse, for a search or generic tool"),
                    Setting("agents", "warehouse"),
                    Derived("the target's warehouse, for a generic tool"),
                ),
            ),
            SpecKey(
                "query_timeout",
                _INTEGER,
                "Seconds a query may run.",
                (_tool("query_timeout"), Setting("agents", "query_timeout")),
            ),
        ),
        Documented(docs.AGENTS_REST_API),
    ),
)
_COLUMNS = SpecNode(
    (
        SpecKey(
            "description",
            _STRING,
            "What the column holds.",
            (_tool("columns.description"), Derived("the backing member's column description")),
        ),
        SpecKey(
            "type", _STRING, "The column's search type.", (_tool("columns.type"), Derived("the member's column type"))
        ),
        SpecKey(
            "searchable",
            _BOOLEAN,
            "Whether search tools search the column.",
            (_tool("columns.searchable"), Derived("the member's column flag")),
        ),
        SpecKey(
            "filterable",
            _BOOLEAN,
            "Whether search tools may filter on it.",
            (_tool("columns.filterable"), Derived("the member's column flag")),
        ),
    )
)
TOOL_RESOURCES: Mapping[str, SpecNode] = MappingProxyType(
    {
        "cortex_analyst_text_to_sql": SpecNode(
            (
                SpecKey(
                    "semantic_view",
                    _STRING,
                    "The semantic view the tool queries, by its three-part name.",
                    (_tool("semantic_view"),),
                ),
                EXECUTION_ENVIRONMENT,
            ),
            Documented(docs.AGENTS_REST_API),
        ),
        "cortex_search": SpecNode(
            (
                SpecKey(
                    "search_service",
                    _STRING,
                    "The search service, by its three-part name.",
                    (_tool("search_service"),),
                ),
                SpecKey("max_results", _INTEGER, "The most results one search returns.", (_tool("max_results"),)),
                SpecKey(
                    "id_column",
                    _STRING,
                    "The column that identifies each document, upper-cased.",
                    (
                        _tool("id_column"),
                        Derived("the backing member's id column, else its first column ending `_id`"),
                    ),
                ),
                SpecKey(
                    "title_column",
                    _STRING,
                    "The column citations title each document by, upper-cased.",
                    (
                        _tool("title_column"),
                        Derived("the backing member's title column, else its first column ending `_name`"),
                    ),
                ),
                SpecKey(
                    "columns_and_descriptions",
                    _MAPPING,
                    "The searchable and filterable columns, by upper-cased name.",
                    (_tool("columns.column"), Derived("the backing member's searchable and filterable columns")),
                    node=_COLUMNS,
                    evidence=Documented(docs.SEARCH_IN_AGENTS),
                    named=True,
                ),
                SpecKey(
                    "stage_path",
                    _STRING,
                    "The stage the documents open from.",
                    (_tool("stage_path"),),
                    evidence=Documented(docs.SEARCH_IN_AGENTS),
                ),
                SpecKey(
                    "relative_path_column",
                    _STRING,
                    "The column holding each document's path on that stage.",
                    (_tool("relative_path_column"),),
                    evidence=Documented(docs.SEARCH_IN_AGENTS),
                ),
                SpecKey("filter", _MAPPING, "A filter every search applies.", (_tool("filter"),)),
                EXECUTION_ENVIRONMENT,
            ),
            Documented(docs.MANAGE_AGENTS),
        ),
        "generic": SpecNode(
            (
                SpecKey(
                    "identifier",
                    _STRING,
                    "The procedure or function the tool calls, by its three-part name.",
                    (_tool("identifier"),),
                ),
                SpecKey(
                    "type",
                    _STRING,
                    "Whether it is a procedure or a function.",
                    (Derived("the backing member's type"),),
                ),
                EXECUTION_ENVIRONMENT,
            ),
            Documented(docs.AGENTS_REST_API),
        ),
        "agent_toolset": SpecNode(
            (
                SpecKey(
                    "agent_name",
                    _STRING,
                    "The agent whose tools this one inherits, by its three-part name.",
                    (_tool("agent"), _tool("identifier")),
                ),
            ),
            Documented(docs.AGENT_TOOLSETS),
        ),
    }
)
_TOOL_SPEC = SpecNode(
    (
        SpecKey("type", _STRING, "The tool's type.", (_tool("type"),)),
        SpecKey(
            "name",
            _STRING,
            "The tool's name, which its `tool_resources` entry is keyed by.",
            (
                _tool("name"),
                Derived("the semantic view's name, for an Analyst tool"),
                Derived("its type, for a built-in"),
            ),
        ),
        SpecKey(
            "description",
            _STRING,
            "What the orchestrator reads to choose the tool.",
            (_tool("description"), Derived("the backing member's description")),
        ),
        SpecKey(
            "input_schema",
            _MAPPING,
            "The JSON Schema of a generic tool's input.",
            (_tool("input_schema"),),
        ),
    )
)


def instructions_key(kind: str) -> SpecKey:
    """The `instructions` section, filled from `kind`'s `spec.instructions:`."""
    return SpecKey(
        "instructions",
        _MAPPING,
        "Instructions the agent follows; left out when none is set.",
        node=SpecNode(
            (
                SpecKey(
                    "orchestration",
                    _STRING,
                    "How the agent plans which tools to use.",
                    (Authored(kind, "spec.instructions.orchestration"),),
                ),
                SpecKey(
                    "response",
                    _STRING,
                    "How the agent writes its answer.",
                    (Authored(kind, "spec.instructions.response"),),
                ),
                SpecKey(
                    "sample_questions",
                    _LIST,
                    "Questions shown to users, each as a `question:` entry.",
                    items=SpecNode(
                        (
                            SpecKey(
                                "question",
                                _STRING,
                                "One question.",
                                (Authored(kind, "spec.instructions.sample_questions.question"),),
                            ),
                        )
                    ),
                ),
            )
        ),
    )


def skills_key(kind: str) -> SpecKey:
    """The `skills` section, filled from `kind`'s `spec.skills:` entries."""
    return SpecKey(
        "skills",
        _LIST,
        "The skills loaded.",
        items=SpecNode(
            (
                SpecKey(
                    "name",
                    _STRING,
                    "The skill's name; left out when only its source names it.",
                    (Authored(kind, "spec.skills.name"), Derived("the name of the skill a `skill()` call resolves to")),
                ),
                SpecKey(
                    "source",
                    _MAPPING,
                    "Where the skill is read from.",
                    node=SpecNode(
                        (
                            SpecKey(
                                "type",
                                _STRING,
                                "The kind of location.",
                                (
                                    Authored(kind, "spec.skills.source.type"),
                                    Derived("the extension a `skill()` or `plugin()` call resolves to"),
                                ),
                            ),
                            SpecKey(
                                "path",
                                _STRING,
                                "The stage path, or the extension's three-part name.",
                                (
                                    Authored(kind, "spec.skills.source.path"),
                                    Derived("the extension a `skill()` or `plugin()` call resolves to"),
                                ),
                            ),
                            SpecKey(
                                "version",
                                _STRING,
                                "The extension version or alias.",
                                (Authored(kind, "spec.skills.source.version"),),
                                evidence=Measured("A skill's version may be an extension version alias"),
                            ),
                        )
                    ),
                ),
            ),
            Documented(docs.AGENT_SKILLS),
        ),
    )


AGENT_SPEC = SpecNode(
    (
        SpecKey(
            "models",
            _MAPPING,
            "The models the agent runs.",
            node=SpecNode(
                (
                    SpecKey(
                        "orchestration",
                        _STRING,
                        "The orchestration model; `auto` lets Snowflake choose.",
                        (Setting("agents", "orchestration_model"),),
                    ),
                )
            ),
        ),
        SpecKey(
            "orchestration",
            _MAPPING,
            "Orchestration limits and capabilities; left out when none is set.",
            node=SpecNode(
                (
                    SpecKey(
                        "budget",
                        _MAPPING,
                        "Limits a request ends at, whichever comes first.",
                        node=SpecNode(
                            (
                                SpecKey(
                                    "seconds", _INTEGER, "The time budget.", (Setting("agents", "budget_seconds"),)
                                ),
                                SpecKey("tokens", _INTEGER, "The token budget.", (Setting("agents", "budget_tokens"),)),
                            )
                        ),
                    ),
                    SpecKey(
                        "tool_not_accessible",
                        _STRING,
                        "Whether a tool the caller cannot use aborts the run.",
                        (Setting("agents", "tool_not_accessible"),),
                        evidence=Documented(docs.INACCESSIBLE_TOOLS),
                    ),
                    SpecKey(
                        "capabilities",
                        _MAPPING,
                        "Capabilities the agent enables.",
                        node=SpecNode(
                            (
                                SpecKey(
                                    "analytical_search",
                                    _BOOLEAN,
                                    "Analytics over a search tool's results.",
                                    (Setting("agents", "analytical_search"),),
                                ),
                            )
                        ),
                    ),
                )
            ),
        ),
        instructions_key("agent"),
        SpecKey(
            "tools",
            _LIST,
            "The agent's tools, in order.",
            items=SpecNode((SpecKey("tool_spec", _MAPPING, "The tool.", node=_TOOL_SPEC),)),
        ),
        SpecKey(
            "tool_resources",
            _MAPPING,
            "Each tool's resources, by tool name; a built-in tool's and an empty one are left out.",
            node=SpecNode((), Documented(docs.AGENTS_REST_API)),
            named=True,
        ),
        skills_key("agent"),
    ),
    Documented(docs.AGENTS_REST_API),
)
# Authored blocks whose keys SST copies into the specification unread, with where each lands.
AGENT_PASSTHROUGH: Mapping[str, tuple[str, ...]] = MappingProxyType(
    {
        "spec.passthrough": (),
        "spec.tools.tool_spec_passthrough": ("tools", "*", "tool_spec"),
        "spec.tools.passthrough": ("tool_resources", "*"),
    }
)
# What an agent's files and settings hold that is not in its specification, with where it goes.
AGENT_NOT_IN_SPEC: Mapping[str, str] = MappingProxyType(
    {
        "agent:name": "the agent object's name",
        "agent:comment": "the object's COMMENT, set by ALTER AGENT",
        "agent:profile.display_name": "the object's PROFILE, set by ALTER AGENT",
        "agent:profile.avatar": "the object's PROFILE, set by ALTER AGENT",
        "agent:profile.color": "the object's PROFILE, set by ALTER AGENT",
        "agent:deprecated": "SST's own lifecycle marker",
        "agent:meta": "free-form metadata SST never sends",
        "agent:evals.dataset": "the agent's evals, which SST runs itself",
        "agent:evals.config": "the agent's evals, which SST runs itself",
        "agents.database": "where the object is created",
        "agents.schema": "where the object is created",
        "agents.scratch_schema": "where a dev run's object is created",
        "agents.allow_auto_model": "a check on orchestration_model, not a value sent",
        "agents.alias": "the version alias SST commits, not part of the specification",
        "agents.enabled": "whether SST publishes the agent at all",
        "agents.secure": "the object's SECURE flag, set by ALTER AGENT",
        "agents.tags": "object tags, set by ALTER AGENT",
    }
)
