"""The key tables of a semantic view and of the five members a semantic models directory declares.

Each node's fields are in the order its keys' nested checks run, so the name tables derived
from it keep the order the diagnostics come out in.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.authored import ACCESS_MODIFIERS, NULL_ORDERS, SORT_DIRECTIONS, VISIBILITY
from snowflake_semantic_tools.domain.model.authored_schema.field_types import (
    Field,
    FieldStatus,
    FileKind,
    Node,
    ValueKind,
)
from snowflake_semantic_tools.domain.model.registry import ARTIFACT_REGISTRY
from snowflake_semantic_tools.domain.model.semantic_view import SCOPE_KINDS

_STRING, _TEMPLATE, _LIST, _MAPPING = ValueKind.STRING, ValueKind.TEMPLATE, ValueKind.LIST, ValueKind.MAPPING
_RENAMED, _REMOVED = FieldStatus.RENAMED, FieldStatus.REMOVED
_IN = "a semantic models directory"


def _roots(name: str) -> tuple[str, ...]:
    """The keys a file holds one declaration of `name`, or a list of them, under, from the registry."""
    entry = ARTIFACT_REGISTRY.artifacts.get(name) or ARTIFACT_REGISTRY.members[name]
    assert entry.root_key is not None and entry.singular_root is not None
    return (entry.singular_root, entry.root_key)


def _renamed(name: str, to: str) -> Field:
    return Field(name, ValueKind.ANY, f"The 0.3 spelling of `{to}`; it is not read.", status=_RENAMED, replacement=to)


def _removed(name: str, reason: str, code: str | None = None) -> Field:
    return Field(name, ValueKind.ANY, f"No longer read: {reason}.", status=_REMOVED, replacement=reason, code=code)


def _name(member: str) -> Field:
    return Field("name", _STRING, f"The {member}'s name, which references to it use.", required=True)


def _documents(member: str) -> Field:
    return Field(
        "description", _STRING, f"Documents the {member} for readers of the YAML; it is never published.", loose=True
    )


def _sort_fields(subject: str) -> tuple[Field, ...]:
    return (
        Field(
            "sort_direction",
            ValueKind.ENUM,
            f"The order {subject} sorts in; ascending when unset.",
            choices=tuple(SORT_DIRECTIONS),
        ),
        Field(
            "null_order",
            ValueKind.ENUM,
            "Whether nulls sort first or last; unset, the default null ordering applies.",
            choices=tuple(NULL_ORDERS),
        ),
    )


_SCOPE_FIELDS = tuple(
    item
    for include, exclude, word in SCOPE_KINDS
    for item in (
        Field(include, _LIST, f"Keeps only the listed {word}s of those the view's tables attach.", items=_TEMPLATE),
        Field(exclude, _LIST, f"Drops the listed {word}s from those the view's tables attach.", items=_TEMPLATE),
    )
)
_TAG = Node(
    (
        Field("name", _TEMPLATE, "One tag the project's `tags:` block declares, as a `tag()` call.", required=True),
        Field(
            "value",
            _STRING,
            "The value the tag is set to; a number is read as text, a boolean is refused.",
            required=True,
            also=(ValueKind.NUMBER,),
        ),
    )
)
# A `config:` block whose own keys are a settings block's; only its tag entries nest a node.
TAGGED_CONFIG = Node(
    (Field("tags", _LIST, "Tags the object is published with, each a name and a value.", items=_TAG),),
    checked_elsewhere=True,
)
_VARIABLE = Node(
    (
        Field("name", _STRING, "The variable's name, unique in the view regardless of case.", required=True),
        Field("data_type", _STRING, "The variable's SQL data type; its default must be of this type.", required=True),
        Field(
            "default_value",
            ValueKind.ANY,
            "The value the variable takes when a query sets none, cast to its declared type.",
            required=True,
        ),
        Field("description", _STRING, "Renders as the variable's COMMENT.", loose=True),
    )
)
SEMANTIC_VIEW = FileKind(
    "semantic_view",
    "A semantic view: the dbt models it holds, the members they attach, and what it keeps of them.",
    f"a `semantic_view:` file, or an entry of a `semantic_views:` list, in {_IN}",
    Node(
        (
            Field(
                "name",
                _STRING,
                "The view's name; unquoted, it folds case, so names differing only in case are one view.",
                required=True,
            ),
            Field("description", _STRING, "What the view answers and when to use it; renders as its COMMENT."),
            Field(
                "tables",
                _LIST,
                "The models and sources the view holds, which decide the members that attach to it.",
                items=_TEMPLATE,
            ),
            Field(
                "custom_instructions",
                _LIST,
                "The custom instruction blocks the view includes; their text joins in list order.",
                items=_TEMPLATE,
            ),
            Field(
                "variables",
                _LIST,
                "View variables, which member expressions refer to by bare name.",
                items=_VARIABLE,
            ),
            Field(
                "config",
                _MAPPING,
                "The view's own `semantic_views:` settings, which win over folder routes.",
                node=TAGGED_CONFIG,
                config_block="semantic_views",
            ),
            *_SCOPE_FIELDS,
            _removed(
                "table_config",
                "a table's synonyms, COMMENT and distinct_range come from its dbt model's "
                "config.meta.sst.synonyms, description and config.meta.sst.distinct_range",
            ),
            _removed("enabled", "a view sets it under config:, as config.enabled"),
            _removed("tags", "a view sets it under config:, as config.tags"),
            _removed("max_staleness", "a view sets it under config:, as config.max_staleness"),
        )
    ),
    _roots("semantic_view"),
)
_NON_ADDITIVE = Node(
    (
        Field("ref", _TEMPLATE, "The dimension column the metric's rows are sorted by.", required=True),
        *_sort_fields("the dimension"),
        _renamed("order", "sort_direction"),
        _renamed("nulls", "null_order"),
        _removed("dimension", "a non-additive entry names its column as ref: \"{{ ref('<model>', '<column>') }}\""),
        _removed("table", "a non-additive entry names its model in its ref: \"{{ ref('<model>', '<column>') }}\""),
    )
)
_ORDER_BY = Node(
    (
        Field("ref", _TEMPLATE, "The dimension, or metric of the same table, to order by.", required=True),
        *_sort_fields("the entry"),
        _renamed("column", "ref"),
        _renamed("direction", "sort_direction"),
    ),
    shorthand=_TEMPLATE,
)
_WINDOW = Node(
    (
        Field("partition_by", _LIST, "Groups the window's rows by the listed entries.", items=_TEMPLATE),
        Field(
            "partition_by_excluding",
            _LIST,
            "Partitions by every requested dimension except those listed.",
            items=_TEMPLATE,
        ),
        Field(
            "order_by",
            _LIST,
            "Orders the window's rows; a bare reference is shorthand for `ref:`.",
            items=_ORDER_BY,
        ),
        Field("frame", _STRING, "The window frame, `ROWS` or `RANGE BETWEEN <bound> AND <bound>`; needs `order_by`."),
    )
)
METRIC = FileKind(
    "metric",
    "An aggregate, attached to every view that holds the models its expression names.",
    f"a `metric:` file, or an entry of a `metrics:` list, in {_IN}",
    Node(
        (
            _name("metric"),
            Field(
                "expr",
                _TEMPLATE,
                "The aggregate expression; the models its `ref()` calls name are the metric's tables.",
                required=True,
            ),
            Field("description", _STRING, "Renders as the metric's COMMENT, its lines joined into one.", loose=True),
            Field("synonyms", _LIST, "Other names for the metric, rendered as WITH SYNONYMS.", items=_STRING),
            Field(
                "using_relationships",
                _LIST,
                "The relationship to join through when several connect the metric's table.",
                items=_TEMPLATE,
            ),
            Field(
                "non_additive_dimensions",
                _LIST,
                "Makes the metric semi-additive: it aggregates only the last rows in these dimensions' order.",
                items=_NON_ADDITIVE,
            ),
            Field(
                "access_modifier",
                ValueKind.ENUM,
                "`private_access` renders the metric as PRIVATE; it is public when unset.",
                choices=ACCESS_MODIFIERS,
            ),
            Field(
                "window", _MAPPING, "Makes the metric a window function metric, such as a running total.", node=_WINDOW
            ),
            Field(
                "visibility",
                ValueKind.ENUM,
                "The 0.3 spelling of `access_modifier`, still read when written alone.",
                choices=tuple(VISIBILITY),
                status=FieldStatus.DEPRECATED,
                replacement="access_modifier",
            ),
            _renamed("non_additive_by", "non_additive_dimensions"),
            _removed("tables", "a metric's tables are the models its expression names with ref()"),
            _removed(
                "derived",
                "a metric is derived when its expression combines other metrics with metric() and names no column",
            ),
        )
    ),
    _roots("metric"),
)
FILTER = FileKind(
    "filter",
    "A named boolean expression, attached to every view that holds the models it names.",
    f"a `filter:` file, or an entry of a `filters:` list, in {_IN}",
    Node(
        (
            _name("filter"),
            Field("expr", _TEMPLATE, "The boolean expression; the models its `ref()` calls name are its tables."),
            Field(
                "description", _STRING, "Renders as the filter's COMMENT, or after its expression in prose.", loose=True
            ),
            Field(
                "labels",
                _LIST,
                "`[filter]` renders the filter as a native filter; without it the filter becomes prose.",
                items=_STRING,
            ),
            _removed("tables", "a filter's tables are the models its expression names with ref()"),
        )
    ),
    _roots("filter"),
)
CUSTOM_INSTRUCTION = FileKind(
    "custom_instruction",
    "A block of guidance for the view's AI SQL generation and question categorization.",
    f"a `custom_instruction:` file, or an entry of a `custom_instructions:` list, in {_IN}",
    Node(
        (
            _name("custom instruction block"),
            _documents("block"),
            Field(
                "ai_sql_generation",
                _TEMPLATE,
                "Guidance on shaping SQL, joined into the view's AI_SQL_GENERATION.",
                loose=True,
            ),
            Field(
                "ai_question_categorization",
                _TEMPLATE,
                "Guidance on classifying questions, joined into the view's AI_QUESTION_CATEGORIZATION.",
                loose=True,
            ),
            Field(
                "sql_generation",
                _TEMPLATE,
                "The 0.3 spelling of `ai_sql_generation`, still read when written alone.",
                status=FieldStatus.DEPRECATED,
                replacement="ai_sql_generation",
            ),
            Field(
                "question_categorization",
                _TEMPLATE,
                "The 0.3 spelling of `ai_question_categorization`, still read when written alone.",
                status=FieldStatus.DEPRECATED,
                replacement="ai_question_categorization",
            ),
        )
    ),
    _roots("custom_instruction"),
)
VERIFIED_QUERY = FileKind(
    "verified_query",
    "A question with SQL known to answer it, attached to every view that holds the models it names.",
    f"a `verified_query:` file, or an entry of a `verified_queries:` list, in {_IN}",
    Node(
        (
            _name("verified query"),
            _documents("verified query"),
            Field("question", _STRING, "The natural-language question the SQL answers.", required=True, loose=True),
            Field(
                "sql",
                _TEMPLATE,
                "The verified SQL; its `ref()` calls name its tables, or one `file()` call loads it.",
                required=True,
            ),
            Field(
                "verified_at",
                _STRING,
                "When the query was verified: epoch seconds, or a quoted ISO date or timestamp.",
                also=(ValueKind.INTEGER,),
            ),
            Field("verified_by", _STRING, "Who verified the query."),
            Field(
                "use_as_onboarding_question",
                ValueKind.BOOLEAN,
                "Publishes the question as an onboarding question.",
            ),
            _removed("tables", "a verified query's tables are the models its SQL names with ref()"),
            _removed("sql_file", "a verified query loads its SQL from a file with sql: \"{{ file('<path>') }}\""),
        )
    ),
    _roots("verified_query"),
)
_LEGACY_SHAPE = "a 1.0 relationship pairs its columns in relationship_conditions"
RELATIONSHIP = FileKind(
    "relationship",
    "A join between two models, attached to every view that holds both.",
    f"a `relationship:` file, or an entry of a `relationships:` list, in {_IN}",
    Node(
        (
            _name("relationship"),
            _documents("relationship"),
            Field("left_table", _TEMPLATE, "The many side of the join, as a `ref()` call.", required=True),
            Field("right_table", _TEMPLATE, "The one side of the join, as a `ref()` call.", required=True),
            Field(
                "relationship_conditions",
                _LIST,
                "Pairs of columns, left with right; the operator (`=`, `>=`, `BETWEEN`) decides the join.",
                required=True,
                items=_STRING,
            ),
            Field(
                "relationship_type",
                ValueKind.ANY,
                "Read only to report that nothing renders it.",
                status=FieldStatus.INERT,
            ),
            Field("join_type", ValueKind.ANY, "Read only to report that nothing renders it.", status=FieldStatus.INERT),
            _removed("relationship_columns", _LEGACY_SHAPE, "SST-PRS021"),
            _removed("left_column", _LEGACY_SHAPE, "SST-PRS021"),
            _removed("right_column", _LEGACY_SHAPE, "SST-PRS021"),
        )
    ),
    _roots("relationship"),
)
# The semantic kinds, in the order their nodes are checked.
SEMANTIC_KINDS = (SEMANTIC_VIEW, METRIC, FILTER, CUSTOM_INSTRUCTION, VERIFIED_QUERY, RELATIONSHIP)
