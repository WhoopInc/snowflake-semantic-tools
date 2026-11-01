"""The key tables of the `meta.sst` blocks a dbt model and its columns carry for SST."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.authored import ACCESS_MODIFIERS
from snowflake_semantic_tools.domain.model.authored_schema.field_types import (
    Field,
    FieldStatus,
    FileKind,
    Node,
    ValueKind,
)
from snowflake_semantic_tools.domain.model.semantic_view import ColumnKind

_STRING, _LIST, _BOOLEAN = ValueKind.STRING, ValueKind.LIST, ValueKind.BOOLEAN


def _location(key: str) -> Field:
    return Field(
        key,
        ValueKind.ANY,
        f"Refused: the table's {key} is the model's resolved {key} in the dbt manifest.",
        status=FieldStatus.REFUSED,
        code="SST-DBT030",
    )


DBT_MODEL_META = FileKind(
    "dbt_model_meta",
    "What a dbt model tells SST about its table: its keys, synonyms and distinct range.",
    "`config.meta.sst` (or `meta.sst`) of a model in the dbt project's YAML",
    Node(
        (
            Field("primary_key", _LIST, "The table's key columns, rendered as PRIMARY KEY.", items=_STRING),
            Field(
                "unique_keys",
                _LIST,
                "Lists of columns, each rendered as a UNIQUE constraint.",
                items=_LIST,
            ),
            Field("synonyms", _LIST, "Other names for the table, rendered in every view that holds it.", items=_STRING),
            Field(
                "distinct_range",
                ValueKind.MAPPING,
                "Two columns, `start` and `end`, whose ranges do not overlap; rendered as a DISTINCT RANGE constraint.",
            ),
            _location("database"),
            _location("schema"),
        )
    ),
)
DBT_COLUMN_META = FileKind(
    "dbt_column_meta",
    "What a dbt model's column tells SST: whether it is a fact or a dimension, and how to describe it.",
    "`config.meta.sst` (or `meta.sst`) of a column of a model in the dbt project's YAML",
    Node(
        (
            Field(
                "column_type",
                ValueKind.ENUM,
                "Whether the column renders as a fact or a dimension; without it the column is in no view.",
                choices=tuple(kind.value for kind in ColumnKind),
            ),
            Field("data_type", _STRING, "The column's data type, when the model does not declare one."),
            Field("synonyms", _LIST, "Other names for the column, rendered as WITH SYNONYMS.", items=_STRING),
            Field(
                "sample_values",
                _LIST,
                "Example values, rendered as SAMPLE_VALUES; never published for a sensitive column.",
                items=ValueKind.ANY,
            ),
            Field("is_enum", _BOOLEAN, "The sample values are the column's complete set; dimensions only."),
            Field("exclude", _BOOLEAN, "Removes the column from every view."),
            Field(
                "access_modifier",
                ValueKind.ENUM,
                "Only `public_access` is honored on a column.",
                choices=ACCESS_MODIFIERS,
            ),
            Field(
                "governance_tags",
                _LIST,
                "Sensitive tags enrich recorded; they mark the column sensitive, so it is never sampled.",
                items=_STRING,
            ),
        )
    ),
)
