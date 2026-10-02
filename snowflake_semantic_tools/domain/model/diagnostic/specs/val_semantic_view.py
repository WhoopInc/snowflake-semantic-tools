"""Validation codes 3xx (VAL): a semantic view's column metadata, keys, and sample values."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.diagnostic.core import ErrorSpec, Severity, spec

TITLE: str = "Validation"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-VAL305",
        Severity.ERROR,
        "Fact column is not numeric",
        "{artifact}: fact '{member}' has type {found}",
        "use a numeric column, or make it a dimension",
    ),
    spec(
        "SST-VAL306",
        Severity.ERROR,
        "Time dimension column is not temporal",
        "{artifact}: time_dimension '{member}' has type {found}",
        "use a date or timestamp column, or change column_type",
    ),
    spec(
        "SST-VAL308",
        Severity.ERROR,
        "Column type metadata is absent",
        "{artifact}: column '{member}' declares no column_type",
        "declare dimension, time_dimension or fact, or run sst enrich to derive it from the column's type",
    ),
    spec(
        "SST-VAL309",
        Severity.ERROR,
        "Data type metadata is absent",
        "{artifact}: column '{member}' declares no data_type",
        "declare the Snowflake type, or run sst enrich to read it from the relation",
    ),
    spec(
        "SST-VAL310",
        Severity.ERROR,
        "Key column is absent",
        "{artifact}: primary_key names '{column}', absent from '{name}'",
        "correct the primary_key or unique_keys list",
    ),
    spec(
        "SST-VAL311",
        Severity.ERROR,
        "Relationship target declares no key",
        "{artifact}: '{name}' declares neither primary_key nor unique_keys, and a relationship references it",
        "declare primary_key (or unique_keys) in the model's config.meta.sst",
    ),
    spec(
        "SST-VAL312",
        Severity.WARNING,
        "Table declares no key",
        "{artifact}: '{name}' declares neither primary_key nor unique_keys",
        "declare one; cardinality is otherwise guessed from data",
    ),
    spec(
        "SST-VAL314",
        Severity.ERROR,
        "Enum has no sample values",
        "{artifact}: '{member}' is is_enum and declares no sample_values",
        "populate sample_values or clear is_enum",
    ),
    spec(
        "SST-VAL315",
        Severity.WARNING,
        "Non-enum sample values look exhaustive",
        "{artifact}: '{member}' declares {count} sample_values and is not is_enum",
        "set is_enum: true if the set is complete, or is_enum: false if it is a sample; "
        "sst enrich --include enums decides from the data",
    ),
    spec(
        "SST-VAL316",
        Severity.WARNING,
        "Auto-managed field contains a sentinel value",
        "{artifact}: '{member}'.{field} contains '{value}'",
        "delete the value; it is a missing-value placeholder, not data. "
        "sst enrich --force sample-values collects the column again",
    ),
    spec(
        "SST-VAL318",
        Severity.ERROR,
        "Excluded column is referenced",
        "{artifact}: '{member}' references excluded column '{column}'",
        "un-exclude the column or change the expression",
    ),
    spec(
        "SST-VAL325",
        Severity.WARNING,
        "Described column is absent from the relation",
        "model '{model}': column '{column}' is described in YAML and absent from the relation",
        "delete the column from the model YAML, or rebuild the model; sst enrich never deletes it",
    ),
    spec(
        "SST-VAL327",
        Severity.WARNING,
        "Declared data type differs from the relation",
        "model '{model}': column '{column}' declares data_type {declared}, and the relation has {found}",
        "correct data_type, or run sst enrich --force data-types",
    ),
    spec(
        "SST-VAL328",
        Severity.WARNING,
        "PII-tagged column carries sample values",
        "model '{model}': column '{column}' carries pii_tags and {count} sample_values",
        "delete the sample_values; sst enrich never samples a column with pii_tags",
    ),
)
