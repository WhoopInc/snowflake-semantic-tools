"""Validation codes 3xx (VAL): the semantic view object, its tables, columns, keys and scope."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Severity, spec

TITLE: str = "Validation"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-VAL301",
        Severity.ERROR,
        "View resolves no dimensions and no metrics",
        "{artifact} resolves no dimension and no metric",
        "attach at least one member, or enrich the base models",
        condition="a semantic view has no queryable member from any source",
    ),
    spec(
        "SST-VAL302",
        Severity.ERROR,
        "Resolved relation differs from the model name",
        "{artifact}: table '{name}' resolves to relation '{value}'",
        "point base_table at the resolved relation, not the model name",
        condition="an alias: makes the dbt relation differ from the model name",
    ),
    spec(
        "SST-VAL303",
        Severity.ERROR,
        "View references a disabled or ephemeral model",
        "{artifact}: '{name}' is {found} in dbt and produces no relation",
        "enable the model, or change its materialisation",
        condition="a referenced model is enabled: false or ephemeral",
    ),
    spec(
        "SST-VAL304",
        Severity.ERROR,
        "max_staleness below the accepted floor",
        "{artifact}: max_staleness is {found}; the minimum is 120",
        "raise max_staleness to at least 120",
        condition="a view declares max_staleness under 120 seconds",
    ),
    spec(
        "SST-VAL305",
        Severity.ERROR,
        "Fact column is not numeric",
        "{artifact}: fact '{member}' has type {found}",
        "use a numeric column, or make it a dimension",
        condition="a fact is declared over a non-numeric column",
    ),
    spec(
        "SST-VAL306",
        Severity.ERROR,
        "Time dimension column is not temporal",
        "{artifact}: time_dimension '{member}' has type {found}",
        "use a date or timestamp column, or change column_type",
        condition="a time dimension is declared over a non-temporal column",
    ),
    spec(
        "SST-VAL307",
        Severity.ERROR,
        "Publisher would replace without preserving grants",
        "{artifact} would be replaced without COPY GRANTS or CREATE OR ALTER",
        "emit COPY GRANTS on every replace",
        demotable=False,
        condition="a replace path omits grant preservation",
        note="Forbidden.",
    ),
    spec(
        "SST-VAL308",
        Severity.ERROR,
        "column_type metadata is absent",
        "{artifact}: column '{member}' declares no column_type",
        "declare dimension, time_dimension or fact",
        condition="a column consumed by the semantic layer has no column_type",
    ),
    spec(
        "SST-VAL309",
        Severity.ERROR,
        "data_type metadata is absent",
        "{artifact}: column '{member}' declares no data_type",
        "declare the Snowflake type",
        condition="a column has no data_type from dbt or from meta.sst",
    ),
    spec(
        "SST-VAL310",
        Severity.ERROR,
        "Primary key column is not on the table",
        "{artifact}: primary_key names '{column}', absent from '{name}'",
        "correct the primary_key list",
        condition=(
            "a column named in a model's `config.meta.sst.primary_key` or `.unique_keys` does not exist on that model"
        ),
    ),
    spec(
        "SST-VAL311",
        Severity.ERROR,
        "Table declares no primary key and one is required",
        "{artifact}: '{name}' declares neither primary_key nor unique_keys, and a relationship references it",
        "declare primary_key in config.meta.sst",
        condition="a table that must declare a key does not",
    ),
    spec(
        "SST-VAL312",
        Severity.WARNING,
        "Table declares no primary key or unique keys",
        "{artifact}: '{name}' declares neither primary_key nor unique_keys",
        "declare one; cardinality is otherwise guessed from data",
        condition="a table leaves the cheapest fan-out protection unused",
    ),
    spec(
        "SST-VAL314",
        Severity.ERROR,
        "Enum column declares no sample values",
        "{artifact}: '{member}' is is_enum and declares no sample_values",
        "populate sample_values, or clear is_enum",
        condition="a closed value set is asserted with no values",
    ),
    spec(
        "SST-VAL315",
        Severity.WARNING,
        "Non-enum column declares sample values that look exhaustive",
        "{artifact}: '{member}' declares {count} sample_values and is not is_enum",
        "set is_enum if the set is genuinely closed",
        condition="a sampled value set may be being read as complete",
    ),
    spec(
        "SST-VAL316",
        Severity.WARNING,
        "Auto-managed field contains a sentinel value",
        "{artifact}: '{member}'.{field} contains '{value}'",
        "re-run sst enrich; the value came from a pandas round-trip",
        condition="an auto-managed field holds nan, NaN, None, null or `<NA>`",
    ),
    spec(
        "SST-VAL317",
        Severity.WARNING,
        "Auto-managed field hand-edited",
        "{artifact}: '{member}'.{field} differs from the enriched value",
        "let sst enrich own the field",
        condition="a field SST manages was edited by hand",
    ),
    spec(
        "SST-VAL318",
        Severity.ERROR,
        "Excluded column referenced by a member",
        "{artifact}: '{member}' references excluded column '{column}'",
        "un-exclude the column, or change the expression",
        condition="an expression names a column marked exclude",
    ),
    spec(
        "SST-VAL319",
        Severity.INFO,
        "Member fan-out onto views",
        "{artifact}: {value}",
        None,
        condition="attachment is implicit, so the reach is reported per view",
    ),
    spec(
        "SST-VAL320",
        Severity.ERROR,
        "Drift comparison not normalised on both sides",
        "{artifact}: drift comparison compared raw DDL",
        "normalise both sides before comparing",
        condition="a definition comparison did not normalise the live side",
    ),
    spec(
        "SST-VAL321",
        Severity.ERROR,
        "Tags would be set by the create statement",
        "{artifact}: tags would be set by CREATE OR ALTER, which cannot set them",
        "apply tags in a separate ALTER",
        condition="tag application is fused to the create path",
    ),
    spec(
        "SST-VAL322",
        Severity.WARNING,
        "Two views share a table with contradictory descriptions",
        "{a} and {b} share '{name}' with conflicting descriptions",
        "reconcile the two descriptions",
        condition="one table is described differently in two views",
    ),
    spec(
        "SST-VAL323",
        Severity.ERROR,
        "Base model has no columns block",
        "{artifact}: '{name}' has no columns: block in dbt",
        "add a columns: block so column refs can be checked",
        condition="a referenced model exposes no column metadata",
    ),
    spec(
        "SST-VAL324",
        Severity.WARNING,
        "Base model has no contract and no tests",
        "{artifact}: '{name}' has {detail}",
        "add a contract, or at least a uniqueness test on the grain",
        condition="a model feeding a view has no contract or no tests at all",
    ),
    spec(
        "SST-VAL325",
        Severity.WARNING,
        "Manifest column absent from the relation",
        "model '{model}': column '{column}' is described in YAML and absent from the relation",
        "remove the stale column entry, or add the column to the model",
        condition="`enrich` found a `nodes.<id>.columns` entry the warehouse relation does not have",
    ),
    spec(
        "SST-VAL326",
        Severity.ERROR,
        "Undefined bare identifier in an attached member",
        "view '{view}': member '{member}' references '{identifier}', which is neither a column on the view's "
        "tables nor a variable the view declares",
        "declare the variable on this view, or correct the identifier",
        condition="an attached member's `expr:` carries a bare identifier the view cannot resolve",
        note="Snowflake would reject the whole CREATE with `invalid identifier`",
    ),
    spec(
        "SST-VAL327",
        Severity.WARNING,
        "Declared data type differs from the relation",
        "model '{model}': column '{column}' declares data_type {declared}, and the relation has {found}",
        "correct data_type, or run sst enrich --force data-types",
        condition="a written meta.sst.data_type differs from the relation's type",
    ),
    spec(
        "SST-VAL328",
        Severity.WARNING,
        "PII-tagged column carries sample values",
        "model '{model}': column '{column}' carries pii_tags and {count} sample_values",
        "delete the sample_values; sst enrich never samples a column with pii_tags",
        condition="a column with pii_tags carries sample_values",
    ),
    spec(
        "SST-VAL329",
        Severity.ERROR,
        "View scope declares include and exclude for one kind",
        "{artifact}: '{field}' and '{other}' are both declared; a view either includes or excludes {kind}",
        "keep one of the two lists",
        condition=(
            "a semantic view declares an include list and the matching exclude list for one kind: "
            "columns with exclude_columns, metrics with exclude_metrics, or relationships with "
            "exclude_relationships"
        ),
    ),
    spec(
        "SST-VAL330",
        Severity.ERROR,
        "View scope names an item that does not exist",
        "{artifact}: {field} names {kind} '{name}', which {reason}",
        "correct the name, or remove the entry",
        condition=(
            "an entry in columns, metrics, relationships or their exclude_ lists names nothing the "
            "view's tables provide, or an include list (columns) names a column excluded globally"
        ),
    ),
    spec(
        "SST-VAL331",
        Severity.WARNING,
        "Excluded column is already excluded globally",
        "{artifact}: exclude_columns names '{column}', which is already excluded globally",
        "remove the entry; the column is excluded from every view",
        condition=(
            "an exclude_columns entry names a column whose dbt metadata already excludes it, so the "
            "entry changes nothing"
        ),
    ),
    spec(
        "SST-VAL332",
        Severity.ERROR,
        "Metric needs a relationship the view excludes",
        "{artifact}: metric '{metric}' needs relationship '{relationship}', which this view excludes",
        "include the relationship, or exclude the metric",
        condition=(
            "a metric left in the view's scope names, in using_relationships, a relationship the view's scope removes"
        ),
    ),
)
