"""Validation codes 3xx (VAL): the semantic view object, its tables, columns, keys and scope."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Precheck, Severity, spec

TITLE: str = "Validation"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-VAL301",
        Severity.ERROR,
        "View resolves no dimensions and no metrics",
        "{artifact} resolves no dimension and no metric",
        "attach a metric or a labelled filter, or set `column_type` on the base models' columns (`sst enrich`)",
        condition=(
            "a semantic view's tables have no dimension or time dimension in scope, and no metric or "
            "filter labelled `filter` attaches to it"
        ),
    ),
    spec(
        "SST-VAL302",
        Severity.ERROR,
        "Resolved relation differs from the model name",
        "{artifact}: table '{name}' resolves to relation '{value}'",
        "report this as a bug; SST renders each table at its dbt model's resolved relation",
        condition=(
            "a table SST built would render against a relation other than its dbt model's resolved "
            "`relation_name`, which honours `alias:`; this is an SST defect"
        ),
    ),
    spec(
        "SST-VAL303",
        Severity.ERROR,
        "View references a disabled or ephemeral model",
        "{artifact}: '{name}' is {found} in dbt and produces no relation",
        "enable the model, or change its materialisation",
        condition="a view's `tables:` names a dbt model that is `enabled: false` or materialised as `ephemeral`",
    ),
    spec(
        "SST-VAL305",
        Severity.ERROR,
        "Fact column is not numeric",
        "{artifact}: fact '{member}' has type {found}",
        "use a numeric column, or set its `column_type` to `dimension`",
        condition="a column with `column_type: fact` has a data type that is not numeric",
    ),
    spec(
        "SST-VAL306",
        Severity.ERROR,
        "Time dimension column is not temporal",
        "{artifact}: time_dimension '{member}' has type {found}",
        "use a date or timestamp column, or change `column_type`",
        condition="a column with `column_type: time_dimension` has a data type that is not a date or timestamp",
    ),
    spec(
        "SST-VAL307",
        Severity.ERROR,
        "Publisher would replace without preserving grants",
        "{artifact} would be replaced without COPY GRANTS or CREATE OR ALTER",
        "report this as a bug",
        demotable=False,
        condition="a `CREATE OR REPLACE` statement SST rendered for a view omits `COPY GRANTS`; this is an SST defect",
        note="Replacing without `COPY GRANTS` drops every grant on the view.",
    ),
    spec(
        "SST-VAL308",
        Severity.ERROR,
        "column_type metadata is absent",
        "{artifact}: column '{member}' declares no column_type",
        "set `column_type` in the column's `meta.sst` to `dimension`, `time_dimension` or `fact`, set "
        "`exclude: true` to keep it out of every view, or run `sst enrich`",
        condition="a column of a dbt model a view uses has no `column_type` and is not excluded (`exclude: true`)",
    ),
    spec(
        "SST-VAL309",
        Severity.ERROR,
        "data_type metadata is absent",
        "{artifact}: column '{member}' declares no data_type",
        "declare `data_type` in dbt or in `meta.sst`, or run `sst enrich`",
        condition="a column of a dbt model a view uses has no data type from dbt or from `meta.sst`",
    ),
    spec(
        "SST-VAL310",
        Severity.ERROR,
        "Key column is not on the model",
        "{artifact}: key column '{column}' is absent from '{name}'",
        "correct the `primary_key` or `unique_keys` entry",
        condition=(
            "a column named in a model's `config.meta.sst.primary_key` or `.unique_keys` does not exist on that model"
        ),
    ),
    spec(
        "SST-VAL311",
        Severity.ERROR,
        "Table declares no primary key and one is required",
        "{artifact}: '{name}' declares neither primary_key nor unique_keys, and a relationship references it",
        "declare `primary_key` or `unique_keys` in the model's `config.meta.sst`",
        condition=(
            "the `right_table` of an equality relationship declares neither `primary_key` nor "
            "`unique_keys`; Snowflake refuses a relationship to a table with no key"
        ),
    ),
    spec(
        "SST-VAL312",
        Severity.WARNING,
        "Table declares no primary key or unique keys",
        "{artifact}: '{name}' declares neither primary_key nor unique_keys",
        "declare one; cardinality is otherwise guessed from data",
        condition=(
            "a dbt model a view uses declares neither `primary_key` nor `unique_keys` in "
            "`config.meta.sst`, so cardinality is left to the data"
        ),
    ),
    spec(
        "SST-VAL314",
        Severity.ERROR,
        "Enum column declares no sample values",
        "{artifact}: '{member}' is is_enum and declares no sample_values",
        "populate `sample_values`, or clear `is_enum`",
        condition="a column declares `is_enum: true` and no `sample_values`",
    ),
    spec(
        "SST-VAL315",
        Severity.WARNING,
        "Non-enum column declares sample values that look exhaustive",
        "{artifact}: '{member}' declares {count} sample_values and is not is_enum",
        "set `is_enum: true` if the set is closed, or `is_enum: false` to mark the values a sample",
        condition="a column that is not a fact declares five or more `sample_values` and no `is_enum`",
    ),
    spec(
        "SST-VAL316",
        Severity.WARNING,
        "Auto-managed field contains a sentinel value",
        "{artifact}: '{member}'.{field} contains '{value}'",
        "delete the value, or run `sst enrich --force sample-values` to sample the column again",
        condition=(
            "a column's `sample_values` holds a missing value written as text: `nan`, `none`, `null` or "
            "`<na>`, in any case; checked on every dbt model"
        ),
    ),
    spec(
        "SST-VAL317",
        Severity.WARNING,
        "Written is_enum contradicts the sampled data",
        "{artifact}: '{member}'.{field} differs from the enriched value",
        "set `is_enum: false`, or run `sst enrich --force enums`",
        condition=(
            "`sst enrich` samples a dimension whose written `is_enum: true` the data shows is not a "
            "closed set, and keeps the written value"
        ),
    ),
    spec(
        "SST-VAL318",
        Severity.ERROR,
        "Excluded column referenced by a member",
        "{artifact}: '{member}' references excluded column '{column}'",
        "un-exclude the column, or change the expression",
        condition=(
            "an expression names a column excluded globally; or a metric a view keeps (its expression, "
            "window or non-additive entries), or a filter or verified query attached to the view, "
            "reads with `ref()` a column the view's scope removes"
        ),
        note=(
            "A scoped-out column is hidden only as a fact or dimension; a member that reads it would "
            "still compute over it, so the view is refused. Bare column names in a verified query's SQL "
            "are not checked."
        ),
    ),
    spec(
        "SST-VAL319",
        Severity.INFO,
        "Member fan-out onto views",
        "{artifact}: {value}",
        None,
        condition=(
            "a view holds relationships, filters, metrics or verified queries by table membership; "
            "attachment is implicit, so the count is reported per view"
        ),
    ),
    spec(
        "SST-VAL320",
        Severity.ERROR,
        "Drift comparison not normalised on both sides",
        "{artifact}: drift comparison compared raw DDL",
        "report this as a bug",
        condition=(
            "a plan compares a recorded or rendered definition that is not a normalised digest, so raw "
            "text would decide drift; this is an SST defect"
        ),
    ),
    spec(
        "SST-VAL321",
        Severity.ERROR,
        "Tags would be set by the create statement",
        "{artifact}: tags would be set by CREATE OR ALTER, which cannot set them",
        "report this as a bug",
        condition="a `CREATE OR ALTER` statement SST rendered for a view sets tags; this is an SST defect",
        note="`CREATE OR ALTER` cannot set tags, so SST applies them in a separate `ALTER`.",
    ),
    spec(
        "SST-VAL323",
        Severity.ERROR,
        "Base model has no columns block",
        "{artifact}: '{name}' has no columns: block in dbt",
        "add a `columns:` block so column refs can be checked",
        condition="a dbt model a view's `tables:` names declares no columns",
    ),
    spec(
        "SST-VAL324",
        Severity.WARNING,
        "Base model has no contract and no tests",
        "{artifact}: '{name}' has {detail}",
        "add a contract, or at least a uniqueness test on the grain",
        condition="a dbt model a view's `tables:` names has neither a contract nor any test",
    ),
    spec(
        "SST-VAL325",
        Severity.WARNING,
        "Manifest column absent from the relation",
        "model '{model}': column '{column}' is described in YAML and absent from the relation",
        "remove the stale column entry, or add the column to the model",
        condition=(
            "`sst enrich` finds a column the model YAML describes that the relation lacks, or "
            "`sst validate --verify-schema` finds a column a view reads missing from its table"
        ),
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
        "correct `data_type`, or run `sst enrich --force data-types`",
        condition=(
            "`sst enrich` finds a written `meta.sst.data_type` that differs from the relation's type, "
            "on a column dbt gives no `data_type`"
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-VAL328",
        Severity.WARNING,
        "PII-tagged column carries sample values",
        "model '{model}': column '{column}' carries pii_tags and {count} sample_values",
        "delete the `sample_values`; `sst enrich` never samples a column with `pii_tags`",
        condition="an `sst enrich` run that reads row data finds a column with `pii_tags` that carries `sample_values`",
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
            "a metric left in the view's scope names, in `using_relationships`, a relationship the view's "
            "scope removes, or one whose tables the view does not hold"
        ),
    ),
    spec(
        "SST-VAL333",
        Severity.ERROR,
        "Column access_modifier holds an unrecognised value",
        "{artifact}: column '{member}' declares access_modifier '{found}'",
        "use `public_access`, or remove `access_modifier` from the column",
        condition=("a column's `meta.sst.access_modifier` is neither `public_access` nor `private_access`"),
    ),
    spec(
        "SST-VAL334",
        Severity.ERROR,
        "Fact declares a private access modifier",
        "{artifact}: fact '{member}' declares access_modifier 'private_access'; SST does not render a private fact",
        "remove `access_modifier` from the column, or hide what it measures behind a private metric",
        condition=("a `fact` column that is not excluded carries `meta.sst.access_modifier: private_access`"),
        note=(
            "A fact renders without `PRIVATE`, so it would publish as public; it is refused instead. "
            "A private dimension is SST-VAL222. A column with no `column_type`, or one excluded, "
            "never renders, so its modifier is not refused."
        ),
    ),
    spec(
        "SST-VAL335",
        Severity.ERROR,
        "Fact declares is_enum",
        "{artifact}: fact '{member}' declares is_enum; only a dimension can be an enum",
        "remove `is_enum`, or change `column_type` to `dimension`",
        condition="a `fact` column that is not excluded declares `meta.sst.is_enum: true`",
        note="Snowflake refuses `IS_ENUM` on a fact when the view is created, so no setting lowers this.",
        demotable=False,
    ),
    spec(
        "SST-VAL336",
        Severity.ERROR,
        "Metric is built on a metric the view excludes",
        "{artifact}: keeps metric '{metric}', which is built on '{other}', which its scope removes",
        "keep the base metric, or exclude the metric built on it too",
        condition=(
            "a metric left in the view's scope calls `metric()` on a metric the view's tables attach "
            "and its scope removes, so the view would hold the metric without its base"
        ),
    ),
)
