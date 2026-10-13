"""Validation codes 1xx (VAL): metrics -- aggregation, derivation, tables, join paths, and windows."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Severity, spec

TITLE: str = "Validation"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-VAL101",
        Severity.ERROR,
        "Metric expression is not an aggregate",
        "metric '{metric}' is table-scoped and its expr is not an aggregate",
        "wrap the expression in an aggregate; for a window function, declare a `window:` block instead of "
        "writing OVER (...) -- do not make the metric derived",
        condition="a table-scoped metric without a `window:` block has an expression with no aggregate at its root",
    ),
    spec(
        "SST-VAL102",
        Severity.ERROR,
        "Window function in a metric that renders at view level",
        "window function {function} in metric '{metric}', which renders at view level",
        "make it a table-scoped metric of one table with a `window:` block, or compute the window in the dbt "
        "model and aggregate the resulting column",
        demotable=False,
        condition=(
            "a derived metric's expression calls a window function (`... OVER (`) or the metric declares "
            "`window:`; or a `window:` metric does not belong to exactly one table, so it would render at "
            "view level as a derived metric does"
        ),
    ),
    spec(
        "SST-VAL103",
        Severity.ERROR,
        "Derived metric aggregates another metric",
        "derived metric '{metric}' aggregates '{other}'",
        "reference the metric without an outer aggregate",
        demotable=False,
        condition=("a derived metric's expression wraps a `metric()` reference in SUM, AVG, MIN, MAX, COUNT or MEDIAN"),
    ),
    spec(
        "SST-VAL105",
        Severity.ERROR,
        "Derived metric references an un-aggregated fact or dimension",
        "derived metric '{metric}' references un-aggregated {member_type} '{other}'",
        "aggregate it in a table-scoped metric first",
        demotable=False,
        condition="a derived metric's expression calls `fact()` or `dimension()`",
    ),
    spec(
        "SST-VAL106",
        Severity.ERROR,
        "Regular metric references a derived metric",
        "metric '{metric}' is table-scoped and references derived metric '{other}'",
        "make this metric derived, or inline the derived metric's expression",
        demotable=False,
        condition="a table-scoped metric's expression calls `metric()` on a derived metric",
    ),
    spec(
        "SST-VAL107",
        Severity.ERROR,
        "Regular metric references a non-additive metric",
        "metric '{metric}' references '{other}', which declares non_additive_dimensions",
        "make this metric derived, remove the reference, or drop the non-additive declaration",
        demotable=False,
        condition=(
            "a table-scoped metric's expression calls `metric()` on a metric that declares `non_additive_dimensions`"
        ),
        note="Only a derived metric may reference a semi-additive metric.",
    ),
    spec(
        "SST-VAL110",
        Severity.WARNING,
        "Metric expression contains an unwrapped bare identifier",
        "metric '{metric}' expression contains bare identifier '{column}'",
        "wrap it in `{{ ref('<model>', '<column>') }}` so it is checked",
        condition=(
            "a metric's expression names a column of one of its tables without `ref()`; only the first "
            "such column is reported"
        ),
    ),
    spec(
        "SST-VAL111",
        Severity.WARNING,
        "Division with no zero guard",
        "metric '{metric}' divides without DIV0 or NULLIF on the denominator",
        "wrap the denominator in NULLIF, or use DIV0",
        condition=("a metric's expression divides by anything other than `NULLIF(...)` or a non-zero number literal"),
    ),
    spec(
        "SST-VAL112",
        Severity.ERROR,
        "Metric expression touches a table outside the view",
        "metric '{metric}' in {artifact} reaches {outside}",
        "add the model to the view's `tables:`, or remove the reference",
        condition="a metric's expression calls `ref()` on a dbt model that is not among the metric's own tables",
    ),
    spec(
        "SST-VAL113",
        Severity.ERROR,
        "using_relationships declared on a derived metric",
        "derived metric '{metric}' declares using_relationships",
        "remove `using_relationships`; derived metrics have no join path",
        condition="a derived metric declares `using_relationships`",
    ),
    spec(
        "SST-VAL114",
        Severity.ERROR,
        "using_relationships does not start from the metric's table",
        "metric '{metric}': relationship '{other}' does not start from '{name}'",
        "name a relationship whose `left_table` is the metric's table",
        condition=(
            "a `using_relationships` entry names a relationship whose `left_table` is not the metric's first table"
        ),
    ),
    spec(
        "SST-VAL115",
        Severity.ERROR,
        "using_relationships is a chain",
        "metric '{metric}' declares a chain of {count} relationships",
        "declare one relationship, not a path",
        condition="`using_relationships` lists more than one relationship",
    ),
    spec(
        "SST-VAL116",
        Severity.ERROR,
        "using_relationships absent where the join path is ambiguous",
        "metric '{metric}' has {count} paths to '{name}' and declares no using_relationships",
        "declare `using_relationships` to pick the path",
        condition=(
            "two or more relationships join the metric's table to the same other table and the metric "
            "declares no `using_relationships`; only the first such metric per table pair is reported"
        ),
    ),
    spec(
        "SST-VAL117",
        Severity.WARNING,
        "Snapshot-grain metric declares no non-additive dimensions",
        "metric '{metric}' is over a snapshot grain and declares no non_additive_dimensions",
        "declare `non_additive_dimensions` over the snapshot date, or confirm the sum is additive",
        condition=(
            "a table-scoped `SUM` metric with no `window:` is over a snapshot grain -- a table whose "
            "primary or unique key holds a date or timestamp column -- and declares no "
            "`non_additive_dimensions`"
        ),
    ),
    spec(
        "SST-VAL118",
        Severity.ERROR,
        "non_additive_dimensions target does not resolve",
        "metric '{metric}': non_additive_dimensions names {value}, which does not resolve",
        "correct the table and dimension names",
        condition=(
            "a `non_additive_dimensions` entry names no column, an excluded column, a column that is not "
            "a dimension or time dimension, or a `table:` that is not one of the view's tables"
        ),
        note="An entry without `table:` names a dimension of the metric's own table.",
    ),
    spec(
        "SST-VAL119",
        Severity.INFO,
        "Effective non-additive dimension order",
        "metric '{metric}': effective non_additive_dimensions order is {value}",
        None,
        condition="a metric declares two or more non-additive dimensions, whose order is significant",
    ),
    spec(
        "SST-VAL120",
        Severity.WARNING,
        "Metric declares a sort direction with no null order",
        "metric '{metric}' declares sort_direction with no null_order",
        "declare `null_order` explicitly",
        condition=(
            "a non-additive dimension or window `order_by` entry sets `sort_direction` and no "
            "`null_order`; reported once per metric"
        ),
        note="Left unset, Snowflake's default null ordering applies.",
    ),
    spec(
        "SST-VAL121",
        Severity.ERROR,
        "access_modifier holds an unrecognised value",
        "metric '{metric}': access_modifier is '{found}'",
        "use `public_access` or `private_access`",
        condition="a metric's `access_modifier` is neither `public_access` nor `private_access`",
    ),
    spec(
        "SST-VAL122",
        Severity.ERROR,
        "visibility used instead of access_modifier",
        "metric '{metric}' uses visibility; the current key is access_modifier",
        "rename `visibility` to `access_modifier`, with `public_access` or `private_access` as its value",
        condition="a metric writes the 0.3 key `visibility` without `access_modifier`",
        note=(
            "An error, so a 0.3 project does not publish under 1.0 unmigrated. The old key is still read "
            "(`public` as `public_access`, `private` as `private_access`), so a run that demotes the code "
            "renders what was meant. Written beside `access_modifier`, it is `SST-PRS020` instead."
        ),
    ),
    spec(
        "SST-VAL123",
        Severity.ERROR,
        "Metric-level restriction set not enforced before publish",
        "metric '{metric}' would publish without the derived-metric restriction check",
        "report this as a bug",
        condition=(
            "a compiled view holds a metric that `SST-VAL102` to `SST-VAL107` reported as blocking; such a "
            "metric is never attached, so this is an SST defect"
        ),
        note="Snowflake enforces none of those derived-metric restrictions itself, so SST checks again before publish.",
    ),
    spec(
        "SST-VAL124",
        Severity.WARNING,
        "Duplicate metric expression under two names",
        "metric '{metric}' has the same expression as '{other}'",
        "keep one, and add the other's name to its `synonyms:`",
        condition=(
            "a table-scoped metric has the same expression (ignoring case and whitespace), tables, kind, "
            "non-additive order and window as an earlier metric"
        ),
    ),
    spec(
        "SST-VAL125",
        Severity.INFO,
        "Metric fan-out",
        "metric '{metric}' attaches to {count} views",
        None,
        condition="a metric attaches to more than one view; attachment is implicit, so its reach is reported",
    ),
    spec(
        "SST-VAL126",
        Severity.ERROR,
        "Window applies to a column",
        "metric '{metric}': {function} must apply to a metric or an aggregate to be a window metric",
        "pass {{ metric() }} or an aggregate such as SUM(...); a row-level window belongs in a fact or dimension",
        demotable=False,
        condition=(
            "the first argument of a `window:` metric's function is neither a `metric()` reference nor an "
            "aggregate, so the window would be row-level"
        ),
    ),
    spec(
        "SST-VAL127",
        Severity.ERROR,
        "Window frame without an order",
        "metric '{metric}': window frame '{value}' needs an order_by",
        "add `order_by`, or remove the frame",
        demotable=False,
        condition="a `window:` block declares `frame` and no `order_by`",
    ),
    spec(
        "SST-VAL128",
        Severity.ERROR,
        "Metric references a window metric",
        "metric '{metric}' references '{other}', a window function metric",
        "reference the metric the window applies to instead; Snowflake does not allow a window metric in "
        "another metric",
        demotable=False,
        condition="a metric's expression calls `metric()` on a metric that declares `window:`",
    ),
    spec(
        "SST-VAL129",
        Severity.ERROR,
        "Window entry does not resolve",
        "metric '{metric}': window {field} names {value}, which is not {expected}",
        "name a dimension the metric's table can reach, or a metric of the same table",
        demotable=False,
        condition=(
            "a window partition_by / partition_by_excluding / order_by entry names something that is "
            "not a dimension (or metric of the same table), or a dimension the metric's table cannot "
            "reach in this view"
        ),
    ),
)
