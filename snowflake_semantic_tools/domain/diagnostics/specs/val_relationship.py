"""Validation codes 2xx (VAL): relationships -- conditions, endpoints, keys, join paths, and cycles."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Severity, spec

TITLE: str = "Validation"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-VAL201",
        Severity.ERROR,
        "relationship_conditions is empty",
        "relationship '{relationship}' declares no conditions",
        "declare at least one condition under `relationship_conditions`",
        condition="a relationship's `relationship_conditions` is absent, empty or not a list",
        note="A relationship written in the 0.3 `relationship_columns` shape is `SST-PRS021` instead.",
    ),
    spec(
        "SST-VAL202",
        Severity.ERROR,
        "A declared condition would not be emitted",
        "relationship '{relationship}': condition '{value}' would be dropped from the DDL",
        "remove the extra condition: a relationship renders one range alone, or equalities with at most one ASOF "
        "comparison",
        demotable=False,
        condition=(
            "a relationship declares a second ASOF comparison, a second range, or any condition beside a "
            "range, none of which the renderer emits"
        ),
    ),
    spec(
        "SST-VAL203",
        Severity.ERROR,
        "Relationship names a table not in the view",
        "relationship '{relationship}' names '{name}', absent from {artifact}",
        "add the missing table to the view's `tables:`, or remove the relationship from the view's scope list",
        condition=(
            "a view's `relationships` or `exclude_relationships` entry names a relationship one of whose "
            "tables the view does not hold"
        ),
    ),
    spec(
        "SST-VAL204",
        Severity.ERROR,
        "Join condition column is not on the named table",
        "relationship '{relationship}': '{column}' is not on '{name}'",
        "correct the column, or swap the sides",
        condition="a column a condition refs is on a table other than its side's `left_table` or `right_table`",
    ),
    spec(
        "SST-VAL205",
        Severity.ERROR,
        "Relationship sides never appear together in any view",
        "relationship '{relationship}' joins '{a}' and '{b}', which share no view",
        "add both tables to one view, or drop the relationship",
        condition="no semantic view holds both the relationship's `left_table` and `right_table`",
        note="The relationship then attaches to no view, which `SST-MEM005` also reports.",
    ),
    spec(
        "SST-VAL206",
        Severity.ERROR,
        "Range relationship target declares no distinct_range",
        "relationship '{relationship}' is a range join and '{name}' declares no distinct_range",
        "declare `table_config.<model>.distinct_range` on the target table in the view",
        condition=(
            "a range relationship attaches to a view whose `table_config` declares no `distinct_range` "
            "for its `right_table`"
        ),
    ),
    spec(
        "SST-VAL207",
        Severity.ERROR,
        "Self-loop on a single logical table",
        "relationship '{relationship}' joins '{name}' to itself",
        "remove the relationship; one model cannot appear under two logical names in 1.0, so model the second "
        "role as its own dbt model",
        condition="a relationship's `left_table` and `right_table` are the same table",
    ),
    spec(
        "SST-VAL208",
        Severity.WARNING,
        "Equality join to a finer-grained table",
        "relationship '{relationship}' joins '{a}' to '{b}', whose grain is finer than one row per key",
        "join on every column of the target's key, or aggregate the target to the join grain",
        condition=(
            "an equality relationship joins on only part of a composite `primary_key` or `unique_keys` "
            "entry of its `right_table`, so several target rows can match each source row"
        ),
    ),
    spec(
        "SST-VAL209",
        Severity.WARNING,
        "Ambiguous join path between two tables",
        "{artifact}: {count} paths between '{a}' and '{b}'",
        "declare `using_relationships` on the metrics that cross the pair",
        condition="two or more relationships join the same pair of tables, in a view that holds both",
        note="Each metric that needs a path through the pair and names none is `SST-VAL116`.",
    ),
    spec(
        "SST-VAL210",
        Severity.WARNING,
        "Join target declares no key covering the join columns",
        "relationship '{relationship}': '{name}' declares neither primary_key nor unique_keys over {value}",
        "join on a declared key of the target, or declare `unique_keys` over the join columns",
        condition=(
            "an equality relationship's `right_table` declares keys, and none of them is covered by the join columns"
        ),
        note="A target that declares no key at all is `SST-VAL311`.",
    ),
    spec(
        "SST-VAL211",
        Severity.ERROR,
        "relationship_type or join_type declared",
        "relationship '{relationship}' declares '{field}', which is not emitted",
        "remove the key; the join's direction is `left_table` (the many side) to `right_table` (the one side)",
        condition="a relationship declares `relationship_type` or `join_type`, 0.3 keys the renderer emits nothing for",
        note=(
            "An error, so a 0.3 project does not publish under 1.0 unmigrated; a run that demotes the "
            "code publishes the relationship without the key."
        ),
    ),
    spec(
        "SST-VAL212",
        Severity.WARNING,
        "Relationship target repeats a join key in the data",
        "relationship '{relationship}' declares cardinality {found}; the spot-check found {value}",
        "fix the target's grain so each key appears once, or swap `left_table` and `right_table`",
        condition=(
            "connected validate counts rows of an equality relationship's `right_table` that repeat a "
            "join key, so the many-to-one join the relationship implies does not hold"
        ),
    ),
    spec(
        "SST-VAL213",
        Severity.ERROR,
        "Multi-column condition is not expressible",
        "relationship '{relationship}': condition '{value}' spans multiple columns per side",
        "split it into one condition per column pair",
        condition="one side of a condition refs more than one column",
    ),
    spec(
        "SST-VAL214",
        Severity.ERROR,
        "using_relationships names an undeclared relationship",
        "metric '{metric}' names relationship '{relationship}', which is not declared",
        "declare the relationship, or correct the name",
        condition="a metric's `using_relationships` lists a bare name that no relationship declares",
        note="A `{{ relationship() }}` call naming no relationship is `SST-REF029`.",
    ),
    spec(
        "SST-VAL215",
        Severity.ERROR,
        "Relationship graph has a cycle that changes results",
        "{artifact}: relationship cycle {cycle}",
        "remove a relationship so the path no longer returns to its start",
        condition=(
            "a view's relationships, read from `left_table` to `right_table`, form a cycle through two or more tables"
        ),
        note="Snowflake refuses the view. A table joined to itself is `SST-VAL207`.",
    ),
    spec(
        "SST-VAL216",
        Severity.INFO,
        "Inferred many-to-many path through a bridge table",
        "{artifact}: many-to-many path {value} inferred through '{name}'",
        None,
        condition=(
            "a table is the `left_table` of equality relationships to two other tables, so those two "
            "join many-to-many through it; each pair is reported once"
        ),
    ),
    spec(
        "SST-VAL217",
        Severity.INFO,
        "Join graph summary",
        "{artifact}: {count} tables, {value}",
        None,
        condition=(
            "a view has at least one relationship in scope; its tables, relationships and connected parts are reported"
        ),
    ),
    spec(
        "SST-VAL218",
        Severity.ERROR,
        "Range join target has overlapping ranges",
        "relationship '{relationship}': '{name}' declares distinct_range over ({a}, {b}) but the ranges "
        "overlap, for example {value}",
        "make the ranges disjoint, or narrow the range columns",
        condition=(
            "connected validate finds rows of a range relationship's `right_table` whose "
            "`distinct_range` ranges overlap, so one source row joins more than one target row"
        ),
    ),
    spec(
        "SST-VAL219",
        Severity.ERROR,
        "A scoped bare column does not exist on its model",
        "{artifact}: table_config.{model}.{field} names column '{column}', which does not exist on {model}",
        "correct the column name, or add it to the dbt model",
        condition=(
            "a `start` or `end` column under a view's `table_config.<model>.distinct_range` is absent "
            "from `<model>` in the dbt manifest"
        ),
        note="Primary and unique keys live on the dbt model; their existence check is `SST-VAL310`.",
    ),
    spec(
        "SST-VAL220",
        Severity.WARNING,
        "A declared view variable is never referenced",
        "{artifact}: variable '{name}' is declared and never used",
        "reference it in an expr, or remove it",
        condition="a name in a view's `variables:` appears in no `expr:` of a metric or filter the view attaches",
    ),
    spec(
        "SST-VAL221",
        Severity.WARNING,
        "A bare identifier in an expr matches no column and no declared variable",
        "{artifact}: '{name}' in {field} is neither a column nor a declared variable",
        "declare the variable, correct the name, or wrap the column in a two-arg ref",
        condition=(
            "the `expr:` of a filter not labelled `filter`, which renders as prose in the view's "
            "instructions, has a bare word that is neither a column of the view's tables nor a variable "
            "the view declares"
        ),
        note="In a metric or a labelled filter, which the view is created with, the same word is `SST-VAL326`.",
    ),
    spec(
        "SST-VAL222",
        Severity.ERROR,
        "Dimension declares a private access modifier",
        "{artifact}: dimension '{member}' declares access_modifier '{value}'; SST does not support a private dimension",
        "remove `access_modifier` from the column",
        condition="a `dimension` or `time_dimension` column carries `access_modifier: private_access`",
    ),
    spec(
        "SST-VAL223",
        Severity.ERROR,
        "A primary-key column is also declared unique",
        "{artifact}: column '{column}' on '{model}' appears in both primary_key and unique_keys",
        "remove it from `unique_keys` -- a primary key is already unique",
        condition="a column appears in both a model's `config.meta.sst.primary_key` and `.unique_keys`",
        note="`{artifact}` is the model, not a view.",
    ),
)
