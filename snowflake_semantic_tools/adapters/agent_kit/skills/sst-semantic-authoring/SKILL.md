---
name: sst-semantic-authoring
description: |-
  Use when writing or changing SST semantic YAML: dbt column metadata under
  `config.meta.sst`, semantic views, metrics (derived, semi-additive, window), relationships,
  filters, verified queries, and custom instructions, and when checking what a change
  renders. Do NOT use for agents, tools, or evals (use sst-agents-and-evals), or to explain a
  diagnostic (use sst-fix-diagnostics).
---
# Authoring semantic YAML

The full rules are in @SST_DOCS@/guides/semantic-views.md and the key list in
@SST_DOCS@/reference/config.md. This skill is the working summary; when it and those pages
differ, trust the pages and `sst explain`.

## Before you edit

- Read `sst_config.yml` for `project.semantic_models_dir` (default `semantic_models`), and
  look at two or three existing files there to match the project's layout and naming.
- Find the models a view reads with `dbt ls` or the dbt YAML; SST reads the dbt manifest.

## Columns live in dbt

Each model's YAML carries SST metadata under `config.meta.sst`:

```yaml
models:
  - name: orders
    description: One row per order.
    config:
      meta:
        sst:
          primary_key: [order_id]
    columns:
      - name: order_id
        description: Surrogate key for the order.
        data_type: VARCHAR
        config:
          meta:
            sst:
              column_type: dimension
      - name: order_total
        description: Order value in cents, including tax.
        data_type: NUMBER(38,0)
        config:
          meta:
            sst:
              column_type: fact
```

- `column_type` is `dimension`, `fact`, or `time_dimension`. A column with none is in no
  view; `exclude: true` removes a column from every view.
- Identifiers are dimensions, never facts. Timestamps and dates are time dimensions.
  Measures are facts.
- Every column a view exposes needs a `data_type` (`SST-VAL309`). `sst enrich` can fill
  types from the warehouse, but it connects: ask first.
- `primary_key` is a list of columns and `unique_keys` a list of column lists.
- `is_enum: true` needs `sample_values` (`SST-VAL314`) and only fits a dimension
  (`SST-VAL335`).
- Write real descriptions. They are what Cortex Analyst reads to choose a column.

## Members find their views

Every YAML file under the semantic models directory is read, and each member is recognised
by its root key: `semantic_views:` (only under `semantic_views/`), `snowflake_metrics:`,
`snowflake_relationships:`, `snowflake_filters:`, `snowflake_verified_queries:`, and
`snowflake_custom_instructions:`. A view does not list most members. A member attaches to
every view whose `tables:` hold all of the member's `tables:`; a member that lands nowhere is
`SST-MEM005`. Custom instructions are the exception: a view names them.

```yaml
semantic_views:
  - name: sales
    description: |-
      Orders and their value. Use for order counts and revenue by customer. Do not use for
      product or line-item detail.
    tables:
      - "{{ ref('orders') }}"
      - "{{ ref('customers') }}"
```

A view's `description` must say when to use it (`SST-VAL005`): it is what an agent reads to
pick the view.

## Metrics

```yaml
snowflake_metrics:
  - name: total_revenue
    description: Order value in cents, including tax.
    tables: [orders]
    expr: "SUM({{ ref('orders', 'order_total') }})"
  - name: revenue_per_order
    derived: true
    description: Mean revenue per order.
    expr: "DIV0({{ metric('total_revenue') }}, {{ metric('order_count') }})"
```

- A metric that is not derived needs `tables:` (`SST-VAL109`) and an aggregate `expr`
  (`SST-VAL101`). A derived metric declares no tables (`SST-VAL108`).
- When two join paths connect a metric's table to another, name one with
  `using_relationships:` (`SST-VAL116`).
- Use `non_additive_dimensions:` for a balance that must not be summed over a date, and a
  `window:` block for running totals; never write `OVER (...)` in `expr`.

## Relationships

```yaml
snowflake_relationships:
  - name: orders_to_customers
    left_table: orders          # the many side, a bare model name
    right_table: customers      # the one side
    relationship_conditions:
      - "{{ ref('orders', 'customer_id') }} = {{ ref('customers', 'customer_id') }}"
```

- Endpoints are bare model names, never `ref()` (`SST-REF045`).
- `=` is an equality join, `>=` an as-of join, and `BETWEEN` a range join.
- The right table of an equality join needs a `primary_key` or `unique_keys`
  (`SST-VAL311`). Joining on part of a composite key fans out and inflates every metric
  across the join (`SST-VAL208`); make such a join as-of instead.
- Never write `relationship_type` or `join_type` (`SST-VAL211`).

## Filters, verified queries, custom instructions

- A filter is a boolean `expr` labelled `labels: [filter]`; an unlabelled boolean filter is
  `SST-VAL405`.
- A verified query has `question`, `tables`, and exactly one of `sql` or `sql_file`
  (`SST-VAL412`). Its SQL names tables by their logical names in the view.
- A custom instruction block uses `ai_sql_generation` and `ai_question_categorization`, and
  a view includes it with `"{{ custom_instructions('<name>') }}"`.

## After every edit

```bash
sst format
sst validate --no-snowflake-syntax-check
sst compile --emit-ddl target/ddl
```

Read the DDL for the views you touched: it is exactly what publishes. A full `sst validate`
also has Snowflake compile every expression; run it when the user is ready to connect.
