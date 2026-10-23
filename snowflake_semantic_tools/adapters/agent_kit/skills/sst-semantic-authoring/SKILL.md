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

## Every reference is a call

Every field that names a model, a column, another member, or a file takes exactly one
`{{ }}` call: `ref()`, `metric()`, `relationship()`, `custom_instructions()`, `tag()`,
`file()`. A bare name where a call belongs is `SST-REF050`, which names the function to write;
another function's call is `SST-REF041`. Snowflake object names, view variables, and an item's
own `name:` stay literal. @SST_DOCS@/guides/references.md lists every field.

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
          synonyms: [sale, transaction]
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
- A table's metadata is its model's too: the model `description` is the table's COMMENT, and
  model-level `synonyms` and `distinct_range: {start, end}` sit beside the keys. A view never
  writes `table_config` (`SST-PRS126`).
- `is_enum: true` needs `sample_values` (`SST-VAL314`) and only fits a dimension
  (`SST-VAL335`).
- Write real descriptions. They are what Cortex Analyst reads to choose a column.

## Members find their views

Write one item per file, named for it (`metrics/total_revenue.yml`), under its singular root
as a mapping: `semantic_view:` (only under `semantic_views/`), `metric:`, `relationship:`,
`filter:`, `verified_query:`, or `custom_instruction:`. A list under the plural root
(`semantic_views:`, `snowflake_metrics:`, `snowflake_relationships:`, `snowflake_filters:`,
`snowflake_verified_queries:`, `snowflake_custom_instructions:`) is still read, as several
items in one file; a file holding both forms of one type is `SST-PRS003`. Every YAML file
under the semantic models directory is read, at any depth, and each member is recognized by
its root key.

A view does not list most members. A member's tables are the models its `ref()` calls name,
and it attaches to every view whose `tables:` hold all of them; a member that lands nowhere is
`SST-MEM005`. Custom instructions are the exception: a view names them.

```yaml
# semantic_views/sales.yml
semantic_view:
  name: sales
  description: |-
    Orders and their value. Use for order counts and revenue by customer. Do not use for
    product or line-item detail.
  tables:
    - "{{ ref('orders') }}"
    - "{{ ref('customers') }}"
```

A view's `description` must say when to use it (`SST-VAL005`): it is what an agent reads to
pick the view.

A view's own settings go under `config:`: the `semantic_views:` `+` keys without the `+`
(`enabled`, `tags`, `max_staleness`, `database`, `schema`). They win over every folder route
and the block root; an absent key inherits, any other key is `SST-CFG053`, and a key with no
value is `SST-CFG054`. Never write them beside `tables:` (`SST-PRS126`).

## Folder routes and precedence

In `sst_config.yml`, a `+` key sets a block's default and an unprefixed key is a folder route:

```yaml
semantic_views:
  +schema: SEMANTIC
  finance:          # views under <semantic_models_dir>/semantic_views/finance/
    +schema: FINANCE_SEMANTIC
```

Each setting resolves closest first: the item's own `config:`, then the closest folder route
over its file, then the block root, then SST's default. A route that names no folder is
`SST-CFG041`; `skills.+threads` is root-only (`SST-CFG055`). Every artifact block routes the
same way: `tools:`, `agents:`, `evals:`, `skills:`, `plugins:`, `coco_desktop_profiles:`. Prefer a folder
route for what a whole folder shares and `config:` for the one item that differs.
`sst list --long` prints each setting with where it came from.

## Metrics

```yaml
# metrics/total_revenue.yml
metric:
  name: total_revenue
  description: Order value in cents, including tax.
  expr: "SUM({{ ref('orders', 'order_total') }})"
```

```yaml
# metrics/revenue_per_order.yml
metric:
  name: revenue_per_order
  description: Mean revenue per order.
  expr: "DIV0({{ metric('total_revenue') }}, {{ metric('order_count') }})"
```

- Never write `tables:` or `derived:` (`SST-PRS126`). A metric's table is the model its `expr`
  names with `ref()`, and it needs an aggregate `expr` (`SST-VAL101`). A metric whose `expr`
  combines `metric()` calls and names no column is derived.
- Every metric, filter and verified query names a column or table with `ref()`, or it attaches
  to no view (`SST-MEM002`): write `COUNT({{ ref('orders', 'order_id') }})`, never `COUNT(*)`.
- When two join paths connect a metric's table to another, name one with
  `using_relationships: ["{{ relationship('<name>') }}"]` (`SST-VAL116`); a bare name is
  `SST-REF050`.
- Use `non_additive_dimensions:` for a balance that must not be summed over a date, each entry
  `- ref: "{{ ref('<model>', '<column>') }}"` with an optional `sort_direction` and
  `null_order` (never `dimension:` or `table:`, `SST-PRS126`), and a `window:` block for running
  totals; never write `OVER (...)` in `expr`.
- A view's scope lists name each member by its call: `columns:` / `exclude_columns:` by
  `{{ ref('<model>', '<column>') }}`, `metrics:` / `exclude_metrics:` by
  `{{ metric('<name>') }}`, and `relationships:` / `exclude_relationships:` by
  `{{ relationship('<name>') }}`. A bare name is `SST-REF050`.

## Relationships

```yaml
# relationships/orders_to_customers.yml
relationship:
  name: orders_to_customers
  left_table: "{{ ref('orders') }}"      # the many side
  right_table: "{{ ref('customers') }}"  # the one side
  relationship_conditions:
    - "{{ ref('orders', 'customer_id') }} = {{ ref('customers', 'customer_id') }}"
```

- Each endpoint is one `{{ ref('<model>') }}` call (`SST-REF046`).
- `=` is an equality join, `>=` an as-of join, and `BETWEEN` a range join.
- The right table of a `BETWEEN` range join needs `distinct_range` on its model (`SST-VAL206`).
- The right table of an equality join needs a `primary_key` or `unique_keys`
  (`SST-VAL311`). Joining on part of a composite key fans out and inflates every metric
  across the join (`SST-VAL208`); make such a join as-of instead.
- Never write `relationship_type` or `join_type` (`SST-VAL211`).

## Filters, verified queries, custom instructions

- A filter is a boolean `expr` labeled `labels: [filter]`; an unlabeled boolean filter is
  `SST-VAL405`.
- A verified query has `question` and `sql` (`SST-PRS002`). Its SQL names each table with
  `{{ ref('<model>') }}`, as a dbt model does (`SST-VAL414`); those models are its tables. Keep
  long SQL in a file with `sql: "{{ file('sql/<name>.sql') }}"`, relative to the YAML file.
- A custom instruction block uses `ai_sql_generation` and `ai_question_categorization`, and
  a view includes it with `"{{ custom_instructions('<name>') }}"`.

## Tool members beside the views

Tool members are not semantic members, but they follow the same file rules: one `tool:`
mapping per file under `project.tools_dir` (a `tools:` list still reads), at any depth, with no
groups. Their settings come from `tools:` folder routes and the member's own `config:`, and
`meta.owner` names the owner. An agent names a member as `{{ tool('<name>') }}`, one argument
(`SST-REF015`), and a defined search service writes every column as
`{{ ref('<on model>', '<column>') }}` (`SST-VAL622` for another model). A routine's body file
names the tables it reads with `{{ ref() }}` or `{{ source() }}`, as a view does. For the rest,
use sst-agents-and-evals.

## After every edit

```bash
sst format
sst validate --no-snowflake-syntax-check
sst compile --emit-ddl target/ddl
```

Read the DDL for the views you touched: it is exactly what publishes. A full `sst validate`
also has Snowflake compile every expression; run it when the user is ready to connect.
