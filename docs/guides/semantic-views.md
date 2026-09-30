# Semantic views

A semantic view is a set of dbt models plus the members that describe how to
query them. This guide covers every member type. The complete key list is in
the [configuration reference](../reference/config.md); every rule below has an
entry in the [error code reference](../reference/error-codes.md).

Files live under `project.semantic_models_dir` (default `semantic_models/`) and
can be split and named however you like: SST reads every YAML file there and
recognises members by their root key. Views themselves live under its
`semantic_views/` folder, where folder routes apply; a `semantic_views:` list
anywhere else is not built and is reported (`SST-PRS004`).

## Views

```yaml
semantic_views:
  - name: sales
    description: |-
      Orders, the customers who placed them, and where. Use for order volume,
      order value, and revenue by customer or location.
    tables:
      - "{{ ref('orders') }}"
      - "{{ ref('customers') }}"
      - "{{ ref('locations') }}"
    table_config:
      orders:
        synonyms: [sale, transaction]
    variables:
      - name: large_order_cents
        data_type: NUMBER
        description: Threshold above which an order counts as large.
        default_value: 1000
    custom_instructions:
      - "{{ custom_instructions('sql_conventions') }}"
```

- `tables:` is required and lists models with `{{ ref() }}`. It decides which
  members attach (see [Concepts](../concepts.md#association-how-members-find-views)).
- `description:` is what an agent reads to pick this view; say what it answers
  and what it does not.
- `variables:` declares view variables that expressions use by bare name.
- `enabled: false` keeps a view in the project without publishing it.

## Columns: facts, dimensions, time dimensions

Columns come from dbt. Each model's YAML carries SST metadata under
`config.meta.sst`:

```yaml
models:
  - name: orders
    config:
      meta:
        sst:
          primary_key: [order_id]
    columns:
      - name: order_state
        description: Where the order is in fulfilment.
        config:
          meta:
            sst:
              column_type: dimension
              is_enum: true
              sample_values: [placed, completed, returned]
```

`column_type` is `dimension`, `fact`, or `time_dimension`. `is_enum: true`
declares that `sample_values` is the complete set of values. The model-level
`primary_key` (or `unique_keys`) tells SST which columns identify a row; a
relationship that joins to a model without a key over its join columns is
warned about (`SST-VAL210`).

## Metrics

```yaml
snowflake_metrics:
  - name: total_revenue
    description: Order value in cents, including tax.
    tables: [orders]
    expr: "SUM({{ ref('orders', 'order_total') }})"
    synonyms: [revenue, sales]

  - name: revenue_per_order
    derived: true
    description: Mean revenue per order.
    expr: "DIV0({{ metric('total_revenue') }}, {{ metric('order_count') }})"
```

- `tables:` names the models the expression reads.
- A `derived: true` metric combines other metrics with `{{ metric() }}`; SST
  infers its tables from the metrics it references. It cannot use window
  functions (`SST-VAL102`), and metric references cannot form a cycle.
- When more than one relationship path joins a metric's table to another table
  in the view, name the path with `using_relationships:`. An ambiguous path is
  an error for the metric (`SST-VAL116`) and a warning for the view (`SST-VAL209`).

### Semi-additive metrics

`non_additive_dimensions:` marks a metric that must not be summed across a
dimension, such as a balance across its snapshot date. Snowflake sorts the rows by
the listed dimensions and aggregates only the values in the last rows of that
order:

```yaml
  - name: total_supply_cost
    tables: [supplies]
    expr: "SUM({{ ref('supplies', 'supply_cost') }})"
    non_additive_dimensions:
      - dimension: snapshot_month        # latest month: the default order

  - name: opening_supply_cost
    tables: [supplies]
    expr: "SUM({{ ref('supplies', 'supply_cost') }})"
    non_additive_dimensions:
      - table: supplies
        dimension: snapshot_month
        sort_direction: descending       # earliest month
        null_order: first
```

- `dimension` names a dimension of the metric's table, or of `table:` when it is
  set; a name that is not a dimension there is an error (`SST-VAL118`).
- `sort_direction` is `ascending` (the default, which takes the latest value of a
  date) or `descending` (the earliest). `null_order` is `first` or `last`; left
  unset, Snowflake's default null ordering applies, which sorts nulls last in
  ascending order. Set `null_order: first` when a row with no date must never be
  the one that counts.
- List order matters, as in an `ORDER BY`.
- Only a derived metric may reference a semi-additive metric (`SST-VAL107`).

These render as `NON ADDITIVE BY (SNAPSHOT_MONTH)` and
`NON ADDITIVE BY (SUPPLIES.SNAPSHOT_MONTH DESC NULLS FIRST)`.

### Window function metrics

A `window:` block makes a table-scoped metric a window function metric: a running
total, a moving average, or a value from an earlier row.

```yaml
  - name: cumulative_customer_count
    tables: [customers]
    expr: "SUM({{ metric('customer_count') }})"
    window:
      partition_by_excluding:
        - "{{ ref('customers', 'first_ordered_at') }}"
      order_by:
        - ref: "{{ ref('customers', 'first_ordered_at') }}"
          sort_direction: ascending        # optional
          null_order: last                 # optional
      frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
```

This renders as:

```sql
CUSTOMERS.CUMULATIVE_CUSTOMER_COUNT AS SUM(CUSTOMERS.CUSTOMER_COUNT) OVER (
  PARTITION BY EXCLUDING CUSTOMERS.FIRST_ORDERED_AT
  ORDER BY CUSTOMERS.FIRST_ORDERED_AT ASC NULLS LAST
  RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
```

- `expr` is one function call, and its first argument is a metric of the same
  table (`{{ metric() }}`) or an aggregate such as `SUM(...)`. A window over a raw
  column is a row-level window; it belongs in a fact or dimension, in the dbt
  model (`SST-VAL126`). `LAG({{ metric('m') }}, 1)` and `AVG(...)` work the same
  way as `SUM`.
- `partition_by` groups rows by the listed entries. `partition_by_excluding`
  instead partitions by every dimension a query requests except those listed.
  Set at most one (`SST-PRS014`).
- `order_by` entries take a reference and, optionally, `sort_direction` and
  `null_order`, with the same values as a non-additive dimension. A bare
  reference is shorthand for `ref:`.
- Each entry is `{{ ref('<model>', '<column>') }}`, naming a dimension the
  metric's table reaches through the view's relationships, or
  `{{ metric('<name>') }}`, naming a metric of the same table. EXCLUDING takes
  dimensions only (`SST-VAL125`).
- `frame` is `ROWS` or `RANGE BETWEEN <bound> AND <bound>`, where a bound is
  `UNBOUNDED PRECEDING`, `UNBOUNDED FOLLOWING`, `CURRENT ROW`, or a number or an
  `INTERVAL '<n> <unit>'` followed by `PRECEDING` or `FOLLOWING`. Anything else is
  an error (`SST-PRS124`); a frame is never passed through as free SQL. A frame
  needs an `order_by` (`SST-VAL127`).
- A window metric cannot also set `using_relationships` or
  `non_additive_dimensions`, cannot be `derived: true` (`SST-VAL102`), and no
  other metric may reference it (`SST-VAL128`).
- Writing `OVER (...)` inside `expr` is an error (`SST-VAL101`); use `window:`.

A query that returns a window metric must also return every dimension in its
partition and order, or Snowflake rejects the query. `SHOW SEMANTIC DIMENSIONS IN
<view> FOR METRIC <metric>` marks them `required`.

## Relationships

```yaml
snowflake_relationships:
  - name: orders_to_customers
    description: Each order belongs to exactly one customer.
    left_table: orders
    right_table: customers
    relationship_conditions:
      - "{{ ref('orders', 'customer_id') }} = {{ ref('customers', 'customer_id') }}"
```

`left_table` is the many side and `right_table` the one side, named as plain
model names. Each condition pairs one column from each side: an equality, an
as-of comparison such as `>=`, or a range with `BETWEEN`:

```yaml
    relationship_conditions:
      - "{{ ref('orders', 'ordered_at') }} BETWEEN {{ ref('pricing_periods', 'effective_start_at') }} AND {{ ref('pricing_periods', 'effective_end_at') }}"
```

Both tables must be in a view for the relationship to attach to it.

## Filters

A filter is a named boolean expression. Label it so it renders as a native
filter dimension (`LABELS = (FILTER)` in the view's DDL):

```yaml
snowflake_filters:
  - name: is_completed_order
    description: Orders that completed; the default lens for revenue.
    tables: ["{{ ref('orders') }}"]
    expr: "{{ ref('orders', 'order_state') }} = 'completed'"
    labels: [filter]
```

A boolean expression without a `labels:` key is an error (`SST-VAL405`), and a
filter labelled `filter` must be boolean (`SST-VAL401`). An entry without
`labels:` whose expression is not boolean renders as an ordinary dimension.

## Verified queries

```yaml
snowflake_verified_queries:
  - name: order_count_by_state
    description: Order volume by fulfilment state.
    tables: [orders]
    question: How many orders are there in each fulfilment state?
    sql: |-
      SELECT orders.order_state, COUNT(DISTINCT orders.order_id) AS order_count
      FROM orders AS orders
      GROUP BY ALL
    use_as_onboarding_question: true
    verified_at: 1769904000
    verified_by: analytics-team
```

Use `sql_file:` instead of `sql:` to keep the query in a `.sql` file beside the
YAML; setting both is an error. The SQL names tables by their logical names in
the view, not by relation.

## Custom instructions

```yaml
snowflake_custom_instructions:
  - name: sql_conventions
    description: How to shape SQL against these views.
    ai_sql_generation: |-
      Monetary columns are stored in cents; divide by one hundred before
      presenting a currency figure.
    ai_question_categorization: |-
      Decline questions about individual named customers.
```

A view includes a block by listing `{{ custom_instructions('<name>') }}`. The 0.3
spellings `sql_generation` and `question_categorization` are still read, with a
deprecation warning (`SST-PRS020`); setting both spellings on one block is an
error.

## Checking the rendered view

```bash
sst compile --emit-ddl target/ddl/
```

writes each view's `CREATE SEMANTIC VIEW` statement to its own file, offline.
Diffing that directory between two commits shows exactly what a change does to
the published DDL. `sst test --suite golden` compares the rendered DDL with
files committed under `--golden-dir`.
