# Semantic Models Guide

Complete reference for creating semantic models with Snowflake Semantic Tools.

## What Are Semantic Models?

Semantic models provide a business-friendly layer on top of your dbt models, defining:

- **Metrics**: Aggregated business calculations (revenue, counts, averages)
- **Relationships**: How tables join together via foreign keys
- **Filters**: Reusable WHERE clause conditions
- **Custom Instructions**: Business rules for Cortex Analyst
- **Verified Queries**: Validated example queries for AI training
- **Semantic Views**: Curated combinations of the above

## Template System

All semantic models use templates that reference your dbt models:

| Template | Purpose | Example |
|----------|---------|---------|
| `{{ ref('name') }}` | Reference a dbt model (unified syntax - recommended) | `{{ ref('orders') }}` |
| `{{ ref('table', 'col') }}` | Reference a specific column (unified syntax - recommended) | `{{ ref('orders', 'customer_id') }}` |
| `{{ table('name') }}` | Reference a dbt model (legacy - still supported) | `{{ table('orders') }}` |
| `{{ column('table', 'col') }}` | Reference a specific column (legacy - still supported) | `{{ column('orders', 'customer_id') }}` |
| `{{ metric('name') }}` | Reference another metric | `{{ metric('total_revenue') }}` |
| `{{ custom_instructions('name') }}` | Apply business rules | `{{ custom_instructions('exclude_test_users') }}` |

**Why Templates?**
- **Validated**: References are checked against your dbt catalog
- **Portable**: Work across environments (dev/staging/prod)
- **Maintainable**: Updates automatically when dbt models change

## Directory Structure

Your semantic models live in `snowflake_semantic_models/` by default:

```
snowflake_semantic_models/
├── metrics/
│   ├── sales.yml
│   ├── customers.yml
│   └── finance.yml
├── relationships/
│   ├── core.yml
│   └── sales.yml
├── filters/
│   └── filters.yml
├── custom_instructions/
│   └── custom_instructions.yml
├── verified_queries/
│   └── verified_queries.yml
└── semantic_views.yml
```

> **Note:** The directory path is configurable via `project.semantic_models_dir` in `sst_config.yml`.

Files are discovered recursively - organize however makes sense for your domain.

## dbt Model Configuration

Your dbt model YAML files need SST metadata in the `config.meta.sst` section. This is the dbt Fusion-compatible format.

> **Note:** If you have existing models using the legacy `meta.sst` format, run `sst migrate-meta` to convert them. See the [dbt Fusion Migration Guide](../guides/dbt-fusion-migration.md).

### Table-Level Metadata

```yaml
models:
  - name: orders
    description: Order fact table with one row per order
    config:
      meta:
        sst:
          primary_key: order_id                      # Required: unique identifier
          unique_keys: [customer_id, ordered_at]     # Optional: for ASOF relationships
          synonyms:                                  # Optional: alternative names
            - purchases
            - transactions
```

### Constraints (DISTINCT RANGE)

For range-based relationships (e.g., date ranges that don't overlap), declare constraints at the table level:

```yaml
models:
  - name: exchange_rates
    description: Exchange rate periods with non-overlapping date ranges
    config:
      meta:
        sst:
          primary_key: rate_id
          constraints:
            - type: distinct_range
              name: rate_date_range
              start_column: effective_start
              end_column: effective_end
```

This generates:
```sql
CONSTRAINT RATE_DATE_RANGE DISTINCT RANGE
    BETWEEN EFFECTIVE_START AND EFFECTIVE_END EXCLUSIVE
```

### Tags

Apply Snowflake governance tags at the table or column level:

```yaml
models:
  - name: customers
    config:
      meta:
        sst:
          primary_key: customer_id
          tags:
            GOVERNANCE.SCHEMA.PII_LEVEL: "high"
            GOVERNANCE.SCHEMA.DATA_DOMAIN: "customer"
    columns:
      - name: email
        config:
          meta:
            sst:
              column_type: dimension
              data_type: TEXT
              tags:
                GOVERNANCE.SCHEMA.PII_TYPE: "email"
```

> **Note:** Tag names should be fully-qualified Snowflake tag objects (`DB.SCHEMA.TAG_NAME`). Unqualified names produce a validation warning.

### Table Metadata Fields

| Field | Required | Purpose |
|-------|----------|---------|
| `primary_key` | Yes | Column(s) that uniquely identify each row. Used in semantic view `PRIMARY KEY` clause. |
| `unique_keys` | No | Column(s) forming a unique constraint. **Required for ASOF relationships** - Snowflake needs this to validate temporal joins. |
| `constraints` | No | List of constraint objects. Currently supports `distinct_range` type for non-overlapping range declarations. |
| `tags` | No | Dict of `tag_name: tag_value` pairs. Tag names should be fully-qualified (e.g., `DB.SCHEMA.TAG_NAME`). |
| `synonyms` | No | Alternative names users might use to refer to this table |

### Column-Level Metadata

```yaml
    columns:
      - name: order_id
        description: Unique order identifier
        config:
          meta:
            sst:
              column_type: dimension    # Required: dimension, fact, or time_dimension
              data_type: TEXT           # Required: Snowflake data type
              synonyms: []              # Optional: alternative column names
              sample_values:            # Optional: example values for AI context
                - "ORD-001"
                - "ORD-002"
              is_enum: false            # Optional: true if sample_values is exhaustive
              exclude: false           # Optional: true to hide column from semantic view
```

### Column Exclusion

Add `exclude: true` to any column's `config.meta.sst` to hide it from the semantic view entirely:

```yaml
columns:
  - name: email_address
    description: Customer email (PII)
    config:
      meta:
        sst:
          exclude: true   # Column will not appear in the semantic view

  - name: customer_id
    config:
      meta:
        sst:
          column_type: dimension
          data_type: TEXT
```

Excluded columns are:
- **Skipped during enrichment** — no sample values, synonyms, or type detection
- **Omitted from extraction** — not written to SM_DIMENSIONS/SM_FACTS/SM_TIME_DIMENSIONS
- **Absent from generated DDL** — do not appear in DIMENSIONS or FACTS clauses
- **Skipped in validation** — no `column_type` requirement

Use cases:
- PII columns on joined tables that shouldn't be queryable via Cortex Agents
- Internal/technical columns (ETL timestamps, hash keys, debug flags)
- Columns that exist in the warehouse but aren't relevant to the semantic layer

### Derived Metrics

Derived metrics combine metrics from multiple entities without belonging to a specific table:

```yaml
snowflake_metrics:
  - name: revenue_to_customer_ratio
    derived: true
    description: Revenue divided by customer count
    expr: "{{ metric('total_revenue') }} / NULLIF({{ metric('total_customers') }}, 0)"
```

Derived metrics:
- Use `{{ metric('name') }}` references (resolved to `TABLE.METRIC_NAME` in DDL)
- Are emitted without a table prefix in the semantic view
- Cannot use `using_relationships`, `non_additive_by`, or `window`
- Don't require a `tables` field (inferred from referenced metrics)

### Auto-Inferred Tables

The `tables` field is optional. SST auto-infers table references from the expression:

```yaml
snowflake_metrics:
  # tables auto-inferred from expr → ['orders']
  - name: total_revenue
    expr: "SUM({{ ref('orders', 'order_total') }})"
```

Inference strategies (in priority order):
1. `{{ ref('table', 'col') }}` and `{{ column('table', 'col') }}` patterns
2. `TABLE.COLUMN` dot-references (post-template-resolution)
3. For derived metrics: resolved transitively from referenced metrics

Explicit `tables` always takes precedence when provided.

### UNIQUE Keys for ASOF Relationships

When defining ASOF (temporal) relationships, Snowflake requires a `UNIQUE` constraint on the columns involved in the join:

```yaml
# In your dbt model YAML
models:
  - name: orders
    config:
      meta:
        sst:
          primary_key: order_id
          unique_keys: [customer_id, ordered_at]  # Columns used in ASOF join

# In your relationship definition (using unified ref() syntax)
snowflake_relationships:
  - name: orders_to_prior_orders
    left_table: {{ ref('orders') }}
    right_table: {{ ref('orders') }}
    relationship_conditions:
      - "{{ ref('orders', 'customer_id') }} = {{ ref('orders', 'customer_id') }}"
      - "{{ ref('orders', 'ordered_at') }} >= {{ ref('orders', 'ordered_at') }}"
```

This generates:
```sql
ORDERS AS SCHEMA.ORDERS
  PRIMARY KEY (ORDER_ID)
  UNIQUE (CUSTOMER_ID, ORDERED_AT)
```

## Metrics

Metrics define aggregated business calculations.

### Basic Structure

```yaml
snowflake_metrics:
  - name: metric_name
    tables:
      - {{ ref('table_name') }}  # Unified syntax (legacy {{ table('table_name') }} also works)
    description: Clear description of what this measures
    expr: SQL expression using aggregate functions (e.g., SUM({{ ref('table_name', 'column_name') }}))
    synonyms:
      - alternative_name_1
      - alternative_name_2
    sample_values:
      - 12345.67
      - 23456.78
```

### Required Properties

- **`name`**: Unique identifier (becomes uppercase in Snowflake)
- **`tables`**: List of dbt tables this metric references
- **`expr`**: SQL expression with aggregate function
- **`description`**: Business explanation

### Optional Properties

- **`synonyms`**: Alternative names users might use
- **`sample_values`**: Example values for AI context
- **`visibility`**: `"private"` or `"public"` (default). Private metrics are hidden from end users in Cortex Analyst.
- **`non_additive_by`**: Semi-additive configuration for measures that cannot be summed across time (e.g., balances, inventory, headcount). See [Semi-Additive Metrics](#semi-additive-metrics-non-additive-by).
- **`using_relationships`**: List of relationship names for join path disambiguation when multiple relationships exist between tables. See [Join Path Disambiguation](#join-path-disambiguation-using).
- **`window`**: Window function configuration for running totals, cumulative sums, rankings, and moving averages. See [Window Function Metrics](#window-function-metrics).
- **`tags`**: Dict of `tag_name: tag_value` pairs. Tag names should be fully-qualified Snowflake tag objects (e.g., `DB.SCHEMA.TAG_NAME`).

### Semi-Additive Metrics (NON ADDITIVE BY)

For measures that cannot be summed across time dimensions (e.g., account balances, inventory levels):

```yaml
snowflake_metrics:
  - name: current_balance
    tables:
      - {{ ref('account_snapshots') }}
    description: Current account balance (semi-additive — use latest value, not sum)
    expr: MAX({{ ref('account_snapshots', 'balance') }})
    non_additive_by:
      - dimension: snapshot_date
        order: DESC
        nulls: LAST
```

**Fields within `non_additive_by`:**

| Field | Required | Values | Description |
|-------|----------|--------|-------------|
| `dimension` | Yes | Column name | The time dimension this metric is non-additive over |
| `order` | No | `ASC` / `DESC` | Sort direction (default: ASC) |
| `nulls` | No | `FIRST` / `LAST` | NULL sort position |

This generates:
```
ACCOUNT_SNAPSHOTS.CURRENT_BALANCE
      NON ADDITIVE BY (SNAPSHOT_DATE DESC NULLS LAST) AS MAX(ACCOUNT_SNAPSHOTS.BALANCE)
```

### Join Path Disambiguation (USING)

When multiple relationships exist between the same tables, use `using_relationships` to specify which join path a metric should use:

```yaml
snowflake_metrics:
  - name: shipping_revenue
    tables:
      - {{ ref('orders') }}
    description: Revenue attributed via shipping relationship
    expr: SUM({{ ref('orders', 'shipping_amount') }})
    using_relationships:
      - orders_to_shipping_address
```

This generates:
```
ORDERS.SHIPPING_REVENUE
      USING (ORDERS_TO_SHIPPING_ADDRESS) AS SUM(ORDERS.SHIPPING_AMOUNT)
```

### Window Function Metrics

For running totals, cumulative sums, rankings, and moving averages:

```yaml
snowflake_metrics:
  - name: running_total_revenue
    tables:
      - {{ ref('orders') }}
    description: Cumulative revenue partitioned by region
    expr: SUM({{ ref('orders', 'amount') }})
    window:
      partition_by:
        - {{ ref('orders', 'region') }}
      order_by:
        - column: {{ ref('orders', 'order_date') }}
          direction: ASC
```

**Window configuration fields:**

| Field | Description |
|-------|-------------|
| `partition_by` | List of columns to partition by |
| `partition_by_excluding` | List of columns to exclude from partitioning (Snowflake-specific, mutually exclusive with `partition_by`) |
| `order_by` | List of order specifications (objects with `column` + `direction`, or bare column strings) |

This generates:
```
ORDERS.RUNNING_TOTAL_REVENUE AS SUM(ORDERS.AMOUNT) OVER (
        PARTITION BY ORDERS.REGION ORDER BY ORDERS.ORDER_DATE ASC
    )
```

### Visibility (PRIVATE / PUBLIC)

Mark metrics or facts as private to hide them from end users while keeping them available for metric composition:

```yaml
snowflake_metrics:
  - name: internal_discount_rate
    tables:
      - {{ ref('pricing') }}
    description: Internal discount calculation — not for external use
    expr: SUM({{ ref('pricing', 'internal_rate') }})
    visibility: private
```

> **Note:** Only facts and metrics support visibility. Dimensions and time dimensions cannot be marked private (Snowflake restriction).

### Real Examples

```yaml
snowflake_metrics:
  - name: total_order_count
    tables:
      - {{ ref('orders') }}  # Unified syntax (legacy {{ table('orders') }} also works)
    description: Total number of orders placed
    expr: |
      COUNT(DISTINCT {{ ref('orders', 'order_id') }})  # Unified syntax (legacy {{ column('orders', 'order_id') }} also works)
    synonyms:
      - total_orders
      - order_count

  - name: average_rating
    tables:
      - {{ ref('product_reviews') }}
    description: Average product rating (1-5 stars) based on customer reviews
    expr: |
      AVG({{ ref('product_reviews', 'rating') }})
    synonyms:
      - avg_rating
      - mean_rating

  - name: conversion_rate
    tables:
      - {{ table('user_funnel') }}
    description: Conversion rate from visitors to customers
    expr: |
      {{ metric('completed_purchases') }} / 
      NULLIF({{ metric('total_visitors') }}, 0)
    synonyms:
      - purchase_conversion_rate
```

### Metric Composition

Reference other metrics in expressions:

```yaml
snowflake_metrics:
  - name: total_orders
    tables:
      - {{ table('orders') }}
    description: Total number of orders
    expr: COUNT(*)

  - name: total_revenue
    tables:
      - {{ table('orders') }}
    description: Sum of all order amounts
    expr: SUM({{ column('orders', 'amount') }})

  - name: average_order_value
    tables:
      - {{ table('orders') }}
    description: Average revenue per order
    expr: {{ metric('total_revenue') }} / NULLIF({{ metric('total_orders') }}, 0)
```

## Relationships

Define how tables join together.

### Structure

```yaml
snowflake_relationships:
  - name: descriptive_relationship_name
    left_table: {{ table('source_table') }}
    right_table: {{ table('target_table') }}
    relationship_conditions:
      - "{{ column('source_table', 'join_key') }} = {{ column('target_table', 'join_key') }}"
```

### Required Properties

- **`name`**: Descriptive relationship identifier
- **`left_table`**: Source table template (contains foreign key)
- **`right_table`**: Target table template (contains primary key)
- **`relationship_conditions`**: Array of join conditions with operators

**Important:** Each condition **must be quoted** (double quotes) because `{{` has special meaning in YAML. Without quotes, YAML will fail to parse the file.

### Multi-Column Joins

```yaml
snowflake_relationships:
  - name: orders_to_customers_by_date
    left_table: {{ table('orders') }}
    right_table: {{ table('customers') }}
    relationship_conditions:
      - "{{ column('orders', 'customer_id') }} = {{ column('customers', 'customer_id') }}"
      - "{{ column('orders', 'order_date') }} = {{ column('customers', 'registration_date') }}"
```

### ASOF Joins (Time-Series)

For time-series data, use the `>=` operator for ASOF joins:

```yaml
snowflake_relationships:
  - name: events_to_sessions
    left_table: {{ table('user_events') }}
    right_table: {{ table('user_sessions') }}
    relationship_conditions:
      - "{{ column('user_events', 'session_id') }} = {{ column('user_sessions', 'session_id') }}"
      - "{{ column('user_events', 'event_time') }} >= {{ column('user_sessions', 'start_time') }}"
```

**Supported operators:**
- `=` - Equality joins
- `>=` - ASOF (temporal) joins
- `BETWEEN...AND...EXCLUSIVE` - Range joins (requires CONSTRAINT DISTINCT RANGE on the right table)

### Range Joins (BETWEEN...AND...EXCLUSIVE)

For range-based relationships where a value falls within a non-overlapping range (e.g., mapping a transaction date to an exchange rate period):

```yaml
snowflake_relationships:
  - name: orders_to_rates
    left_table: {{ ref('orders') }}
    right_table: {{ ref('exchange_rates') }}
    relationship_conditions:
      - "{{ ref('orders', 'ordered_at') }} BETWEEN {{ ref('exchange_rates', 'effective_start') }} AND {{ ref('exchange_rates', 'effective_end') }} EXCLUSIVE"
```

This generates:
```sql
ORDERS (ORDERED_AT) REFERENCES EXCHANGE_RATES (BETWEEN EFFECTIVE_START AND EFFECTIVE_END EXCLUSIVE)
```

> **Prerequisite:** The right table must have a `CONSTRAINT DISTINCT RANGE` declared (see [Constraints](#constraints-distinct-range)) to guarantee non-overlapping ranges.

### Real Example

```yaml
snowflake_relationships:
  - name: customers_to_orders
    left_table: {{ table('customers') }}
    right_table: {{ table('orders') }}
    relationship_conditions:
      - "{{ column('customers', 'customer_id') }} = {{ column('orders', 'customer_id') }}"
```

## Filters

Reusable WHERE clause conditions that are automatically converted to AI_SQL_GENERATION instructions in the generated semantic view DDL.

> **How it works:** Snowflake's `CREATE SEMANTIC VIEW` DDL has no native `FILTERS` clause. SST converts your filter definitions into natural language instructions appended to the `AI_SQL_GENERATION` clause, guiding Cortex Analyst to apply them by default unless the user explicitly requests unfiltered data. This behavior is controlled by the `generation.filters_to_instructions` config option (default: `true`).

### Structure

```yaml
snowflake_filters:
  - name: filter_name
    tables:
      - {{ table('table_name') }}
    description: What this filter does
    expr: SQL boolean expression
    synonyms:
      - alternative_name
```

### Real Example

```yaml
snowflake_filters:
  - name: active_customers_only
    tables:
      - {{ table('customers') }}
    synonyms:
      - active_customers
      - exclude_inactive
    description: Filters to show only active customers (excludes churned and suspended)
    expr: |
      {{ column('customers', 'status') }} = 'Active'
      AND {{ column('customers', 'account_type') }} != 'Test'
```

## Custom Instructions

Guide Cortex Analyst's behavior with business-specific rules. Custom instructions are automatically included in the generated `CREATE SEMANTIC VIEW` DDL as `AI_SQL_GENERATION` and `AI_QUESTION_CATEGORIZATION` clauses.

When you reference custom instructions in a semantic view using `{{ custom_instructions('name') }}`, the instruction's `sql_generation` field is included in the `AI_SQL_GENERATION` clause and the `question_categorization` field is included in the `AI_QUESTION_CATEGORIZATION` clause.

### Structure

```yaml
snowflake_custom_instructions:
  - name: instruction_name
    question_categorization: Instructions for categorizing questions
    sql_generation: Instructions for generating SQL queries
```

### Real Example

```yaml
snowflake_custom_instructions: 
  - name: customer_privacy_rules
    question_categorization: >
      Reject all questions asking about individual customer details. Ask users to contact their admin.
    sql_generation: >
      When querying customer data, always apply the active_customers_only filter.
      Always round currency metrics to 2 decimal places.
```

## Verified Queries

Validated example queries that train Cortex Analyst and provide onboarding templates. SST emits these as `AI_VERIFIED_QUERIES` clauses in the generated semantic view DDL.

### Structure

```yaml
snowflake_verified_queries:
  - name: query_name
    tables:
      - {{ ref('table_name') }}
    question: Natural language question this query answers
    use_as_onboarding_question: true  # Optional: show in onboarding UI
    sql: |                              # Inline SQL (use this OR sql_file)
      SELECT
        table_name.column,
        COUNT(*) AS total
      FROM {{ ref('table_name') }}
      GROUP BY 1
    verified_by: Data Team
    verified_at: "2025-06-15"           # ISO date (YYYY-MM-DD) — converted to epoch in DDL
```

For longer queries, reference an external `.sql` file instead of inline SQL:

```yaml
snowflake_verified_queries:
  - name: query_name
    tables:
      - {{ ref('table_name') }}
    question: Natural language question this query answers
    sql_file: sql/query_name.sql        # Path relative to the YAML file
    verified_by: Data Team
    verified_at: "2025-06-15"
```

> **Note:** Exactly one of `sql` or `sql_file` is required — specifying both is a validation error. SQL supports `{{ ref('table') }}` templates for table name resolution.

### Real Example

```yaml
snowflake_verified_queries:
  - name: monthly_revenue_by_region
    tables: 
      - {{ table('orders') }}
    question: What is the total revenue by region for each month?
    use_as_onboarding_question: true
    sql: |
      SELECT 
        DATE_TRUNC('MONTH', order_date) AS month,
        region,
        SUM(order_amount) AS total_revenue,
        COUNT(DISTINCT order_id) AS order_count
      FROM orders
      WHERE order_status = 'Completed'
        AND order_date >= DATEADD('year', -1, CURRENT_DATE())
      GROUP BY 1, 2
      ORDER BY 1 DESC, 3 DESC
    verified_by: Data Team
    verified_at: "2024-10-04"
```

## Semantic Views

Combine tables, metrics, and relationships into queryable Snowflake semantic views.

### Structure

```yaml
semantic_views:
  - name: view_name
    description: What this view provides
    tables:
      - {{ table('table1') }}
      - {{ table('table2') }}
```

**Optional:**
```yaml
    custom_instructions:
      - {{ custom_instructions('instruction_name') }}
```

### Cortex Analyst Integration

When SST generates semantic views, it automatically includes a `WITH EXTENSION (CA='...')` clause containing `sample_values` metadata from your dbt models. This helps Cortex Analyst:

- **Understand valid values**: Know what categorical values exist (e.g., `["new", "returning"]` for customer_type)
- **Recognize enums**: When `is_enum: true`, Cortex knows the sample_values are exhaustive
- **Generate better queries**: More accurate SQL generation with proper value references

The CA extension JSON structure:
```json
{
  "tables": [{
    "name": "CUSTOMERS",
    "dimensions": [
      {"name": "CUSTOMER_TYPE", "sample_values": ["new", "returning"], "is_enum": true}
    ],
    "time_dimensions": [
      {"name": "CREATED_AT", "sample_values": ["2025-01-15", "2025-02-20"]}
    ],
    "facts": [
      {"name": "LIFETIME_SPEND", "sample_values": ["100.50", "250.00"]}
    ]
  }]
}
```

**Notes:**
- Only columns with non-empty `sample_values` are included. If no sample_values exist, the CA extension is omitted.
- The CA extension is a temporary solution recommended by Snowflake Engineering until native `sample_values` support is added to the `CREATE SEMANTIC VIEW` DDL syntax. Once Snowflake releases official support, SST will be updated to use the native syntax.

### Real Examples

```yaml
semantic_views:
  - name: customer_orders_view
    description: Combines customer and order data for comprehensive sales analysis
    tables:
      - {{ table('customers') }}
      - {{ table('orders') }}

  - name: product_sales_view
    description: Product sales data with category and region breakdowns
    tables:
      - {{ table('products') }}
      - {{ table('order_items') }}
```

## Validation

Validate your models before deployment:

```bash
# Validate all models
sst validate --verbose

# Validation checks:
- All templates resolved
- All table references exist in dbt
- All column references exist
- No circular dependencies in metrics
- No duplicate names
```

### Common Validation Errors

**Template Resolution Error**:
```
ERROR: Template {{ table('user_orders') }} could not be resolved
Available tables: users_orders, customer_orders
```

**Missing Column**:
```
ERROR: Column 'total' not found in table 'orders'
Available columns: amount, quantity, tax, total_amount
```

**Circular Dependency**:
```
ERROR: Circular dependency in metrics:
metric_a → metric_b → metric_c → metric_a
```

## Best Practices

### 1. Use Clear, Descriptive Names

```yaml
# Bad - unclear abbreviations
- name: m1
  expr: SUM(a)

# Good - descriptive and clear
- name: total_subscription_revenue
  tables:
    - {{ table('subscriptions') }}
  description: Total monthly subscription revenue
  expr: SUM({{ column('subscriptions', 'monthly_amount') }})
```

### 2. Always Add Descriptions

```yaml
- name: customer_lifetime_value
  tables:
    - {{ table('customers') }}
  description: |
    Total revenue generated by a customer across all time,
    including subscriptions, one-time purchases, and add-ons.
    Excludes refunds and chargebacks.
  expr: SUM({{ column('customers', 'lifetime_spend') }})
```

### 3. Group Related Metrics by Domain

```
metrics/
├── customers.yml     # Customer-related metrics
├── sales.yml         # Product sales metrics  
└── finance.yml       # Financial metrics
```

### 4. Use Metric Composition

```yaml
snowflake_metrics:
  # Define base metrics first
  - name: gross_revenue
    tables:
      - {{ table('orders') }}
    description: Total revenue before refunds
    expr: SUM({{ column('orders', 'amount') }})

  - name: refunds
    tables:
      - {{ table('refunds') }}
    description: Total refund amount
    expr: SUM({{ column('refunds', 'amount') }})

  # Then compose complex metrics
  - name: net_revenue
    tables:
      - {{ table('orders') }}
      - {{ table('refunds') }}
    description: Revenue after refunds
    expr: {{ metric('gross_revenue') }} - {{ metric('refunds') }}
```

### 5. Provide Synonyms for Natural Language

```yaml
- name: average_rating
  tables:
    - {{ table('reviews') }}
  description: Average customer rating (1-5 stars)
  expr: AVG({{ column('reviews', 'rating') }})
  synonyms:
    - avg_rating
    - mean_rating
    - customer_rating
```

## Next Steps

- [CLI Reference](../cli/index.md) - Complete command documentation
- [CI/CD Guide](../guides/ci-cd.md) - CI/CD integration
- [Validation Rules](validation-rules.md) - All validation rules