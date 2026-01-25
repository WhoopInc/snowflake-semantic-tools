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
| `{{ table('name') }}` | Reference a dbt model | `{{ table('orders') }}` |
| `{{ column('table', 'col') }}` | Reference a specific column | `{{ column('orders', 'customer_id') }}` |
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
          cortex_searchable: true                    # Optional: include in Dynamic SV Generation (future feature)
          synonyms:                                  # Optional: alternative names
            - purchases
            - transactions
```

### Table Metadata Fields

| Field | Required | Purpose |
|-------|----------|---------|
| `primary_key` | Yes | Column(s) that uniquely identify each row. Used in semantic view `PRIMARY KEY` clause. |
| `unique_keys` | No | Column(s) forming a unique constraint. **Required for ASOF relationships** - Snowflake needs this to validate temporal joins. |
| `cortex_searchable` | No | If `true`, table is included in Cortex Search for dynamic SV generation. Default: `false` |
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
```

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

# In your relationship definition
snowflake_relationships:
  - name: orders_to_prior_orders
    left_table: {{ table('orders') }}
    right_table: {{ table('orders') }}
    relationship_conditions:
      - "{{ column('orders', 'customer_id') }} = {{ column('orders', 'customer_id') }}"
      - "{{ column('orders', 'ordered_at') }} >= {{ column('orders', 'ordered_at') }}"
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
      - {{ table('table_name') }}
    description: Clear description of what this measures
    expr: SQL expression using aggregate functions
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

### Real Examples

```yaml
snowflake_metrics:
  - name: total_order_count
    tables:
      - {{ table('orders') }}
    description: Total number of orders placed
    expr: |
      COUNT(DISTINCT {{ column('orders', 'order_id') }})
    synonyms:
      - total_orders
      - order_count

  - name: average_rating
    tables:
      - {{ table('product_reviews') }}
    description: Average product rating (1-5 stars) based on customer reviews
    expr: |
      AVG({{ column('product_reviews', 'rating') }})
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

**Not supported:** `<=`, `>`, `<`, `BETWEEN` (Snowflake semantic views only support `=` and `>=`)

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

Reusable WHERE clause conditions.

> **Future Feature:** Filters are extracted to metadata tables and validated, but Snowflake's `CREATE SEMANTIC VIEW` DDL does not yet support filters. Defining them now prepares your semantic layer for when Snowflake adds support. Including filters won't cause errors—they're simply not included in the generated view.

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

Validated example queries that train AI models and provide templates.

> **Future Feature:** Verified queries are extracted to metadata tables and validated, but Snowflake's `CREATE SEMANTIC VIEW` DDL does not yet support verified queries. Defining them now prepares your semantic layer for when Snowflake adds support. Including verified queries won't cause errors—they're simply not included in the generated view.

### Structure

```yaml
snowflake_verified_queries:
  - name: query_name
    tables:
      - {{ table('table_name') }}
    question: Natural language question this query answers
    use_as_onboarding_question: true|false
    sql: Complete SQL query
    verified_by: Person Name
    verified_at: Unix timestamp
```

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
    verified_at: 1728000000
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

**Optional (future-ready):**
```yaml
    custom_instructions:
      - {{ custom_instructions('instruction_name') }}  # Not yet supported by Snowflake
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