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

Your semantic models live in `snowflake_semantic_models/`:

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

Files are discovered recursively - organize however makes sense for your domain.

## dbt Model Configuration

Your dbt model YAML files need SST metadata in the `meta.sst` section.

### Table-Level Metadata

```yaml
models:
  - name: orders
    description: Order fact table with one row per order
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
    expr: COUNT(*)

  - name: total_revenue
    tables:
      - {{ table('orders') }}
    expr: SUM({{ column('orders', 'amount') }})

  - name: average_order_value
    tables:
      - {{ table('orders') }}
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

For time-series data, use >= or <= operators:

```yaml
snowflake_relationships:
  - name: events_to_sessions
    left_table: {{ table('user_events') }}
    right_table: {{ table('user_sessions') }}
    relationship_conditions:
      - "{{ column('user_events', 'session_id') }} = {{ column('user_sessions', 'session_id') }}"
      - "{{ column('user_events', 'event_time') }} >= {{ column('user_sessions', 'start_time') }}"
      - "{{ column('user_events', 'event_time') }} <= {{ column('user_sessions', 'end_time') }}"
```

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

Guide Cortex Analyst's behavior with business-specific rules.

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

Combine metrics, relationships, and filters into queryable views.

### Structure

```yaml
semantic_views:
  - name: view_name
    description: What this view provides
    tables:
      - {{ table('table1') }}
      - {{ table('table2') }}
    custom_instructions:
      - {{ custom_instructions('instruction_name') }}
```

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
    custom_instructions:
      - {{ custom_instructions('customer_privacy_rules') }}
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
  expr: "SUM(a)"

# Good - descriptive and clear
- name: total_subscription_revenue
  expr: "SUM({{ column('subscriptions', 'monthly_amount') }})"
```

### 2. Always Add Descriptions

```yaml
- name: customer_lifetime_value
  description: |
    Total revenue generated by a customer across all time,
    including subscriptions, one-time purchases, and add-ons.
    Excludes refunds and chargebacks.
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
# Define base metrics first
- name: gross_revenue
  expr: SUM({{ column('orders', 'amount') }})

- name: refunds
  expr: SUM({{ column('refunds', 'amount') }})

# Then compose complex metrics
- name: net_revenue
  expr: {{ metric('gross_revenue') }} - {{ metric('refunds') }}
```

### 5. Provide Synonyms for Natural Language

```yaml
- name: average_rating
  synonyms:
    - avg_rating
    - mean_rating
    - customer_rating
```

## Next Steps

- [CLI Reference](cli-reference.md) - Complete command documentation
- [Deployment Guide](cli-reference.md) - CI/CD integration
- [Validation Guide](user-guide.md) - Deep dive into validation rules