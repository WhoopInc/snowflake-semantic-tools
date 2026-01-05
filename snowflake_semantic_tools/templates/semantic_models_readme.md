# Semantic Models Directory

This directory contains semantic model definitions for Snowflake Semantic Views
and Cortex Analyst.

## Directory Structure

```
snowflake_semantic_models/
├── metrics/              # Business metrics (KPIs, calculations)
├── relationships/        # How tables join together
├── filters/              # Reusable WHERE clauses
├── verified_queries/     # Example queries for AI training
├── custom_instructions/  # AI behavior customization
└── semantic_views.yml    # Semantic view definitions
```

## Getting Started

1. **Review the examples**: Each subdirectory contains `_examples.yml` with
   commented-out examples showing proper syntax.

2. **Create your models**: Uncomment and modify the examples, or create new files.

3. **Validate**: Run `sst validate` to check for errors.

4. **Deploy**: Run `sst deploy` to push to Snowflake.

## Documentation

- [Semantic Models Guide](https://github.com/WhoopInc/snowflake-semantic-tools/blob/main/docs/semantic-models-guide.md)
- [CLI Reference](https://github.com/WhoopInc/snowflake-semantic-tools/blob/main/docs/cli-reference.md)
- [Validation Checklist](https://github.com/WhoopInc/snowflake-semantic-tools/blob/main/docs/validation-checklist.md)

## Quick Reference

### Metrics
```yaml
snowflake_metrics:
  - name: total_revenue
    description: Total revenue from all orders
    tables:
      - {{ table('orders') }}
    expr: SUM({{ column('orders', 'amount') }})
```

### Relationships
```yaml
snowflake_relationships:
  - name: orders_to_customers
    left_table: {{ table('orders') }}
    right_table: {{ table('customers') }}
    join_type: left_outer
    relationship_type: many_to_one
    relationship_columns:
      - left_column: {{ column('orders', 'customer_id') }}
        right_column: {{ column('customers', 'id') }}
```

### Semantic Views
```yaml
semantic_views:
  - name: sales_analytics
    description: Sales data with customer context
    tables:
      - {{ table('orders') }}
      - {{ table('customers') }}
```

