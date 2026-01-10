# Snowflake Semantic Tools (SST) Documentation Guide

This file helps Cursor understand how to work with Snowflake Semantic Tools in dbt projects.

---

## What is SST?

Snowflake Semantic Tools (SST) is a CLI and Python library for building Snowflake Semantic Views from dbt projects. It provides:

- **Metadata enrichment** - Auto-populate dbt YAML with column types, sample values, synonyms
- **Validation** - 99+ checks for semantic model correctness
- **Deployment** - Generate native Snowflake SEMANTIC VIEW objects

---

## Documentation Structure

SST documentation is organized as follows:

```
docs/
├── index.md                 # Master navigation hub
├── getting-started.md       # Installation and first steps
├── cli/
│   ├── index.md            # CLI overview and workflows
│   ├── init.md             # sst init command
│   ├── debug.md            # sst debug command
│   ├── enrich.md           # sst enrich command
│   ├── validate.md         # sst validate command
│   ├── format.md           # sst format command
│   ├── extract.md          # sst extract command
│   ├── generate.md         # sst generate command
│   ├── deploy.md           # sst deploy command
│   └── migrate-meta.md     # sst migrate-meta command
├── concepts/
│   ├── semantic-models.md  # Metrics, relationships, filters
│   └── validation-rules.md # Complete validation checklist
├── guides/
│   ├── authentication.md   # Snowflake connection setup
│   ├── ci-cd.md           # Pipeline integration
│   └── dbt-fusion-migration.md
└── reference/
    ├── config.md           # sst_config.yaml reference
    ├── api.md              # Python API
    └── quick-reference.md  # CLI cheat sheet
```

---

## Key Files in dbt Projects Using SST

When working with a dbt project that uses SST, look for:

| File | Purpose |
|------|---------|
| `sst_config.yaml` | SST configuration (project root) |
| `snowflake_semantic_models/` | Semantic model definitions |
| `target/manifest.json` | dbt manifest (needed for `--models` flag) |
| `~/.dbt/profiles.yml` | Snowflake credentials |

---

## Common SST Tasks

### Enriching Models

```bash
# Enrich specific models
sst enrich --models model1,model2

# With synonyms
sst enrich --models model1 --synonyms

# All components
sst enrich --models model1 --all
```

### Validating

```bash
# Basic validation
sst validate

# With SQL syntax checking
sst validate --snowflake-syntax-check
```

### Deploying

```bash
# One-step deployment
sst deploy --target prod

# Or step-by-step
sst validate
sst extract --target prod
sst generate --target prod --all
```

---

## SST YAML Metadata Format

SST stores metadata in dbt YAML files using `config.meta.sst`:

```yaml
models:
  - name: customers
    description: Customer dimension table
    config:
      meta:
        sst:
          primary_key: customer_id
          synonyms:
            - clients
            - users
    columns:
      - name: customer_id
        description: Unique customer identifier
        config:
          meta:
            sst:
              column_type: dimension
              data_type: TEXT
              synonyms: []
              sample_values:
                - "CUST-001"
                - "CUST-002"
              is_enum: false
```

### Column Types

- `dimension` - Categorical/grouping columns
- `fact` - Numeric measures
- `time_dimension` - Date/timestamp columns

---

## Semantic Model Definitions

Semantic models are defined in `snowflake_semantic_models/`:

### Metrics

```yaml
# metrics/sales.yml
snowflake_metrics:
  - name: total_revenue
    tables:
      - {{ table('orders') }}
    description: Total revenue from all orders
    expr: SUM({{ column('orders', 'amount') }})
```

### Relationships

```yaml
# relationships/core.yml
snowflake_relationships:
  - name: orders_to_customers
    left_table: {{ table('orders') }}
    right_table: {{ table('customers') }}
    relationship_conditions:
      - "{{ column('orders', 'customer_id') }} = {{ column('customers', 'customer_id') }}"
```

### Semantic Views

```yaml
# semantic_views.yml
semantic_views:
  - name: sales_analytics
    description: Sales data with customer context
    tables:
      - {{ table('orders') }}
      - {{ table('customers') }}
```

---

## Template System

SST uses templates that reference dbt models:

| Template | Purpose |
|----------|---------|
| `{{ table('name') }}` | Reference a dbt model |
| `{{ column('table', 'col') }}` | Reference a specific column |
| `{{ metric('name') }}` | Reference another metric |
| `{{ custom_instructions('name') }}` | Apply business rules |

Templates are validated against the dbt catalog.

---

## Configuration Reference

`sst_config.yaml` in project root:

```yaml
project:
  semantic_models_dir: "snowflake_semantic_models"
  dbt_models_dir: "models"

validation:
  exclude_dirs:
    - "_intermediate"
    - "staging"
  strict: false
  snowflake_syntax_check: true

enrichment:
  distinct_limit: 25
  sample_values_display_limit: 10
  synonym_model: 'mistral-large2'
  synonym_max_count: 4

defer:
  target: prod
  state_path: ./prod_run_artifacts
```

---

## When to Use SST Commands

| Situation | Command |
|-----------|---------|
| First time setup | `sst init` |
| Adding new models | `sst enrich --models new_model` |
| Before committing | `sst validate` |
| Deploying to prod | `sst deploy --target prod` |
| Formatting YAML | `sst format models/` |
| Testing connection | `sst debug --test-connection` |

---

## Troubleshooting Tips

### manifest.json not found

```bash
dbt compile --target prod
```

### Validation errors

Check the error message for "Did you mean?" suggestions.

### Model unavailable (synonyms)

Use a universally available model:

```yaml
enrichment:
  synonym_model: 'mistral-large2'
```

---

## External Resources

- [SST GitHub Repository](https://github.com/WhoopInc/snowflake-semantic-tools)
- [Snowflake Semantic Views Documentation](https://docs.snowflake.com/en/user-guide/semantic-views)
- [Snowflake Cortex LLM Functions](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions)
