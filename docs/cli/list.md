# sst list

Explore semantic model components (offline, no Snowflake needed).

---

## Overview

The `list` command inspects your semantic model definitions and displays metrics, tables, relationships, filters, semantic views, and more. It reads from the compiled manifest (`target/sst_manifest.json`) when available, falling back to YAML parsing otherwise.

**Snowflake Connection:** Not required

---

## Quick Start

```bash
# Show summary of all components
sst list

# List all metrics
sst list metrics

# Filter metrics by table
sst list metrics --table orders

# Export to CSV
sst list metrics -o metrics.csv -f csv
```

---

## Syntax

```bash
sst list [SUBCOMMAND] [OPTIONS]
```

---

## Subcommands

| Subcommand | Description |
|------------|-------------|
| `summary` | Overview of all components (default) |
| `metrics` | Metrics with expressions |
| `tables` | Tables and column counts |
| `relationships` | Join relationships between tables |
| `filters` | Named filters |
| `semantic-views` | Configured semantic views |
| `custom-instructions` | Custom LLM instructions |
| `verified-queries` | Verified query examples |

---

## Options

Options vary by subcommand. Common options:

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--table` | | TEXT | | Filter results by table name |
| `--format` | `-f` | TEXT | `table` | Output format: `table`, `json`, `csv` |
| `--output` | `-o` | PATH | stdout | Write output to file |

---

## Examples

```bash
# Summary of all components
sst list
sst list summary

# Metrics exploration
sst list metrics
sst list metrics --table orders
sst list metrics --format json

# Tables and columns
sst list tables

# Relationships
sst list relationships

# Filters
sst list filters

# Semantic views
sst list semantic-views

# Export to file
sst list metrics -o metrics.csv -f csv
sst list tables -o tables.json -f json
```

---

## Output

### Summary (Default)

```
Running with sst=0.3.0

Component Summary:
  Tables:              12
  Metrics:             45
  Relationships:       18
  Filters:              6
  Semantic Views:       3
  Custom Instructions:  2
  Verified Queries:    15

Done [OK]
```

### Metrics Table

```
Running with sst=0.3.0

Metrics (45):
┌──────────────────────┬──────────────┬────────────────────────────────┐
│ Name                 │ Table        │ Expression                     │
├──────────────────────┼──────────────┼────────────────────────────────┤
│ total_revenue        │ orders       │ SUM(amount)                    │
│ order_count          │ orders       │ COUNT(*)                       │
│ avg_order_value      │ orders       │ AVG(amount)                    │
└──────────────────────┴──────────────┴────────────────────────────────┘

Done [OK]
```

---

## Data Sources

The `list` command reads data from two sources (in priority order):

1. **Compiled manifest** (`target/sst_manifest.json`) — Used when available. Run `sst compile` to generate.
2. **YAML files** — Falls back to parsing semantic model YAML directly.

For the most accurate results, run `sst compile` first:

```bash
sst compile
sst list metrics
```

---

## When to Use

- **Discover what's defined** — Quickly see all metrics, relationships, filters in your project
- **Audit coverage** — Check which tables have metrics, which lack relationships
- **Export for review** — Generate CSV/JSON for stakeholder review
- **CI validation** — Verify expected component counts in pipelines

---

## Related

- [sst compile](compile.md) - Compile manifest for accurate listing
- [sst validate](validate.md) - Validate semantic models
- [sst enrich](enrich.md) - Add metadata to models
