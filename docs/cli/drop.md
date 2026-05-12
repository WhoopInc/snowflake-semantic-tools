# sst drop

Remove semantic views from Snowflake.

## Usage

```bash
# Drop a specific view
sst drop VIEW_NAME [--target TARGET] [--db DB] [--schema SCHEMA] [--dry-run]

# Find and drop orphaned views
sst drop --prune [--target TARGET] [--dry-run] [--yes]
```

## Modes

### Drop a specific view

```bash
sst drop OLD_VIEW_NAME --target prod
```

Executes `DROP SEMANTIC VIEW IF EXISTS db.schema.VIEW_NAME`.

### Prune orphaned views

```bash
sst drop --prune --dry-run --target prod
```

Cross-references actual semantic views in the schema (via `SHOW SEMANTIC VIEWS`) against the `SM_SEMANTIC_VIEWS` tracking table. Views that exist in Snowflake but are NOT tracked by SST are considered orphaned.

## Options

| Option | Description |
|--------|-------------|
| `--target` | dbt profile target |
| `--db` | Override database |
| `--schema` | Override schema |
| `--prune` | Find and drop all orphaned views |
| `--dry-run` | Show what would be dropped without executing |
| `--yes` / `-y` | Skip confirmation prompt (for CI) |
| `--verbose` / `-V` | Show detailed output |

## Safety

- `--prune` shows a confirmation prompt before dropping (skip with `--yes`)
- `--dry-run` shows what would be dropped without executing
- Only targets SST-managed schemas — uses `SM_SEMANTIC_VIEWS` to identify orphans
- Uses `IF EXISTS` for idempotency

## Examples

```bash
# Preview orphaned views
sst drop --prune --dry-run --target prod

# Drop orphaned views (interactive confirmation)
sst drop --prune --target prod

# Drop orphaned views in CI (no prompt)
sst drop --prune --yes --target prod

# Drop a specific renamed view
sst drop OLD_CUSTOMER_360 --target prod
```

## Prerequisites

- Snowflake connection configured (via dbt profiles.yml)
- `SM_SEMANTIC_VIEWS` table must exist (created by `sst extract`)

## Related Commands

- `sst generate` — Create/update semantic views
- `sst deploy` — Full pipeline (validate → extract → generate)
- `sst list` — Show available semantic views
