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

Cross-references actual semantic views in the schema (via `SHOW SEMANTIC VIEWS`) against the compiled manifest (`target/sst_manifest.json`). Views that exist in Snowflake but are NOT defined in the manifest are considered orphaned.

If no manifest is found, falls back to the legacy `SM_SEMANTIC_VIEWS` tracking table with a warning.

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
- Only targets SST-managed schemas — uses compiled manifest to identify orphans
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
- `sst compile` has been run (creates `target/sst_manifest.json`)
- Falls back to `SM_SEMANTIC_VIEWS` if no manifest exists (legacy workflows)

## Related Commands

- `sst generate` — Create/update semantic views
- `sst deploy` — Full pipeline (validate → extract → generate)
- `sst list` — Show available semantic views
