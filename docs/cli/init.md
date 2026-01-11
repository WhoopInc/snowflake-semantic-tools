# sst init

Interactive setup wizard for configuring SST in a dbt project.

---

## Overview

The `init` command guides you through setting up Snowflake Semantic Tools in your dbt project. It creates configuration files, directory structure, and optionally generates example files to help you get started.

**Snowflake Connection:** Optional (for connection test)

---

## Quick Start

```bash
# Interactive setup (recommended for first time)
cd your-dbt-project
sst init

# Non-interactive with defaults
sst init --skip-prompts

# Check current setup status
sst init --check-only
```

---

## Syntax

```bash
sst init [OPTIONS]
```

---

## Options

| Option | Type | Description |
|--------|------|-------------|
| `--skip-prompts` | FLAG | Use defaults without prompting |
| `--check-only` | FLAG | Check current setup status without making changes |

---

## What It Does

1. **Detects your dbt project** - Reads `dbt_project.yml`
2. **Reads existing profile** - Finds `~/.dbt/profiles.yml` (or helps create one)
3. **Creates `sst_config.yml`** - Project settings for SST
4. **Creates semantic models directory** - Default: `snowflake_semantic_models/`
5. **Generates example files** - Metrics, relationships, filters, etc.
6. **Optionally tests connection** - Verifies Snowflake credentials

---

## Examples

```bash
# Full interactive setup
sst init

# Skip all prompts, use defaults
sst init --skip-prompts

# Check if SST is already configured
sst init --check-only
```

---

## Output

```
╭─────────────────────────────────────────╮
│ Welcome to Snowflake Semantic Tools!    │
╰─────────────────────────────────────────╯

✓ Detected dbt project: jaffle_shop
✓ Found profile: jaffle_shop (targets: dev, prod)

? Where should SST store semantic models?
  > snowflake_semantic_models (recommended)

? Create example semantic models? Yes

✓ Created sst_config.yml
✓ Created snowflake_semantic_models/
✓ Created example files

Setup Complete!
```

---

## Files Created

### sst_config.yml

```yaml
project:
  semantic_models_dir: "snowflake_semantic_models"

validation:
  exclude_dirs: []
  strict: false
  snowflake_syntax_check: true

enrichment:
  distinct_limit: 25
  sample_values_display_limit: 10
  synonym_model: 'mistral-large2'
  synonym_max_count: 4
```

**Note:** The dbt models directory is auto-detected from `dbt_project.yml`.

### Directory Structure

```
your-dbt-project/
├── sst_config.yml                    # Created
├── snowflake_semantic_models/         # Created
│   ├── metrics/
│   │   └── example_metrics.yml
│   ├── relationships/
│   │   └── example_relationships.yml
│   ├── filters/
│   │   └── example_filters.yml
│   ├── custom_instructions/
│   │   └── example_instructions.yml
│   ├── verified_queries/
│   │   └── example_queries.yml
│   └── semantic_views.yml
└── models/                            # Your existing dbt models
```

---

## Next Steps

After running `sst init`:

```bash
# 1. Verify your configuration
sst debug --test-connection

# 2. Enrich your first models
sst enrich --models customers,orders

# 3. Validate your semantic models
sst validate
```

---

## Troubleshooting

### "No dbt_project.yml found"

Run `sst init` from your dbt project root directory (where `dbt_project.yml` exists).

### "No profiles.yml found"

Create `~/.dbt/profiles.yml` with your Snowflake credentials:

```yaml
your_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: abc12345.us-east-1
      user: your.email@company.com
      authenticator: externalbrowser
      role: YOUR_ROLE
      warehouse: YOUR_WAREHOUSE
      database: YOUR_DATABASE
      schema: YOUR_SCHEMA
```

See [Authentication Guide](../guides/authentication.md) for detailed setup.

### "Profile name mismatch"

Ensure the profile name in `~/.dbt/profiles.yml` matches the `profile:` field in your `dbt_project.yml`.

---

## Related

- [Getting Started](../getting-started.md) - Full setup guide
- [sst debug](debug.md) - Test your configuration
- [Authentication Guide](../guides/authentication.md) - Snowflake connection setup
- [Configuration Reference](../reference/config.md) - sst_config.yml options
