# Getting Started with Snowflake Semantic Tools

This guide walks you through **setting up SST in an existing dbt project** to build and deploy Snowflake Semantic Views.

**Already have a project using SST?** Just install SST and you're ready to go:

```bash
pip install snowflake-semantic-tools
sst debug --test-connection  # Verify your setup
```

---

## Prerequisites

Before installing SST, ensure you have:

- **Python 3.10 or 3.11** (required)
- **Access to a Snowflake account** with appropriate permissions
- **A dbt project** with models defined
- **dbt installed** (dbt Core or dbt Cloud CLI)

---

## Installation

```bash
pip install snowflake-semantic-tools
```

**Verify installation:**

```bash
sst --version
# Should show: snowflake-semantic-tools, version 0.2.2
```

---

## Quick Start with `sst init`

The easiest way to set up SST in your dbt project:

```bash
cd your-dbt-project
sst init
```

The wizard will:
1. Detect your dbt project and profile configuration
2. Help you set up Snowflake credentials (if not already configured)
3. Create `sst_config.yaml` with sensible defaults
4. Create the semantic models directory structure
5. Generate example files to get you started

**Example output:**
```
╭─────────────────────────────────────────╮
│ Welcome to Snowflake Semantic Tools!    │
╰─────────────────────────────────────────╯

✓ Detected dbt project: jaffle_shop
✓ Found profile: jaffle_shop (targets: dev, prod)

? Where should SST store semantic models?
  > snowflake_semantic_models (recommended)

✓ Created sst_config.yaml
✓ Created snowflake_semantic_models/
✓ Created example files

Setup Complete!
```

**Options:**
- `sst init --skip-prompts` - Use defaults without prompting
- `sst init --check-only` - Check current setup status

If you prefer manual setup, continue with the steps below.

---

## Manual Configuration

### Step 2: Create Required Directories

Create the semantic models directory in your dbt project:

```bash
cd your-dbt-project

# Create semantic models directory
mkdir -p snowflake_semantic_models/metrics
mkdir -p snowflake_semantic_models/relationships
mkdir -p snowflake_semantic_models/filters
mkdir -p snowflake_semantic_models/custom_instructions
mkdir -p snowflake_semantic_models/verified_queries

# Create initial files (optional but helpful)
touch snowflake_semantic_models/semantic_views.yml
```

**Directory structure you just created:**
```
your-dbt-project/
├── snowflake_semantic_models/     # This directory holds semantic layer definitions
│   ├── metrics/                   # Business metrics (KPIs, calculations)
│   ├── relationships/             # How tables join together
│   ├── filters/                   # Reusable WHERE clauses
│   ├── custom_instructions/       # AI behavior customization
│   ├── verified_queries/          # Example queries for AI
│   └── semantic_views.yml         # View definitions
└── models/                        # Your existing dbt models (already exists)
```

**Note:** The directory name `snowflake_semantic_models` is a convention. You can use a different name, but it must match what you specify in `sst_config.yaml` in the next step.

### Step 3: Create sst_config.yaml

Create this file in your dbt project root (same directory as `dbt_project.yml`):

```yaml
# sst_config.yaml
project:
  # Directory you created in Step 2
  semantic_models_dir: "snowflake_semantic_models"  # Required - must match directory name
  
  # Your existing dbt models directory
  dbt_models_dir: "models"                          # Required - typically "models"

validation:
  exclude_dirs: []             # Paths to skip during validation
  strict: false                # Warnings don't block deployment
  snowflake_syntax_check: true # Validate SQL against Snowflake

enrichment:
  distinct_limit: 25                    # Distinct values to fetch
  sample_values_display_limit: 10       # Sample values to show
  synonym_model: 'mistral-large2'       # LLM for synonyms (universally available)
  synonym_max_count: 4                  # Max synonyms per field
```

**Required fields:**
- `project.semantic_models_dir` - Directory you created in Step 2 (e.g., "snowflake_semantic_models")
- `project.dbt_models_dir` - Your existing dbt models directory (typically "models")

**Important:** These paths are relative to your project root (where `dbt_project.yml` and `sst_config.yaml` live).

### Step 4: Set Up Snowflake Authentication

SST uses dbt's `~/.dbt/profiles.yml` for Snowflake authentication. If you already have this configured for dbt, you're all set!

If not, create `~/.dbt/profiles.yml`:

```bash
mkdir -p ~/.dbt
```

Add your Snowflake connection (the profile name must match `profile:` in your `dbt_project.yml`):

```yaml
# ~/.dbt/profiles.yml
your_project:  # Must match 'profile:' in dbt_project.yml
  target: dev
  outputs:
    dev:
      type: snowflake
      account: your_account.us-east-1  # Your Snowflake account
      user: your.email@company.com
      authenticator: externalbrowser   # Opens browser for SSO
      role: YOUR_ROLE
      warehouse: YOUR_WAREHOUSE
      database: ANALYTICS
      schema: DEV
```

**Authentication methods:**
- **SSO/Browser** (recommended): Use `authenticator: externalbrowser`
- **Password**: Add `password: your_password` or use `{{ env_var('SNOWFLAKE_PASSWORD') }}`
- **RSA Key Pair** (production): Add `private_key_path: ~/.ssh/snowflake_key.p8`

**See:** [Authentication Guide](authentication.md) for detailed setup of each auth method.

### Step 5: Generate manifest.json

SST uses dbt's manifest to auto-detect database and schema locations:

```bash
cd your-dbt-project
dbt compile --target prod
# Creates target/manifest.json
```

**Why this matters:**
- SST auto-detects database/schema from manifest
- No need to specify `--database` and `--schema` for every command
- Works correctly across all environments (dev/qa/prod)

---

## Verify Setup

Test that everything is configured correctly:

```bash
# Test 1: Show configuration (verifies profiles.yml is read correctly)
sst debug

# Test 2: Test Snowflake connection
sst debug --test-connection

# Test 3: Validate semantic models
sst validate

# Test 4: Check version
sst --version
```

**Example `sst debug` output:**
```
SST Debug (v0.2.2)

  ──────────────────────────────────────────────────
  Profile Configuration
  ──────────────────────────────────────────────────
  Profile:        your_project
  Target:         dev
  ──────────────────────────────────────────────────
  Account:        your_account.us-east-1
  User:           your.email@company.com
  Role:           YOUR_ROLE
  Warehouse:      YOUR_WAREHOUSE
  Database:       ANALYTICS
  Schema:         DEV
  Auth Method:    sso_browser
  ──────────────────────────────────────────────────

  ✓ Configuration valid
```

If `sst debug` shows your configuration and `sst validate` passes, you're ready to go!

---

## Quick Start Workflow

### 1. Enrich dbt Models with Metadata

Add semantic metadata to your existing dbt models:

```bash
# Compile dbt to generate manifest (required for --models)
dbt compile --target prod

# Enrich specific models by name
sst enrich --models customers,orders

# Or enrich an entire directory
sst enrich models/analytics/
```

**What this does:**
- Queries Snowflake schema
- Populates `config.meta.sst` blocks (dbt Fusion compatible)
- Adds column types (dimension/fact/time_dimension)
- Adds sample values
- Detects enums

> **Note:** SST writes metadata in the new `config.meta.sst` format required by dbt Fusion. If you have existing `meta.sst` blocks, run `sst migrate-meta` to migrate them. See [dbt Fusion Migration Guide](migration-guide-dbt-fusion.md).

**Output:**
```
09:15:00  Running with sst=0.2.2
09:15:00  Resolving 2 model name(s)...
09:15:00  Resolved 2 model(s) [OK]
09:15:00  Connecting to Snowflake...
09:15:02  Connected to Snowflake [OK in 2.1s]

09:15:02  Enriching 2 model(s)...
09:15:03   1 of  2  customers .......... [OK in 2.3s]
09:15:05   2 of  2  orders ............. [OK in 1.8s]
```

### 2. Create Semantic Models

Create semantic layer definitions in `snowflake_semantic_models/`:

**Metrics** (`metrics/sales.yml`):
```yaml
snowflake_metrics:
  - name: total_revenue
    description: Total revenue from all orders
    tables:
      - {{ table('orders') }}
    expr: SUM({{ column('orders', 'amount') }})
```

**Relationships** (`relationships/core.yml`):
```yaml
snowflake_relationships:
  - name: orders_to_customers
    left_table: {{ table('orders') }}
    right_table: {{ table('customers') }}
    relationship_conditions:
      - "{{ column('orders', 'customer_id') }} = {{ column('customers', 'customer_id') }}"
```

**Semantic Views** (`semantic_views.yml`):
```yaml
semantic_views:
  - name: sales_analytics
    description: Sales data with customer context
    tables:
      - {{ table('orders') }}
      - {{ table('customers') }}
```

### 3. Validate Everything

Check for errors before deployment:

```bash
sst validate --verbose

# Output shows:
# - 0 errors
# - Warnings by category (missing primary_key, no synonyms, etc.)
# - Grouped summary
```

### 4. Deploy to Snowflake

Deploy metadata and generate semantic views:

```bash
# Option A: One-step deployment (recommended)
sst deploy --target prod

# Option B: Step-by-step (for debugging)
sst validate
sst extract --target prod
sst generate --target prod --all
```

**What deployment does:**
1. Validates semantic models (0 errors required)
2. Extracts metadata to Snowflake tables (SM_*)
3. Generates semantic views for BI tools

---

## Common Tasks

### Format YAML Files

Keep your YAML files consistently formatted:

```bash
# Format all models
sst format models/

# Sanitize problematic characters (apostrophes in synonyms)
sst format models/ --sanitize

# Preview changes before applying
sst format models/ --dry-run
```

### Update Metadata

Re-enrich when your Snowflake schema changes:

```bash
# Refresh sample values for specific models
sst enrich --models customers,orders --sample-values

# Re-generate synonyms
sst enrich --models customers --synonyms --force-synonyms
```

---

## Directory Structure

After setup, your dbt project should look like:

```
your-dbt-project/
├── dbt_project.yml
├── sst_config.yaml              # SST configuration
├── models/                      # dbt models
│   └── analytics/
│       ├── customers/
│       │   ├── customers.sql
│       │   └── customers.yml    # Contains config.meta.sst blocks
│       └── orders/
│           ├── orders.sql
│           └── orders.yml
├── snowflake_semantic_models/   # Semantic layer definitions
│   ├── metrics/
│   │   └── sales.yml
│   ├── relationships/
│   │   └── core.yml
│   └── semantic_views.yml
└── target/
    └── manifest.json            # Generated by dbt compile

# Plus your dbt authentication (in home directory):
~/.dbt/
└── profiles.yml                 # Snowflake credentials (don't commit!)
```

---

## Troubleshooting

### "Config error: Missing required field"

**Problem:** `sst_config.yaml` missing or incorrectly configured

**Solution:**
```bash
# Ensure file exists in dbt project root
ls sst_config.yaml

# Check required fields are present:
# - project.semantic_models_dir
# - project.dbt_models_dir
```

### "manifest.json not found"

**Problem:** SST can't find dbt's compiled manifest

**Solution:**
```bash
# Compile your dbt project
dbt compile --target prod

# Or use auto-compile flag
sst validate --dbt-compile
```

### "Failed to connect to Snowflake"

**Problem:** Authentication or credentials issue

**Solution:**
1. Run `sst debug` to verify your profile configuration
2. Run `sst debug --test-connection` to test the Snowflake connection
3. Check `~/.dbt/profiles.yml` exists and is correctly configured
4. Verify profile name in `dbt_project.yml` matches profiles.yml
5. Verify Snowflake account URL is correct
6. See [Authentication Guide](authentication.md)

### "No models found"

**Problem:** SST can't find your models

**Solution:**
```bash
# Check sst_config.yaml paths are correct
# Paths should be relative to project root

# Verify models exist
ls models/
ls snowflake_semantic_models/
```

### "Model unavailable" during synonym generation

**Problem:** Cortex model not available error when running `sst enrich --synonyms`:
```
Model "openai-gpt-4.1" is unavailable
```

**Cause:** OpenAI models (gpt-4.1, gpt-5, etc.) are only available on Snowflake accounts hosted on Azure, or require cross-region inference to be enabled.

**Solution:** Use a universally available model in `sst_config.yaml`:
```yaml
enrichment:
  synonym_model: 'mistral-large2'  # Works on AWS, Azure, GCP
```

Other universally available models:
- `llama3.1-70b`, `llama3.1-8b` (Meta open models)
- `mixtral-8x7b`, `mistral-7b` (fast, lower cost)

**See:** [Snowflake Cortex Model Availability](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions#availability) for models available in your region.

### YAML Parsing Errors

**Problem:** Metrics or other semantic models are silently skipped or fail with YAML errors:
```
YAML error in metrics.yml at line 6: Invalid mapping values
```

**Cause:** YAML has strict syntax rules. Common issues include:
- Unquoted colons (`:`) in descriptions
- Template syntax (`{{ }}`) on the same line as other content

**Solution:** Use proper YAML syntax for strings with special characters:

```yaml
# ❌ WRONG - colon breaks YAML parsing
description: Use this metric like this: SUM(amount)

# ✅ CORRECT - use multiline syntax
description: |-
  Use this metric like this: SUM(amount)

# ✅ CORRECT - or quote the string
description: "Use this metric like this: SUM(amount)"
```

**For template syntax:**
```yaml
# ✅ CORRECT - templates on their own line
tables:
  - {{ table('customers') }}

expr: |
  SUM({{ column('orders', 'amount') }})
```

---

## Next Steps

Now that SST is set up:

1. **Enrich your models:** [User Guide - Enrichment](user-guide.md#metadata-enrichment)
2. **Learn CLI commands:** [CLI Reference](cli-reference.md)
3. **Write semantic models:** [Semantic Models Guide](semantic-models-guide.md)
4. **Configure authentication:** [Authentication Guide](authentication.md)
5. **Understand validation:** [Validation Checklist](validation-checklist.md)

---

**You're ready to use Snowflake Semantic Tools!**

For questions or issues, see the [GitHub repository](https://github.com/WhoopInc/snowflake-semantic-tools).
