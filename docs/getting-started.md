# Getting Started with Snowflake Semantic Tools

Complete guide to installing and configuring SST in your dbt project.

---

## Prerequisites

Before installing SST, ensure you have:

- **Python 3.9 or higher** (SST supports Python 3.9-3.13)
- **Access to a Snowflake account** with appropriate permissions
- **A dbt project** with models defined
- **dbt installed** (dbt Core 1.10+ or dbt Cloud CLI)

---

## Installation

### Step 1: Install SST

**Recommended for users:**

**With Poetry (if your dbt project uses Poetry):**
```bash
cd your-dbt-project
poetry add snowflake-semantic-tools
```

**With pip:**
```bash
pip install snowflake-semantic-tools
```

**For development only (contributors):**

If you're contributing to the project or need to modify the source code:
```bash
git clone https://github.com/WhoopInc/snowflake-semantic-tools.git
cd snowflake-semantic-tools
poetry install
```

**Verify installation:**
```bash
sst --version
# Should show: snowflake-semantic-tools, version 0.1.0
```

---

## Configuration

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
  exclude_dirs:
    - "_intermediate"        # Exclude intermediate models
    - "staging"              # Exclude staging models
  strict: false              # Warnings don't block deployment

enrichment:
  distinct_limit: 25                    # Distinct values to fetch
  sample_values_display_limit: 10       # Sample values to show
  synonym_model: 'openai-gpt-4.1'       # LLM for synonyms
  synonym_max_count: 4                  # Max synonyms per field
```

**Required fields:**
- `project.semantic_models_dir` - Directory you created in Step 2 (e.g., "snowflake_semantic_models")
- `project.dbt_models_dir` - Your existing dbt models directory (typically "models")

**Important:** These paths are relative to your project root (where `dbt_project.yml` and `sst_config.yaml` live).

**Optional fields:**
- `validation.exclude_dirs` - Paths to skip during validation
- `validation.strict` - Treat warnings as errors (for CI/CD)
- `enrichment.*` - Control enrichment behavior

### Step 4: Set Up Snowflake Authentication

Create `.env` file in your dbt project root (this file should be in `.gitignore`):

```bash
# .env - Required variables
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your.email@company.com
SNOWFLAKE_WAREHOUSE=YOUR_WAREHOUSE
SNOWFLAKE_ROLE=YOUR_ROLE

# Authentication (choose one):
# Option A: SSO/Browser (recommended for development)
# Leave password empty - browser will open for auth

# Option B: Password
# SNOWFLAKE_PASSWORD=your_password

# Option C: RSA Key Pair (recommended for production)
# SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/rsa_key.p8
```

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
# Test 1: Validate (no Snowflake connection needed)
sst validate

# Should show:
# HH:MM:SS  Running with sst=0.1.0
# HH:MM:SS  Starting validation...
# HH:MM:SS  Validation completed in X.Xs [OK]

# Test 2: Check version
sst --version

# Test 3: View help for any command
sst enrich --help
```

If validation passes, you're ready to go!

---

## Quick Start Workflow

### 1. Enrich dbt Models with Metadata

Add semantic metadata to your existing dbt models:

```bash
# Enrich a specific domain
sst enrich models/analytics/customers/ \
  --database ANALYTICS \
  --schema customers

# Or let SST auto-detect from manifest
dbt compile --target prod
sst enrich models/analytics/customers/
```

**What this does:**
- Queries Snowflake schema
- Populates `meta.sst` blocks
- Adds column types (dimension/fact/time_dimension)
- Adds sample values
- Detects enums

**Output:**
```
09:15:00  Running with sst=0.1.0
09:15:00  Connecting to Snowflake...
09:15:02  Connected to Snowflake [OK in 2.1s]

09:15:02  Enriching metadata from Snowflake
09:15:02  Discovering models in models/analytics/customers/...
09:15:02    Found 5 model(s) to enrich

09:15:02  Enriching 5 model(s)...
09:15:03   1 of  5  customer_status_daily .......... [RUN]
09:15:05   1 of  5  customer_status_daily .......... [OK in 2.3s]
...
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
    join_type: left_outer
    relationship_type: many_to_one
    relationship_columns:
      - left_column: {{ column('orders', 'customer_id') }}
        right_column: {{ column('customers', 'id') }}
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
sst deploy --db ANALYTICS --schema SEMANTIC_VIEWS --verbose

# Option B: Step-by-step (for debugging)
sst validate
sst extract --db ANALYTICS --schema SEMANTIC_VIEWS
sst generate --metadata-db ANALYTICS --metadata-schema SEMANTIC_VIEWS \
  --target-db ANALYTICS --target-schema SEMANTIC_VIEWS --all
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
# Refresh sample values
sst enrich models/domain/ --sample-values

# Re-generate synonyms
sst enrich models/domain/ --synonyms --force-synonyms
```

---

## Directory Structure

After setup, your dbt project should look like:

```
your-dbt-project/
├── dbt_project.yml
├── sst_config.yaml              # SST configuration
├── .env                         # Snowflake credentials (don't commit!)
├── models/                      # dbt models
│   └── analytics/
│       ├── customers/
│       │   ├── customers.sql
│       │   └── customers.yml    # Contains meta.sst blocks
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
1. Check `.env` file has required variables
2. Test with `sst extract --verbose` to see auth details
3. Verify Snowflake account URL is correct
4. See [Authentication Guide](authentication.md)

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

### "OCSP certificate validation error" (Error 254007)

**Problem:** Certificate validation fails when connecting to Snowflake S3 staging:
```
ERROR: 254007: The certificate is revoked or could not be validated
```

**Cause:** A known issue with recent `certifi` package versions (2025.4.26+) affecting the Snowflake Python connector's OCSP validation.

**Solution (temporary):** Downgrade certifi to a stable version:
```bash
pip install certifi==2025.1.31
```

**Alternative solutions:**
```python
# For Python connector 3.14.0+
con = snowflake.connector.connect(disable_ocsp_checks=True)

# For Python connector 3.13.2 or lower
con = snowflake.connector.connect(insecure_mode=True)
```

**Note:** Snowflake is actively working on a permanent fix. Monitor [status.snowflake.com](https://status.snowflake.com/) for updates.

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
