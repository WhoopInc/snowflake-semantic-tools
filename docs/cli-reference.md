# Snowflake Semantic Tools CLI Reference

## Table of Contents
- [Prerequisites](#prerequisites)
- [Commands Overview](#commands-overview)
- [Command Reference](#command-reference)
  - [debug](#debug)
  - [validate](#validate)
  - [enrich](#enrich)
  - [format](#format)
  - [extract](#extract)
  - [generate](#generate)
  - [deploy](#deploy)
- [Quick Reference](#command-quick-reference)

## Prerequisites

Before using the CLI, ensure you have:

- **Installation:** See [Getting Started](getting-started.md) for installation instructions
- **Configuration:** See [Configuration Guide](getting-started.md) for setup
- **Authentication:** See [Authentication Guide](authentication.md) for Snowflake credentials

This reference focuses on command syntax and options.

## Commands Overview

| Command | Description | Snowflake Connection Required |
|---------|-------------|-------------------------------|
| `debug` | Show configuration and test Snowflake connection | Optional (for `--test-connection`) |
| `validate` | Validate semantic models against dbt definitions | No |
| `enrich` | Enrich dbt YAML metadata with semantic information | Yes |
| `format` | **YAML Linter** - Ensure project-wide formatting consistency | No |
| `extract` | Extract metadata from dbt/semantic models to Snowflake | Yes |
| `generate` | Generate Snowflake SEMANTIC VIEWs and/or YAML models | Yes |
| `deploy` | **One-step:** validate → extract → generate | Yes |

**For production deployments, use the `deploy` command (see below) which orchestrates validate → extract → generate automatically.**

## Command Reference

### debug

Show configuration and optionally test Snowflake connection. Use this command to verify your dbt profile is configured correctly before running other SST commands.

```bash
sst debug [OPTIONS]
```

#### Options

| Option | Short | Type | Description |
|--------|-------|------|-------------|
| `--target` | `-t` | TEXT | dbt target from profiles.yml (default: uses profile's default) |
| `--test-connection` | | FLAG | Test Snowflake connection |
| `--verbose` | `-v` | FLAG | Show additional details |

#### What It Does

1. Reads `dbt_project.yml` to find the profile name
2. Locates `profiles.yml` (in project directory or `~/.dbt/`)
3. Parses the profile for the specified target
4. Displays all connection parameters
5. Optionally tests the Snowflake connection

#### Examples

```bash
# Show current configuration
sst debug

# Test Snowflake connection
sst debug --test-connection

# Use a specific target
sst debug --target prod

# Test production connection
sst debug --target prod --test-connection
```

#### Output Format

```
SST Debug (v0.1.1)

  ──────────────────────────────────────────────────
  Profile Configuration
  ──────────────────────────────────────────────────
  Profile:        my_project
  Target:         dev
  ──────────────────────────────────────────────────
  Account:        abc12345.us-east-1
  User:           your.email@company.com
  Role:           DATA_ENGINEER
  Warehouse:      MY_WAREHOUSE
  Database:       ANALYTICS
  Schema:         DEV
  Auth Method:    sso_browser
  ──────────────────────────────────────────────────
  profiles.yml:   ~/.dbt/profiles.yml
  dbt_project:    ./dbt_project.yml
  ──────────────────────────────────────────────────

  ✓ Configuration valid
```

With `--test-connection`:

```
  Testing Snowflake connection...

  ✓ Connection successful!
    Connected as: YOUR_USER
    Current role: DATA_ENGINEER
    Warehouse: MY_WAREHOUSE
```

---

### enrich

Enrich dbt YAML metadata with semantic information from Snowflake. Automatically populates meta.sst blocks with column types, data types, sample values, primary keys, and enum detection.

```bash
sst enrich TARGET_PATH [OPTIONS]
```

#### Arguments

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `TARGET_PATH` | PATH | **Yes** | Path to: directory, .sql file, or .yml/.yaml file |

#### Options

##### Component Selection

Control which metadata components to enrich:

| Option | Short | Type | Description |
|--------|-------|------|-------------|
| `--column-types` | `-ct` | FLAG | Enrich column types (dimension/fact/time_dimension) |
| `--data-types` | `-dt` | FLAG | Enrich data types (map Snowflake types) |
| `--sample-values` | `-sv` | FLAG | Enrich sample values (queries data - SLOW) |
| `--detect-enums` | `-de` | FLAG | Detect enum columns (low cardinality) |
| `--primary-keys` | `-pk` | FLAG | Validate primary key candidates |
| `--table-synonyms` | `-ts` | FLAG | Generate table-level synonyms via Cortex LLM |
| `--column-synonyms` | `-cs` | FLAG | Generate column-level synonyms via Cortex LLM |
| `--synonyms` | `-syn` | FLAG | Generate both table and column synonyms (shorthand) |
| `--all` | | FLAG | Enrich ALL components including synonyms |

**Note:** If no component flags are specified, defaults to: `--column-types --data-types --sample-values --detect-enums`

##### Force Options

Overwrite existing metadata (normally preserved):

| Option | Description |
|--------|-------------|
| `--force-synonyms` | Overwrite existing synonyms (re-generate even if synonyms exist) |
| `--force-column-types` | Overwrite existing column types (re-infer even if types exist) |
| `--force-data-types` | Overwrite existing data types (re-map even if data types exist) |
| `--force-primary-keys` | Overwrite existing primary keys (re-validate even if primary key exists) |
| `--force-all` | Overwrite ALL existing values (force refresh everything) |

##### General Options

| Option | Type | Description |
|--------|------|-------------|
| `--database` | TEXT | Override database name detection |
| `--schema` | TEXT | Override schema name detection |
| `--manifest` | PATH | Path to manifest.json (default: ./target/manifest.json) |
| `--allow-non-prod` | FLAG | Allow enrichment from non-production manifest |
| `--pk-candidates` | PATH | JSON file with primary key candidates |
| `--exclude` | TEXT | Comma-separated list of directories to exclude (adds to sst_config.yaml exclusions) |
| `--dry-run` | FLAG | Preview changes without writing files |
| `--fail-fast` | FLAG | Stop on first error |
| `--verbose` | FLAG | Enable verbose logging |

#### Features

- **Column Type Classification**: Automatically determines `dimension`, `fact`, or `time_dimension`
- **Data Type Mapping**: Maps Snowflake types to SST semantic types
- **Sample Values**: Extracts up to 25 distinct sample values per column
- **Primary Key Validation**: Validates user-provided primary key candidates
- **Enum Detection**: Identifies low-cardinality columns suitable for enums
- **LLM-Based Synonym Generation**: Uses Snowflake Cortex to generate natural language synonyms for tables and columns
- **PII Protection**: Respects `privacy_category: direct_identifier` columns
- **Metadata Preservation**: Never overwrites existing descriptions, synonyms, or manual metadata (unless using `--force-*` flags)
- **Retry Logic**: Automatically retries Snowflake connection errors with exponential backoff

#### Examples

```bash
# Enrich entire directory with default components
sst enrich models/domain/ --database PROD_DB --schema domain

# Enrich single model (via SQL file)
sst enrich models/users/users.sql --database PROD_DB --schema users

# Fast enrichment: only data types (skips expensive sample queries)
sst enrich models/domain/ -dt --database PROD_DB --schema domain

# Fast enrichment: only column types  
sst enrich models/domain/ -ct --database PROD_DB --schema domain

# Refresh sample values only (slower - queries Snowflake data)
sst enrich models/domain/ -sv --database PROD_DB --schema domain

# Enrich with LLM-generated synonyms (using short flags)
sst enrich models/domain/ -ct -dt -sv -syn --database PROD_DB --schema domain

# Generate only table-level synonyms (faster)
sst enrich models/domain/ -ts --database PROD_DB --schema domain

# Enrich everything including synonyms
sst enrich models/domain/ --all --database PROD_DB --schema domain

# Re-generate synonyms even if they already exist
sst enrich models/domain/ -syn --force-synonyms --database PROD_DB --schema domain

# With primary key candidates
sst enrich models/domain/ -pk --pk-candidates pk_candidates.json --database PROD_DB --schema domain

# Dry run to preview changes
sst enrich models/ --dry-run --verbose

# Exclude directories (adds to sst_config.yaml exclusions for this run only)
sst enrich models/ \
  --exclude _intermediate,temp_models \
  --database PROD_DB \
  --schema public
  
# Note: Configure permanent exclusions in sst_config.yaml instead of using --exclude repeatedly
```

#### Synonym Generation Configuration

Control LLM behavior in `sst_config.yaml`:

```yaml
enrichment:
  # LLM model for synonym generation (mistral-large2 is universally available)
  synonym_model: 'mistral-large2'  # Options: mistral-large2, llama3.1-70b, etc.
  
  # Maximum synonyms per table/column
  synonym_max_count: 4
```

#### Primary Key Candidates Format

Create a JSON file with primary key candidates:

```json
{
  "customers": [
    ["customer_id"]
  ],
  "orders": [
    ["order_id"]
  ],
  "order_items": [
    ["order_id", "line_item_id"],
    ["order_item_id"]
  ],
  "daily_metrics": [
    ["date", "metric_id"]
  ]
}
```

#### Output Format

```
18:20:00  Enriching 15 model(s) in models/customers
18:20:01   1 of 15  customer_status_daily          [OK in 2.3s]
18:20:03   2 of 15  customer_lifecycle             [OK in 1.8s]
18:20:05   3 of 15  customer_summary               [OK in 2.1s]
...
18:20:30  15 of 15  customer_preferences           [OK in 1.5s]
18:20:30  Completed in 30.2s (15 enriched)
```

For detailed usage, see the [Enrichment Guide](user-guide.md).

---

### validate

Validate semantic models against dbt definitions. Checks for missing references, circular dependencies, duplicates, and syntax errors.

```bash
sst validate [OPTIONS]
```

#### Options

| Option | Short | Type | Required | Default | Description |
|--------|-------|------|----------|---------|-------------|
| `--dbt` | | PATH | No | Auto-detect | Path to dbt models directory |
| `--semantic` | | PATH | No | Auto-detect | Path to semantic models directory |
| `--strict` | | FLAG | No | False | Fail on warnings (not just errors) |
| `--verbose` | `-v` | FLAG | No | False | Show detailed validation output |
| `--exclude` | | TEXT | No | | Comma-separated list of directories to exclude |
| `--dbt-compile` | | FLAG | No | False | Auto-run `dbt compile` to generate/refresh manifest.json before validation |

**Important Notes:**
- Validates files as they exist in your working directory (committed or uncommitted changes)
- Uses `sst_config.yml` to locate model directories unless overridden with `--dbt` or `--semantic`
- `--dbt-compile` automatically generates manifest.json using `DBT_TARGET` env var (defaults to `prod`)
- See [Validation Checklist](validation-checklist.md) for complete list of all 98 checks

#### Examples

```bash
# Most common: validate current directory (run from your dbt project root)
sst validate

# Auto-compile dbt if manifest missing/stale (uses 'prod' target by default)
sst validate --dbt-compile

# Use custom dbt target with auto-compile
export DBT_TARGET=ci
sst validate --dbt-compile

# Validate with verbose output
sst validate --verbose

# Strict mode (fail on warnings)
sst validate --strict

# Custom paths (override config)
sst validate --dbt models/ --semantic semantic_models/

# Exclude directories (temporary, adds to sst_config.yaml)
sst validate --exclude temp_models,experimental

# Better approach: Configure in sst_config.yaml:
#   exclude_dirs:
#     - "_intermediate"
#     - "models/legacy/*"  # Glob patterns supported!
```

#### Output Format

**With warnings (real-time display):**
```
18:15:14  Validating 1188 model(s)
18:15:15  WARNING in INCOMPLETE_MODEL: Table has no synonyms
18:15:16  WARNING in ANOTHER_MODEL: Skipped (missing primary_key)
18:15:17  WARNING in TABLE_C: Missing description
18:15:37  Validation completed with 0 error(s), 3 warning(s)

Warnings:
  [WARNING] Table 'INCOMPLETE_MODEL' has no synonyms defined
  [WARNING] Table 'ANOTHER_MODEL' skipped (missing primary_key)
  [WARNING] Table 'TABLE_C' is missing description
```

**With errors (real-time display):**
```
18:15:14  Validating 1188 model(s)
18:15:15  ERROR in bad_metric: References non-existent table 'sales'
18:15:16  ERROR in broken_relationship: Missing left table
18:15:37  Validation failed with 2 error(s), 0 warning(s)

Errors:
  [ERROR] Metric 'revenue' references non-existent table 'sales'
  [ERROR] Relationship 'orders_to_customers' missing left table
```

**Success (clean):**
```
18:15:14  Validating 1188 model(s)
18:15:37  Validation completed with 0 error(s), 0 warning(s)
```

**Note:** All errors/warnings appear in real-time (as they're found) AND are summarized at the end. Technical logs go to `logs/sst.log`.

---

## format

**YAML Linter** - Standardizes YAML file structure and formatting for project-wide consistency.

**Purpose:** Acts as a linter to ensure all YAML files follow consistent formatting standards across your entire dbt project. Makes files easier to read, maintain, and review in pull requests.

**Snowflake Connection:** Not required

### Syntax

```bash
sst format PATH [OPTIONS]
```

### Options

| Option | Short | Type | Required | Default | Description |
|--------|-------|------|----------|---------|-------------|
| `PATH` | | PATH | Yes | - | File or directory to format |
| `--dry-run` | | FLAG | No | False | Preview changes without modifying files |
| `--check` | | FLAG | No | False | Check if files need formatting (exit code 1 if changes needed) |
| `--force` | | FLAG | No | False | Always write files, even if content appears unchanged (useful for IDE cache issues) |

### What It Does

The format command applies the following transformations:

**Field Ordering:**
- Models: `name` → `description` → `meta` → `config` → `columns`
- Columns: `name` → `description` → `data_tests` → `meta`
- SST metadata: `column_type` → `data_type` → `synonyms` → `sample_values` → `is_enum` → `privacy_category`

**Blank Line Management:**
- Adds blank line before each new column definition
- Removes excessive consecutive blank lines
- Ensures file ends with exactly one newline

**Indentation:**
- 2-space indentation for maps/objects
- 4-space indentation for sequences/lists
- Consistent dash offset for list items

**Multi-line Descriptions:**
- Converts `>` (folded) to `|-` (literal with strip)
- Wraps at 80 characters without breaking words
- Preserves line breaks for readability

**List Formatting:**
- Empty lists → `[]` (inline format, clean placeholder)
- Lists with items → Multi-line format (one item per line)
- Applies to: `synonyms`, `sample_values`, `primary_key`, `unique_keys`

**Character Sanitization (with `--sanitize` flag):**
- **Synonyms:** Removes apostrophes, quotes (SQL-safe for `WITH SYNONYMS` clause)
- **Sample values:** Removes apostrophes, quotes (prevents downstream issues)
- **Descriptions:** Escapes Jinja characters (`{{` → `{ {`), removes control chars
- **Preserves:** Semicolons (useful data delimiters), meaningful content
- **Fixes:** All validation warnings about "problematic characters"

### Examples

```bash
# Format a single file
sst format models/users/users.yml

# Format all files in a directory
sst format models/

# Preview changes without modifying files
sst format models/ --dry-run

# Check if formatting is needed (useful for automation/CI)
sst format models/ --check
# Exit code 0: All files properly formatted
# Exit code 1: Some files need formatting

# Sanitize problematic characters
sst format models/ --sanitize
# Removes apostrophes from synonyms/sample values
# Escapes Jinja characters in descriptions
# Fixes validation warnings about "problematic characters"

# Preview sanitization changes
sst format models/ --sanitize --dry-run

# Sanitize and format in one command
sst format models/ --sanitize

# Format semantic models
sst format snowflake_semantic_models/

# Format entire project
sst format .
```

### Output Format

```
18:35:00  Formatting 10 file(s)
18:35:01  Formatted 3 of 10 file(s) in 1.2s
```

### Best Practices

1. **Run after enrichment:** Format files after running `sst enrich` to ensure consistent structure
2. **Team consistency:** Run format before committing to maintain consistent code style across your project
3. **Safe operation:** Format preserves all content - only structure and spacing change

---

---

### extract

Extract metadata from dbt and semantic models to Snowflake tables.

```bash
sst extract [OPTIONS]
```

#### Options

| Option | Short | Type | Required | Default | Description |
|--------|-------|------|----------|---------|-------------|
| `--db` | | TEXT | **Yes** | | Target database for metadata tables |
| `--schema` | `-s` | TEXT | **Yes** | | Target schema for metadata tables |
| `--dbt` | | PATH | No | Auto-detect | Path to dbt models directory |
| `--semantic` | | PATH | No | Auto-detect | Path to semantic models directory |
| `--account` | | TEXT | No | $SNOWFLAKE_ACCOUNT | Snowflake account |
| `--user` | `-u` | TEXT | No | $SNOWFLAKE_USER | Snowflake user |
| `--role` | | TEXT | No | $SNOWFLAKE_ROLE | Snowflake role |
| `--warehouse` | `-w` | TEXT | No | $SNOWFLAKE_WAREHOUSE | Snowflake warehouse |
| `--verbose` | `-v` | FLAG | No | False | Show detailed extraction progress |

**Important Notes:**
- Run from your dbt project root directory (where `dbt_project.yml` exists)
- Creates or updates the following tables in the target schema:
  - `SM_METRICS` - Metric definitions
  - `SM_RELATIONSHIPS` - Table relationships
  - `SM_RELATIONSHIP_COLUMNS` - Relationship column mappings
  - `SM_DIMENSIONS` - Dimension columns
  - `SM_FACTS` - Fact columns
  - `SM_FILTERS` - Filter definitions
  - `SM_VERIFIED_QUERIES` - Verified query examples
  - `SM_CUSTOM_INSTRUCTIONS` - Custom AI instructions
  - `SM_SEMANTIC_VIEWS` - Semantic view definitions
  - `SM_TABLE_SUMMARIES` - Table metadata summaries
- Extracts files as they exist in your working directory (committed or uncommitted changes)

#### Cortex Search Service

The extract command automatically creates or updates a Cortex Search Service named `SEMANTIC_SEARCH_SERVICE` that indexes the `SM_TABLE_SUMMARIES` table. This service enables AI-powered table discovery for dynamic model generation, but only includes tables where `cortex_searchable=True`.

#### Examples

```bash
# Most common: extract from current directory
sst extract --db PROD_DB --schema SEMANTIC_METADATA

# With verbose output
sst extract --db DEV_DB --schema TEST_SCHEMA --verbose

# Custom paths (override config)
sst extract --db PROD_DB --schema METADATA \
  --dbt models/ --semantic semantic_models/

# With explicit Snowflake credentials
sst extract --db PROD_DB --schema SEMANTIC_VIEWS \
  --account my_account.us-east-1 \
  --user analytics_user \
  --warehouse MY_WAREHOUSE
```

#### Output Format

```
Starting semantic metadata extraction...
Successfully extracted 3942 models

Tables populated:
  SM_METRICS: 177 rows
  SM_RELATIONSHIPS: 44 rows
  SM_DIMENSIONS: 2531 rows
  SM_FACTS: 554 rows
  ...
```

---

### generate

Generate Semantic Views and/or YAML Models from metadata.

This unified command can generate both native Snowflake Semantic Views and YAML semantic models from the same metadata source.

```bash
sst generate [OPTIONS]
```

#### Options

| Option | Short | Type | Required | Default | Description |
|--------|-------|------|----------|---------|-------------|
| `--metadata-db` | | TEXT | **Yes** | | Database containing metadata tables |
| `--metadata-schema` | | TEXT | **Yes** | | Schema containing metadata tables |
| `--target-db` | | TEXT | **Yes** | | Target database for semantic views |
| `--target-schema` | | TEXT | **Yes** | | Target schema for semantic views |
| `--views` | `-v` | TEXT | No* | | Specific views to generate (repeatable) |
| `--all` | `-a` | FLAG | No* | False | Generate all available views |
| `--defer-database` | | TEXT | No | | Override table database (like dbt defer) - reference prod tables from dev |
| `--account` | | TEXT | No | $SNOWFLAKE_ACCOUNT | Snowflake account |
| `--user` | `-u` | TEXT | No | $SNOWFLAKE_USER | Snowflake user |
| `--role` | | TEXT | No | $SNOWFLAKE_ROLE | Snowflake role |
| `--warehouse` | `-w` | TEXT | No | $SNOWFLAKE_WAREHOUSE | Snowflake warehouse |
| `--dry-run` | | FLAG | No | False | Preview without executing |
| `--verbose` | | FLAG | No | False | Show detailed progress |

*Note: Either `--views` or `--all` must be provided

#### Defer Database (Local Development)

The `--defer-database` parameter enables local development similar to dbt's defer feature. Use this when you want to:
- Generate semantic views in your development database (SCRATCH, DEV)
- But have those views reference tables from production database (ANALYTICS, PROD)
- Test metadata changes locally without rebuilding all dbt models

**How it works:**
- Metadata is read from `--metadata-db.--metadata-schema` (your dev database)
- Views are created in `--target-db.--target-schema` (your dev database)
- But table references in the generated SQL use `--defer-database` instead (prod database)

**Example:**
```bash
# Extract metadata to SCRATCH
sst extract --db SCRATCH --schema my_test

# Generate views that reference ANALYTICS tables
sst generate \
  --metadata-db SCRATCH \
  --metadata-schema my_test \
  --target-db SCRATCH \
  --target-schema my_test \
  --defer-database ANALYTICS \
  --all
```

Generated view will contain:
```sql
CREATE SEMANTIC VIEW scratch.my_test.my_view AS
  SELECT ... FROM analytics.memberships.users  -- Uses ANALYTICS (prod)
```

Instead of:
```sql
CREATE SEMANTIC VIEW scratch.my_test.my_view AS
  SELECT ... FROM scratch.memberships.users  -- Would fail - doesn't exist
```

#### Examples

```bash
# Generate both views and YAML models (default)
sst generate \
  --metadata-db PROD_DB \
  --metadata-schema SM_METADATA \
  --target-db PROD_DB \
  --target-schema SEMANTIC_VIEWS \
  --all

# Generate only Semantic Views for BI tools
sst generate \
  --metadata-db PROD_DB \
  --metadata-schema SM_METADATA \
  --target-db PROD_DB \
  --target-schema SEMANTIC_VIEWS \
  --output views \
  --all

# Generate only YAML models for AI/LLM tools
sst generate \
  --metadata-db PROD_DB \
  --metadata-schema SM_METADATA \
  --output models \
  --stage @AI_MODELS \
  --target-db DEV_DB \
  --target-schema AI \
  --all

# Local development: Generate views in SCRATCH that reference ANALYTICS tables (like dbt defer)
sst generate \
  --metadata-db SCRATCH \
  --metadata-schema luizzi_semantic_test \
  --target-db SCRATCH \
  --target-schema luizzi_semantic_test \
  --defer-database ANALYTICS \
  --output views \
  --all

# Generate specific views/models
sst generate \
  --metadata-db PROD_DB \
  --metadata-schema SM_METADATA \
  --target-db PROD \
  --target-schema SEMANTIC \
  -v customer_360 \
  -v all_orders

# Save YAML models locally for version control
sst generate \
  --metadata-db PROD_DB \
  --metadata-schema SM_METADATA \
  --output models \
  --save-locally \
  --output-dir ./semantic_models \
  --all

# Dry run to preview
sst generate \
  --metadata-db DEV \
  --metadata-schema METADATA \
  --target-db TEST \
  --target-schema TEMP \
  --all \
  --dry-run
```

**Output Types:**
- `views`: Creates SQL SEMANTIC VIEWs in Snowflake (for BI tools)
- `models`: Generates YAML files with sample values (for Cortex Analyst/AI)
- `both`: Creates both formats (recommended for production)

**Cortex Analyst Metadata:**
When generating semantic views, SST automatically includes a `WITH EXTENSION (CA='...')` clause containing:
- `sample_values` for dimensions, time_dimensions, and facts
- `is_enum: true` for columns where sample_values is exhaustive

This metadata helps Cortex Analyst understand valid categorical values and generate more accurate queries.

*Note: The CA extension is a temporary solution recommended by Snowflake Engineering until native `sample_values` support is added to the DDL syntax.*

---

### deploy

**One-step deployment:** Orchestrates the complete workflow (validate → extract → generate) in a single command.

```bash
sst deploy [OPTIONS]
```

#### Options

| Option | Short | Type | Required | Default | Description |
|--------|-------|------|----------|---------|-------------|
| `--db` | | TEXT | **Yes** | | Target database (used for both extraction and generation) |
| `--schema` | `-s` | TEXT | **Yes** | | Target schema (used for both extraction and generation) |
| `--output` | | CHOICE | No | all | What to generate: `views`, `models`, or `all` |
| `--skip-validation` | | FLAG | No | False | Skip validation step (use when validation already run separately) |
| `--save-models-locally` | | FLAG | No | False | Save generated YAML models to local directory |
| `--models-dir` | | PATH | No | | Local directory for YAML models (requires --save-models-locally) |
| `--account` | | TEXT | No | $SNOWFLAKE_ACCOUNT | Snowflake account |
| `--user` | `-u` | TEXT | No | $SNOWFLAKE_USER | Snowflake user |
| `--role` | | TEXT | No | $SNOWFLAKE_ROLE | Snowflake role |
| `--warehouse` | `-w` | TEXT | No | $SNOWFLAKE_WAREHOUSE | Snowflake warehouse |
| `--verbose` | `-v` | FLAG | No | False | Show detailed progress |
| `--quiet` | `-q` | FLAG | No | False | Show errors and warnings only |

#### What It Does

The deploy command executes three steps in sequence:

1. **Validate** - Checks semantic models for errors (unless `--skip-validation`)
2. **Extract** - Parses YAML and loads metadata to Snowflake
3. **Generate** - Creates semantic views and/or YAML models

**Stops at first failure** - If validation fails, extraction is skipped. If extraction fails, generation is skipped.

#### Examples

```bash
# Most common: full deployment from current directory
sst deploy --db QA_DB --schema SEMANTIC_VIEWS

# Production deployment (validation already run separately)
sst deploy --db PROD_DB --schema SEMANTIC_VIEWS --skip-validation

# Deploy only semantic views (no YAML models)
sst deploy --db PROD_DB --schema VIEWS --output views

# Deploy with local YAML model save
sst deploy --db PROD_DB --schema SEMANTIC --save-models-locally --models-dir ./output

# Quiet mode (errors only)
sst deploy --db PROD_DB --schema SEMANTIC --quiet
```

#### Output Format

```
Step 1/3: Validating semantic models...
Validation passed (0 errors, 5 warnings)

Step 2/3: Extracting metadata to Snowflake...
Extraction completed (1,188 models)

Step 3/3: Generating semantic artifacts...
Generated 3 views

Deployment completed successfully in 45.2s
```

#### Workflow

```
┌─────────────────┐
│    VALIDATE     │  Checks for errors (fail fast)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    EXTRACT      │  Loads metadata to Snowflake
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    GENERATE     │  Creates semantic views/models
└─────────────────┘
```

**Best Practice:** Use `deploy` for convenience in QA/dev environments. For production, run commands separately for better debugging and control.

---

## Command Quick Reference

```bash
# Show configuration and test connection
sst debug
sst debug --test-connection

# Validate semantic models (run from dbt project root)
sst validate

# Enrich dbt models with metadata
sst enrich models/ --database PROD_DB --schema my_schema

# Format YAML files
sst format models/

# Extract metadata to Snowflake (run from dbt project root)
sst extract --db PROD_DB --schema SEMANTIC_METADATA

# Generate semantic views
sst generate --metadata-db PROD_DB --metadata-schema SEMANTIC_METADATA \
             --target-db PROD_DB --target-schema SEMANTIC_VIEWS --all

# One-step deployment (validate → extract → generate)
sst deploy --db PROD_DB --schema SEMANTIC_VIEWS
```

## Next Steps

- [Getting Started](getting-started.md) - Quick setup guide
- [Enrichment Guide](user-guide.md) - Metadata enrichment deep dive
- [Formatting Guide](cli-reference.md) - YAML formatting guide
- [Semantic Models Guide](semantic-models-guide.md) - Learn to write models
- [Validation Guide](user-guide.md) - Deep dive into validation
- [Deployment Guide](cli-reference.md) - automation setup
- [Authentication Guide](authentication.md) - Snowflake authentication options