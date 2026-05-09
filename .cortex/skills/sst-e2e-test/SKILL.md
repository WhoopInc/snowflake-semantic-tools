---
name: sst-e2e-test
description: "End-to-end testing for SST against sst-jaffle-shop. Runs unit tests, validate, enrich, extract, generate, and Snowflake verification. Use when: testing SST changes, QA before release, verifying pipeline. Triggers: e2e test, run tests, test sst, qa sst, integration test, full test, test pipeline."
---

# SST End-to-End Testing

## Purpose

This skill validates that SST (snowflake-semantic-tools) works correctly after code changes. It uses [sst-jaffle-shop](https://github.com/WhoopInc/sst-jaffle-shop) — a comprehensive dbt project with 15 models, 80+ metrics, 8 relationships, 6 filters, 9 verified queries, and 3 semantic views — as a reference integration test. The project also includes intentional error fixtures (`_error_examples.yml` files) that exercise every SST validation error code.

If SST can successfully validate, enrich, extract, and generate semantic views from this project, the code is working.

Use this skill when:
- You've made changes to SST and want to verify nothing broke
- You're preparing a release or PR and need QA
- You want to confirm the full pipeline works end-to-end against a real dbt project

The skill walks through 6 phases sequentially. Each phase has pass/fail criteria. If a phase fails, report the failure and do NOT continue to dependent phases.

Read `references/expected-outputs.md` for baseline counts and `references/setup-guide.md` for environment details before starting.

## Conventions

Throughout this skill, two directory variables are used:

- **SST_REPO**: The root of this repository (snowflake-semantic-tools). Determined by finding the directory containing `pyproject.toml` and `snowflake_semantic_tools/`. This is typically the current working directory.
- **JAFFLE_SHOP_DIR**: The sst-jaffle-shop test project. The skill will locate or clone it during Phase 0.

All commands should use absolute paths derived from these variables. Never hardcode user-specific paths.

## Prerequisites

These steps prepare the environment. They are not part of the test itself.

### Locate SST Repo

Determine the SST repo root. If the current working directory contains `pyproject.toml` with `snowflake-semantic-tools`, use it. Otherwise, search parent directories.

```bash
# Verify we are in the SST repo
test -f ./pyproject.toml && grep -q "snowflake-semantic-tools" ./pyproject.toml
```

Store this as SST_REPO for all subsequent commands.

### Python Environment

SST requires Python 3.11+. The agent should detect the active Python environment:

1. Check if `sst` is already on PATH: `which sst`
2. If not, install SST in editable mode from the repo root:
   ```bash
   pip install -e "$SST_REPO"
   ```
3. Verify: `sst --version` — should return a version string (e.g., `0.3.0`)

If the user has a conda environment or virtualenv they prefer, ask them. Do NOT assume a specific conda path or environment name.

### sst-jaffle-shop

Locate or clone the test project:

1. Check if it exists as a sibling directory:
   ```bash
   test -f "$SST_REPO/../sst-jaffle-shop/dbt_project.yml"
   ```
2. If not found, clone it next to the SST repo:
   ```bash
   git clone https://github.com/WhoopInc/sst-jaffle-shop.git "$SST_REPO/../sst-jaffle-shop"
   ```
3. Ensure the `main` branch is checked out and up to date:
   ```bash
   cd "$JAFFLE_SHOP_DIR"
   git checkout main
   git pull origin main
   ```

Store the resolved path as JAFFLE_SHOP_DIR.

### dbt Setup

For Snowflake-connected phases, the dbt models must be materialized in the target schema. Check if tables exist:

```sql
SELECT TABLE_NAME FROM <DB>.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = '<SCHEMA>' AND TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME;
```

Expected mart tables (8): CUSTOMERS, LOCATIONS, METRICFLOW_TIME_SPINE, ORDER_ITEMS, ORDERS, PRICING_PERIODS, PRODUCTS, SUPPLIES.

If the schema is empty (or doesn't exist), the dbt project needs to be built first.

**⚠️ MANDATORY STOPPING POINT**: Ask the user how they want to proceed:

1. **Build now** — the agent runs `dbt deps`, `dbt seed`, and `dbt build` in the jaffle-shop project against the chosen target. This creates seed tables in a `raw` sub-schema and 15 models in the target schema. It will incur Snowflake compute costs.
2. **Skip Snowflake phases** — run only the offline phases (unit tests + validate) and skip enrich, extract, generate, and Snowflake verification.
3. **Point to an existing schema** — the user already has the jaffle-shop tables in a different database/schema and wants to use that instead.

If the user chooses option 1, run:
```bash
cd "$JAFFLE_SHOP_DIR"
dbt deps
dbt seed --vars '{"load_source_data": true}' --full-refresh
dbt build
```

Verify all 15 models pass. If any fail, report the errors before continuing.

Critical notes:
- `dbt deps` MUST run before `dbt build` — `stg_supplies` uses `dbt_utils` macros and will fail without it
- Seeds require `--vars '{"load_source_data": true}'` because they are disabled by default in `dbt_project.yml`
- Seeds load into a `raw` sub-schema (configured in dbt_project.yml `seeds.+schema: raw`)
- The `dbt_project.yml` profile is `sst_jaffle_shop` — this must exist in `~/.dbt/profiles.yml` with valid Snowflake credentials

If the schema already has tables, verify 8 mart tables exist. If there are fewer, ask the user if they want to re-run `dbt build` to refresh.

### dbt Target

**⚠️ MANDATORY STOPPING POINT**: Ask the user which dbt target to use for Snowflake-connected phases (enrich, extract, generate). Default suggestion: `dev`.

Store the target for use in subsequent phases.

### Prerequisites Pass Criteria
- `sst --version` succeeds and shows expected version
- sst-jaffle-shop is on `main` branch and up to date
- User has confirmed a dbt target
- If running Snowflake phases: 8 mart tables exist in target schema (or user chose to skip)

---

## Test Phase 1: Unit Tests (SST Repo)

Run the full unit test suite in the SST repository:

```bash
cd "$SST_REPO"
python -m pytest tests/unit/ -v --tb=short 2>&1 | tail -30
```

Parse the output for:
- Total tests collected
- Passed count
- Failed count
- Error count

### Pass Criteria
- 0 failures
- 0 errors
- Total passed should be close to the baseline in `expected-outputs.md` (currently ~1191)

### Known Issues
- `TestDeferManifestIntegration` (4 tests) may show collection errors — this is a pre-existing fixture/import issue, not a regression. Track but do not block on these.

### On Failure
Report which tests failed with their error messages. Distinguish between:
- **Collection errors** (ERROR) — tests that can't load, usually fixture issues
- **Failures** (FAILED) — tests that ran and produced wrong results, these are real bugs

Do NOT continue to Phase 2 if there are new failures beyond the known issues above.

---

## Test Phase 2: Validate

Run semantic model validation in the jaffle-shop project. This validates the ENTIRE project including the intentional error fixtures.

```bash
cd "$JAFFLE_SHOP_DIR"
echo "1" | sst validate --target <TARGET>
```

If running offline only (no Snowflake connection), use `--no-snowflake-check`:
```bash
echo "1" | sst validate --no-snowflake-check
```

Note: `sst validate` may prompt interactively if the dbt manifest is stale. Pipe `1` to choose "continue with current manifest".

### Interpreting Results

The project INTENTIONALLY contains error fixtures. The validation output will show errors — this is expected. The key distinction:

- **Errors from `_error_examples.yml` or `_error_semantic_views.yml` files** → EXPECTED (pass)
- **Errors from any other file** → UNEXPECTED (fail)

To check this, grep the validation output:
```bash
sst validate 2>&1 | grep -A1 "^error" | grep "\-\->" | grep -v "_error_examples\|_error_semantic"
```

If this produces NO output, all real models pass cleanly.

Similarly for warnings:
```bash
sst validate 2>&1 | grep -A1 "^warning" | grep "\-\->" | grep -v "_error_examples\|_error_semantic"
```

### Pass Criteria
- 0 errors from non-error fixture files
- 0 warnings from non-error fixture files
- 20+ errors from _error_examples files (intentional — proves error detection works)
- Exit code 1 (because error fixtures exist)

### On Failure
If errors appear from non-error files, report each one. These indicate either:
- A bug in SST's validation logic
- A regression in the jaffle-shop positive test cases
- A false positive in a validation rule

---

## Test Phase 3: Enrich (Requires Snowflake)

Run metadata enrichment against the Snowflake target. This modifies YAML files in the jaffle-shop project.

```bash
cd "$JAFFLE_SHOP_DIR"
echo "1" | sst enrich models/ --target <TARGET>
```

Replace `<TARGET>` with the user's chosen dbt target from the Prerequisites section.

After enrichment completes, show the diff:
```bash
git diff --stat
```

### Pass Criteria
- Enrichment command exits with code 0
- No Python tracebacks or unhandled exceptions in output
- Existing `meta.sst` annotations are preserved (not clobbered)

**⚠️ MANDATORY STOPPING POINT**: Do NOT proceed until user responds.

Show the user the `git diff --stat` output. Ask:
1. Do the changes look reasonable?
2. Should we reset changes before proceeding? (`git checkout .`)
3. Should we proceed to extract/generate?

Default for automated QA: reset with `git checkout .` after confirming enrich succeeded, then proceed.

NEVER proceed without user confirmation.

---

## Test Phase 4: Extract + Generate (Requires Snowflake)

### 4a. Extract

Load semantic metadata to Snowflake SM_* tables:

```bash
cd "$JAFFLE_SHOP_DIR"
echo "1" | sst extract --target <TARGET>
```

Note: Extract may also prompt about stale manifests — pipe `1` to continue.

### Known Warning
Extract may show: `Cortex Search setup failed: Object 'SM_TABLE_SUMMARIES' does not exist`. This is expected and non-blocking — it's an optional Cortex Search feature that requires a separate setup step not part of the core pipeline.

### 4b. Generate (Dry Run)

Preview the SQL that would be generated:

```bash
cd "$JAFFLE_SHOP_DIR"
echo "1" | sst generate --all --target <TARGET> --dry-run 2>&1 | tee /tmp/sst-dry-run-output.txt
```

Compare the dry-run output against `references/expected-outputs.md` baselines:
- Verify all 3 expected semantic views appear in the output
- Verify each view references the correct tables
- Verify metrics, dimensions, and facts are present for each view
- Verify relationships are included
- If the output differs significantly from expectations, flag it as a potential regression

**⚠️ MANDATORY STOPPING POINT**: Do NOT proceed until user responds.

Show the user the dry-run output (or a summary). Ask if they want to proceed with actual generation.

### 4c. Generate (Execute)

If approved:
```bash
cd "$JAFFLE_SHOP_DIR"
echo "1" | sst generate --all --target <TARGET>
```

### Pass Criteria
- Extract exits with code 0 (warnings about Cortex Search are acceptable)
- Generate creates all 3 semantic views
- Dry-run SQL contains expected tables, relationships, and metrics per view
- No SQL syntax errors or Snowflake errors during generation

### On Failure
Report the exact error. Common issues:
- Missing SM_* tables (extract didn't run or failed silently)
- SQL syntax errors in generated DDL (SST generation bug)
- Permission errors (wrong role/warehouse for the target)
- Stale manifest prompt hanging (forgot to pipe input)

---

## Test Phase 5: Snowflake Verification

Verify the generated semantic views exist and are valid in Snowflake.

### 5a. List Views

```sql
SHOW SEMANTIC VIEWS IN <DB>.<SCHEMA>;
```

### 5b. Describe Each View

For each expected semantic view:
```sql
DESCRIBE SEMANTIC VIEW <DB>.<SCHEMA>.<VIEW_NAME>;
```

Expected views:
- `JAFFLE_SHOP_SALES_ANALYTICS`
- `JAFFLE_SHOP_MENU_ANALYTICS`
- `JAFFLE_SHOP_COMPLETE`

### 5c. Cross-Reference

Verify:
- All views from `semantic_views.yml` exist in Snowflake
- Each view's DESCRIBE output contains the expected tables and metrics
- No orphaned views (views in Snowflake not in YAML) — note any for cleanup

### Pass Criteria
- All 3 expected semantic views exist
- All views are describable (no broken references)

---

## Test Phase 6: Summary Report

Compile results from all test phases into a summary:

```
Phase                 | Status | Details
----------------------------------------------
1. Unit Tests         | ...    | X/Y passed
2. Validate           | ...    | 0 errors on real models, N intentional errors caught
3. Enrich             | ...    | exit code 0, N files modified
4. Extract + Generate | ...    | 3 semantic views created
5. Snowflake Verify   | ...    | 3 views confirmed
----------------------------------------------
Overall: PASS / FAIL
```

If any phase failed:
- List the specific failures
- Suggest next steps (which code to investigate, which tests to write)

If all phases passed:
- Confirm the SST pipeline is working end-to-end
- Note any warnings or unexpected counts for follow-up

## Success Criteria

- All phases report PASS (or PASS with known caveats)
- 0 new test failures beyond known issues
- 0 validation errors on non-error fixture files
- All 3 semantic views created and verified in Snowflake
- No unhandled exceptions or tracebacks in any SST command

## Output

A phase-by-phase summary report (Phase 6) with PASS/FAIL status for each phase, overall result, and actionable next steps if any phase failed.

## Stopping Points

- ✋ Prerequisites: Before building dbt models (Snowflake compute costs)
- ✋ Prerequisites: Before proceeding without a confirmed dbt target
- ✋ Phase 3: After enrich, before proceeding (review git diff)
- ✋ Phase 4b: After generate dry-run, before executing DDL

## Troubleshooting

**`sst validate` hangs or times out**
- The manifest is stale and SST is prompting interactively. Pipe `1` to continue, or use `--dbt-compile` to refresh.

**`dbt build` fails on `stg_supplies`**
- Run `dbt deps` first. The model uses `dbt_utils` macros.

**`sst extract` shows Cortex Search error**
- `SM_TABLE_SUMMARIES does not exist` is expected and non-blocking. Ignore it.

**`sst enrich` shows "Manifest compiled for 'None' target"**
- This is a warning, not an error. The manifest was compiled without a target flag. Safe to continue.

**Permission errors during generate**
- Verify the dbt profile role has CREATE SEMANTIC VIEW privileges on the target schema.

**`sst --version` shows wrong version**
- Re-run `pip install -e .` from the SST repo root to ensure the dev branch is active.

**Validation shows errors but they're all from `_error_examples.yml`**
- This is EXPECTED. The error fixtures intentionally trigger validation errors. Only worry about errors from other files.
