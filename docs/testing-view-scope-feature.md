# Testing View-Level Scope Feature (PR #256)

## Environment Setup

```bash
# 1. Create/activate the conda environment
conda create -n sst-view-scope-test python=3.11 -y
conda activate sst-view-scope-test

# 2. Install the feature branch of SST
git clone -b mp_DATA-2983-prototype_sst_development \
  https://github.com/WhoopInc/snowflake-semantic-tools.git ~/sst-feature-test
cd ~/sst-feature-test
pip install -e .
pip install pytest pytest-mock

# 3. Verify installation
sst --version
# Expected: snowflake-semantic-tools, version 0.3.1

# 4. Run unit tests
pytest tests/unit/ --tb=short
# Expected: 1901 passed, 25 subtests passed
```

---

## Test 1: sst-jaffle-shop (12/12 views)

### Setup

```bash
cd ~/Documents/GitHub/sst-jaffle-shop
git fetch origin
git checkout feature/view-scope-test-fixtures
```

Add to `~/.dbt/profiles.yml`:

```yaml
sst_jaffle_shop:
  target: local
  outputs:
    local:
      type: snowflake
      account: <your-account>
      user: <your-user>
      authenticator: externalbrowser
      role: <your-role>
      warehouse: <your-warehouse>
      database: <your-database>
      schema: <your-schema>
      threads: 4
```

### Run

```bash
# Create schema: CREATE SCHEMA IF NOT EXISTS <database>.<schema>;
dbt run --target local
sst validate --dbt-compile --target local
sst compile --target local
sst generate --all --target local
```

### Expected: 12/12 CREATED, Status: SUCCESS

---

## Test 2: analytics-dbt (regression + 12 scope test cases)

### Setup

```bash
conda activate sst-view-scope-test
cd ~/Documents/GitHub/analytics-dbt/main
git pull origin main

# Create a test worktree
git worktree add ../test-view-scope -b test-view-scope
cd ../test-view-scope
```

### Step A: Regression check

```bash
sst validate --dbt-compile
```

**Expected:** `Errors: 0` — no regression on existing 67 views.

### Step B: Add 12 scope test views

Add the following to `snowflake_semantic_models/semantic_views/transactions.yml` at the end of the file:

```yaml
  # ===========================================================================
  # SCOPE TEST VIEWS (PR #256, issue #248)
  # ===========================================================================

  # Test 1: No scope (baseline — unchanged behavior)
  - name: scope_test_baseline
    description: Full view with no scope restrictions — confirms unchanged behavior
    tables:
      - {{ table('all_orders') }}
      - {{ table('single_customer_view') }}

  # Test 2: INCLUDE metrics only (allowlist)
  - name: scope_test_include_metrics
    description: Only DTC-relevant metrics appear
    tables:
      - {{ table('all_orders') }}
      - {{ table('single_customer_view') }}
    metrics:
      - "{{ metric('total_orders') }}"
      - "{{ metric('dtc_distinct_orders') }}"
      - "{{ metric('consumer_new_membership_orders') }}"
      - "{{ metric('trial_orders') }}"
    relationships:
      - all_orders_to_single_customer_view

  # Test 3: INCLUDE columns only (allowlist)
  - name: scope_test_include_columns
    description: Only key order columns appear as dimensions/facts
    tables:
      - {{ table('all_orders') }}
      - {{ table('single_customer_view') }}
    columns:
      - "{{ column('all_orders', 'order_id') }}"
      - "{{ column('all_orders', 'order_type') }}"
      - "{{ column('all_orders', 'order_source') }}"
      - "{{ column('all_orders', 'order_channel') }}"
      - "{{ column('all_orders', 'order_date') }}"
      - "{{ column('all_orders', 'quantity') }}"
      - "{{ column('single_customer_view', 'user_id') }}"
      - "{{ column('single_customer_view', 'country_code') }}"
    relationships:
      - all_orders_to_single_customer_view

  # Test 4: INCLUDE metrics + columns combined
  - name: scope_test_include_both
    description: Both metrics and columns scoped — focused DTC view
    tables:
      - {{ table('all_orders') }}
      - {{ table('single_customer_view') }}
    metrics:
      - "{{ metric('total_orders') }}"
      - "{{ metric('dtc_distinct_orders') }}"
    columns:
      - "{{ column('all_orders', 'order_id') }}"
      - "{{ column('all_orders', 'order_type') }}"
      - "{{ column('all_orders', 'order_date') }}"
      - "{{ column('all_orders', 'quantity') }}"
      - "{{ column('single_customer_view', 'user_id') }}"
      - "{{ column('single_customer_view', 'country_code') }}"
    relationships:
      - all_orders_to_single_customer_view

  # Test 5: INCLUDE relationships (allowlist)
  - name: scope_test_include_relationships
    description: Only specific join paths included
    tables:
      - {{ table('all_orders') }}
      - {{ table('single_customer_view') }}
      - {{ table('membership_status_daily') }}
    relationships:
      - all_orders_to_single_customer_view

  # Test 6: EXCLUDE columns (blocklist)
  - name: scope_test_exclude_columns
    description: Hide internal columns from view
    tables:
      - {{ table('all_orders') }}
      - {{ table('single_customer_view') }}
    exclude_columns:
      - "{{ column('all_orders', 'salesforce_order_status') }}"
      - "{{ column('all_orders', 'salesforce_opportunity_id') }}"
    relationships:
      - all_orders_to_single_customer_view

  # Test 8: EXCLUDE metrics (blocklist)
  - name: scope_test_exclude_metrics
    description: Remove specific metrics from view
    tables:
      - {{ table('all_orders') }}
      - {{ table('single_customer_view') }}
    exclude_metrics:
      - "{{ metric('trial_orders') }}"
      - "{{ metric('advanced_labs_orders') }}"
    relationships:
      - all_orders_to_single_customer_view

  # Test 9: EXCLUDE relationships
  - name: scope_test_exclude_relationships
    description: Remove membership_status_daily join path
    tables:
      - {{ table('all_orders') }}
      - {{ table('single_customer_view') }}
      - {{ table('membership_status_daily') }}
    exclude_relationships:
      - all_orders_to_membership_status_daily

  # Test 10: Single table with columns scope
  - name: scope_test_single_table
    description: Single table with only a few columns — tests auto-skip of incompatible metrics
    tables:
      - {{ table('all_orders') }}
    columns:
      - "{{ column('all_orders', 'order_id') }}"
      - "{{ column('all_orders', 'order_type') }}"
      - "{{ column('all_orders', 'order_date') }}"
      - "{{ column('all_orders', 'quantity') }}"
    metrics:
      - "{{ metric('total_orders') }}"

  # Test 12: Mixed include + exclude across different types
  - name: scope_test_mixed
    description: Include columns + exclude metrics in same view
    tables:
      - {{ table('all_orders') }}
      - {{ table('single_customer_view') }}
    columns:
      - "{{ column('all_orders', 'order_id') }}"
      - "{{ column('all_orders', 'order_type') }}"
      - "{{ column('all_orders', 'order_date') }}"
      - "{{ column('single_customer_view', 'user_id') }}"
    exclude_metrics:
      - "{{ metric('advanced_labs_orders') }}"
    relationships:
      - all_orders_to_single_customer_view
```

### Step C: Validate

```bash
sst validate --dbt-compile
```

**Expected:** `Errors: 0` — all 12 test views + existing 67 views pass validation.

### Step D: Generate

```bash
sst compile
sst generate --all
```

**Expected:** All new scope test views CREATED alongside existing views.

### Step E: Verify DDL content

```bash
# Check include_metrics only has 4 metrics
sst generate --views scope_test_include_metrics --dry-run
grep "AS SUM\|AS COUNT\|AS AVG\|AS MAX\|AS MIN" target/semantic_views/scope_test_include_metrics.sql | wc -l
# Expected: 4

# Check exclude_columns removes the specified columns from FACTS/DIMENSIONS
sst generate --views scope_test_exclude_columns --dry-run
grep -i "salesforce_order_status\|salesforce_opportunity_id" target/semantic_views/scope_test_exclude_columns.sql
# Expected: NOT in FACTS/DIMENSIONS (may still appear in metric expressions)
```

---

## Test 3: Error cases

### SST-V080: Both include and exclude for same type

```yaml
  - name: error_both_modes
    tables:
      - {{ table('all_orders') }}
    metrics:
      - "{{ metric('total_orders') }}"
    exclude_metrics:
      - "{{ metric('trial_orders') }}"
```

**Expected:**
```
error[SST-V080]: Semantic view 'error_both_modes' specifies both 'metrics' and
'exclude_metrics'. Choose one mode: include (allowlist) or exclude (blocklist), not both.
```

### SST-V082: Non-existent metric

```yaml
  - name: error_bad_metric
    tables:
      - {{ table('all_orders') }}
    metrics:
      - "{{ metric('nonexistent_metric_xyz') }}"
```

**Expected:**
```
error[SST-V082]: Semantic view 'error_bad_metric' references metric
'nonexistent_metric_xyz' in 'metrics' but no metric with that name exists.
```

### SST-V082: Non-existent column

```yaml
  - name: error_bad_column
    tables:
      - {{ table('all_orders') }}
    columns:
      - "{{ column('all_orders', 'this_column_does_not_exist') }}"
```

**Expected:**
```
error[SST-V082]: Semantic view 'error_bad_column' references column
'ALL_ORDERS.THIS_COLUMN_DOES_NOT_EXIST' in 'columns' but that column does not
exist in the view's tables.
```

### SST-V082: Globally excluded column in include (ERROR)

If a column has `meta.sst.exclude: true`:

**Expected:**
```
error[SST-V082]: ... may be globally excluded via meta.sst.exclude: true —
globally excluded columns cannot be re-included at the view level.
```

### SST-V082: Globally excluded column in exclude (WARNING, not error)

**Expected:**
```
warning[SST-V082]: ... already globally excluded via meta.sst.exclude: true.
This exclusion is redundant.
```

### SST-V083: Metric requires excluded relationship

**Expected:**
```
error[SST-V083]: ... includes metric 'X' which requires relationship 'Y'
(via using_relationships), but that relationship is not included in this view's
relationship scope.
```

### Builder: Incompatible window metric

**Expected (at generate time):**
```
Error building semantic view '...': A metric (TABLE.METRIC) cannot be compiled
by Snowflake in this view's configuration.

This typically happens when a window function or semi-additive metric references
dimensions from a related entity, but the required relationship or table is not
included in this view.

To fix this, either:
  1. Add the related table to the view's 'tables' list
  2. Exclude the problematic metric using 'exclude_metrics'
  3. Restrict metrics to only the ones you need using the 'metrics' include list
```

> Most incompatible metrics are auto-skipped. This only appears for edge cases.

---

## Cleanup

```bash
cd ~/Documents/GitHub/analytics-dbt/main
git worktree remove ../test-view-scope
git branch -D test-view-scope
```

---

## Validation Error Codes

| Code | Severity | Meaning |
|------|----------|---------|
| SST-V080 | ERROR | Both include and exclude specified for same type |
| SST-V082 | ERROR | Referenced item doesn't exist (or globally excluded in include) |
| SST-V082 | WARNING | Globally excluded column in `exclude_columns` (redundant) |
| SST-V083 | ERROR | Metric requires a relationship that's excluded |

---

## PR Link

https://github.com/WhoopInc/snowflake-semantic-tools/pull/256
