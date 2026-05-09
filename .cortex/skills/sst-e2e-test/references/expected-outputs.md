# Expected Outputs — sst-jaffle-shop (main branch)

Baseline counts for the `main` branch. The E2E skill uses these to detect regressions. Update this file when the jaffle-shop project changes.

## dbt Models

| Layer   | Count | Models |
|---------|-------|--------|
| Staging | 7     | stg_customers, stg_locations, stg_order_items, stg_orders, stg_pricing_periods, stg_products, stg_supplies |
| Marts   | 8     | customers, locations, metricflow_time_spine, order_items, orders, pricing_periods, products, supplies |
| Total   | 15    | |

Note: `sst validate` reports 16 models because it also counts the _error_examples.yml fixtures.

## Semantic Model Components (Positive Test Cases Only)

| Component            | Count | Notes |
|----------------------|-------|-------|
| Metrics              | 80+   | Across all 6 core tables + advanced (window, composition, non_additive, etc.) |
| Relationships        | 8     | Standard (5) + ASOF (1) + Range/BETWEEN EXCLUSIVE (1) + Composite (1) |
| Semantic Views       | 3     | Sales analytics, menu analytics, complete |
| Custom Instructions  | 2     | business_rules, menu_analytics_guidance |
| Filters              | 6     | Equality, date range, numeric, boolean, multi-table, legacy syntax |
| Verified Queries     | 9     | Inline SQL (6) + sql_file (3), with descriptions and onboarding flags |

## Error Fixtures (Intentional — Expected to Fail Validation)

| File | Error Codes Exercised |
|------|----------------------|
| `models/marts/_error_examples.yml` | V007, V008, V010, V011, V012, V013, V014, V015, V016, V020, V023, V024, V025 |
| `snowflake_semantic_models/metrics/_error_examples.yml` | V002, V003, V004, V032, V033, V034, V035, V036, V037, V038, V044, V091 |
| `snowflake_semantic_models/relationships/_error_examples.yml` | V040, V041, V043 |
| `snowflake_semantic_models/filters/_error_examples.yml` | V051 |
| `snowflake_semantic_models/verified_queries/_error_examples.yml` | V060, V061, V062 |
| `snowflake_semantic_models/_error_semantic_views.yml` | V070, V071 |

These files are EXPECTED to produce validation errors. The test passes if:
- Errors come ONLY from `_error_examples.yml` or `_error_semantic_views.yml` files
- Zero errors/warnings come from non-error files

## Relationships

| Name                       | Left Table   | Right Table     | Type              |
|----------------------------|-------------|-----------------|-------------------|
| orders_to_customers        | orders      | customers       | Standard equality |
| orders_to_locations        | orders      | locations       | Standard equality |
| order_items_to_orders      | order_items | orders          | Standard equality |
| order_items_to_products    | order_items | products        | Standard equality |
| supplies_to_products       | supplies    | products        | Standard equality |
| orders_to_locations_asof   | orders      | locations       | ASOF (temporal)   |
| orders_to_pricing_periods  | orders      | pricing_periods | Range/BETWEEN EXCLUSIVE |
| order_items_composite_join | order_items | orders          | Composite (multi-column) |

## Semantic Views

| Name                          | Tables | Custom Instructions |
|-------------------------------|--------|---------------------|
| jaffle_shop_sales_analytics   | orders, customers, locations | jaffle_shop_business_rules |
| jaffle_shop_menu_analytics    | order_items, products, supplies | menu_analytics_guidance |
| jaffle_shop_complete          | orders, order_items, customers, products, locations, supplies, pricing_periods | jaffle_shop_business_rules, menu_analytics_guidance |

## Validation (Expected)

| Check              | Expected |
|--------------------|----------|
| Errors from non-error files | 0 |
| Warnings from non-error files | 0 |
| Errors from _error_examples files | 20+ (intentional) |
| Warnings from _error_examples files | 10+ (intentional) |

## Unit Tests (SST Repo)

| Metric        | Expected |
|---------------|----------|
| Total tests   | ~1191    |
| Failures      | 0        |
| Errors        | 0        |

### Known Test Issues

| Test Class | Count | Issue | Blocking? |
|------------|-------|-------|-----------|
| TestDeferManifestIntegration | 4 | Collection errors (fixture/import issue) | No |

## SM_* Metadata Tables (After Extract)

These tables should be populated in the target schema after `sst extract`:

- SM_TABLES
- SM_DIMENSIONS
- SM_FACTS
- SM_TIME_DIMENSIONS
- SM_METRICS
- SM_RELATIONSHIPS
- SM_RELATIONSHIP_COLUMNS
- SM_SEMANTIC_VIEWS
- SM_CUSTOM_INSTRUCTIONS
- SM_FILTERS
- SM_VERIFIED_QUERIES
- SM_TABLE_SUMMARIES (optional, Cortex Search)

## Feature Coverage Exercised

| Category | Features |
|----------|----------|
| Templates | `{{ ref() }}`, `{{ ref(table, col) }}`, `{{ table() }}`, `{{ column() }}`, `{{ metric() }}`, `{{ custom_instructions() }}` |
| Metric types | SUM, COUNT, COUNT DISTINCT, AVG, MIN, MAX, CASE WHEN, PERCENTILE_CONT, VARIANCE, DATEDIFF, compound, window, non_additive_by, using_relationships, metric composition, visibility: private |
| Column types | dimension, time_dimension, fact |
| Data types | TEXT, NUMBER, FLOAT, BOOLEAN, DATE, TIMESTAMP_NTZ |
| Table features | primary_key, unique_keys (single + multi-column), constraints (DISTINCT_RANGE), tags, synonyms, cortex_searchable, visibility |
| Column features | data_type, synonyms, sample_values, is_enum, visibility, tags, cortex_searchable |
| VQR features | inline sql, sql_file, description, use_as_onboarding_question, verified_by, verified_at, multi-table |
| Filter features | equality, date range, numeric, boolean, synonyms, legacy syntax |
| Relationship features | standard equality, ASOF, range/BETWEEN EXCLUSIVE, composite/multi-column, description |

## Known Warnings (Expected, Non-Blocking)

| Phase | Warning | Reason |
|-------|---------|--------|
| Extract | `Cortex Search setup failed: SM_TABLE_SUMMARIES does not exist` | Optional Cortex Search feature, not part of core pipeline |
| Validate | `Manifest may be outdated` | Manifest was compiled in a different session; pipe `1` to continue |
| Enrich | `Manifest compiled for 'None' target` | Manifest target doesn't match current target; safe to continue |
