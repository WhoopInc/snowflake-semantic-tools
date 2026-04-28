# Expected Outputs — sst-jaffle-shop (complete-project branch)

Baseline counts for the `complete-project` branch. The E2E skill uses these to detect regressions. Update this file when the jaffle-shop project changes.

## dbt Models

| Layer   | Count | Models |
|---------|-------|--------|
| Staging | 6     | stg_customers, stg_locations, stg_order_items, stg_orders, stg_products, stg_supplies |
| Marts   | 7     | customers, locations, metricflow_time_spine, order_items, orders, products, supplies |
| Total   | 13    | |

## Semantic Model Components

| Component            | Count | Notes |
|----------------------|-------|-------|
| Metrics              | 38    | Across orders, customers, order_items, products, locations, supplies |
| Relationships        | 5     | See details below |
| Semantic Views       | 3     | See details below |
| Custom Instructions  | 2     | business_rules, menu_analytics_guidance |
| Filters              | 0     | Only commented examples exist |
| Verified Queries     | 0     | Only commented examples exist |

## Relationships

| Name                    | Left Table   | Right Table | Type        |
|-------------------------|-------------|-------------|-------------|
| orders_to_customers     | orders      | customers   | many_to_one |
| orders_to_locations     | orders      | locations   | many_to_one |
| order_items_to_orders   | order_items | orders      | many_to_one |
| order_items_to_products | order_items | products    | many_to_one |
| supplies_to_products    | supplies    | products    | many_to_one |

## Semantic Views

| Name                          | Description |
|-------------------------------|-------------|
| jaffle_shop_sales_analytics   | Sales performance analysis |
| jaffle_shop_menu_analytics    | Menu and product analysis |
| jaffle_shop_complete          | Complete view across all tables |

### Expected View Structure (for dry-run SQL comparison)

#### jaffle_shop_sales_analytics
- **Tables**: ORDERS, CUSTOMERS, LOCATIONS
- **Relationships**: ORDERS_TO_CUSTOMERS, ORDERS_TO_LOCATIONS
- **Metrics**: TOTAL_ORDERS, TOTAL_REVENUE, AVERAGE_ORDER_VALUE, TOTAL_CUSTOMERS, and order/customer metrics
- **Custom Instructions**: 2 (AI_SQL_GENERATION, AI_QUESTION_CATEGORIZATION)

#### jaffle_shop_menu_analytics
- **Tables**: ORDER_ITEMS, PRODUCTS, SUPPLIES
- **Relationships**: ORDER_ITEMS_TO_PRODUCTS, SUPPLIES_TO_PRODUCTS
- **Metrics**: TOTAL_ORDER_ITEMS, TOTAL_PRODUCT_REVENUE, FOOD_ITEMS_SOLD, DRINK_ITEMS_SOLD, and product metrics
- **Custom Instructions**: 1 (AI_SQL_GENERATION)

#### jaffle_shop_complete
- **Tables**: ORDERS, ORDER_ITEMS, CUSTOMERS, PRODUCTS, LOCATIONS, SUPPLIES (all 6)
- **Relationships**: All 5 relationships
- **Metrics**: All 38+ metrics
- **Custom Instructions**: 2 (AI_SQL_GENERATION, AI_QUESTION_CATEGORIZATION)

When comparing dry-run SQL output, verify:
1. Each view contains the expected TABLES clause with the correct table names
2. Each view contains the expected RELATIONSHIPS clause
3. Metrics are present and reference the correct tables
4. No unexpected tables or relationships appear (would indicate a regression in view generation)

## Validation

| Check              | Expected |
|--------------------|----------|
| Validation errors  | 0        |
| Validation warnings| Variable (acceptable) |

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

These 4 tests fail to collect (not fail to run). They are pre-existing and not caused by code changes. Track but do not block the pipeline on them.

## SM_* Metadata Tables (After Extract)

These tables should be populated in the target schema after `sst extract`:

- SM_TABLES
- SM_DIMENSIONS
- SM_FACTS
- SM_TIME_DIMENSIONS
- SM_METRICS
- SM_RELATIONSHIPS
- SM_SEMANTIC_VIEWS

## Semantic Model Files

| File Path | Contents |
|-----------|----------|
| snowflake_semantic_models/metrics/metrics.yml | 38 metrics |
| snowflake_semantic_models/relationships/relationships.yml | 5 relationships |
| snowflake_semantic_models/semantic_views.yml | 3 semantic views |
| snowflake_semantic_models/custom_instructions/jaffle_shop_instructions.yml | 2 instructions |
| snowflake_semantic_models/filters/_examples.yml | Commented examples only |
| snowflake_semantic_models/verified_queries/_examples.yml | Commented examples only |

## Known Warnings (Expected, Non-Blocking)

| Phase | Warning | Reason |
|-------|---------|--------|
| Extract | `Cortex Search setup failed: SM_TABLE_SUMMARIES does not exist` | Optional Cortex Search feature, not part of core pipeline |
| Validate | `Manifest may be outdated` | Manifest was compiled in a different session; pipe `1` to continue |
| Enrich | `Manifest compiled for 'None' target` | Manifest target doesn't match current target; safe to continue |
