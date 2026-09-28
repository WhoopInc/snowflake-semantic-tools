-- ============================================================================
-- models/marts/supplies.sql
-- ============================================================================
-- The snapshot grain. `snapshot_month` is what makes a non-additive metric
-- authorable, and non-additivity is unreachable without a snapshot table.
--
-- WHOOP SQL conventions, the same set `../staging/stg_orders.sql` documents:
-- import CTEs first, one per `source()` or `ref()`; UPPERCASE keywords, functions
-- and types; leading commas; tables always aliased; columns always prefixed with
-- the alias; explicit JOIN syntax.
--
-- NO DATA VALUES. Every literal below is structural -- a cast, a default or a
-- boundary -- never a member value. See `../README.md`.
-- ============================================================================

WITH source_supplies AS (

    SELECT *
    FROM {{ source('jaffle_service', 'supplies_raw') }}

)

SELECT
    s.supply_id
    , s.product_id
    -- A SNAPSHOT COLUMN, so this table restates its whole population every month.
    -- Summing `supply_cost` across months therefore double-counts, which is
    -- exactly why `total_supply_cost` declares a `non_additive_dimensions` entry
    -- on this column.
    , s.snapshot_month
    , s.is_perishable
    , s.supply_cost::NUMBER(38,2) AS supply_cost
FROM source_supplies AS s
