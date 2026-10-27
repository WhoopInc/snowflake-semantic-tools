-- ============================================================================
-- models/marts/pricing_periods.sql
-- ============================================================================
-- The range-join partner, and the reason `DISTINCT RANGE` is reachable. Its
-- `alias:` diverges from its model name, which is its second assertion.
--
-- The house SQL conventions, the same set `../staging/stg_orders.sql` documents:
-- import CTEs first, one per `source()` or `ref()`; UPPERCASE keywords, functions
-- and types; leading commas; tables always aliased; columns always prefixed with
-- the alias; explicit JOIN syntax.
--
-- NO DATA VALUES. Every literal below is structural -- a cast, a default or a
-- boundary -- never a member value. See `../README.md`.
-- ============================================================================

WITH source_pricing_periods AS (

    SELECT *
    FROM {{ source('jaffle_service', 'pricing_periods_raw') }}

)

SELECT
    pp.pricing_period_id
    , pp.period_name
    , pp.effective_start_at
    -- HALF-OPEN ON THE RIGHT is the whole reason a range join is safe here: a
    -- `BETWEEN` against a closed upper bound double-counts any order landing
    -- exactly on a boundary, and the boundary is the case that gets hit.
    , pp.effective_end_at
    , pp.base_price_multiplier::NUMBER(38,4) AS base_price_multiplier
    , pp.discount_pct::NUMBER(38,4)          AS discount_pct
FROM source_pricing_periods AS pp
