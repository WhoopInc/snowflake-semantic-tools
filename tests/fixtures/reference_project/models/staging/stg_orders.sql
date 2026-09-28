-- ============================================================================
-- models/staging/stg_orders.sql
-- ============================================================================
-- THE ONLY STAGING MODEL IN THIS FIXTURE, and the only model whose raw relation
-- needs casting and renaming before anything can join to it. The seven marts
-- each have their own .sql; six read source() directly and this one sits in
-- front of orders_raw. See models/staging/_sources.yml for that asymmetry.
--
-- IT IS THE ONE MODEL NO GOLDEN ASSERTS. Its output reaches the goldens only
-- through marts/orders.sql, so it is here to show the shape.
--
-- WHOOP SQL conventions, applied so the fixture does not teach a house style it
-- does not follow: import CTEs first, one per source() or ref(); UPPERCASE
-- keywords, functions and types; leading commas; tables always aliased; columns
-- always prefixed with the alias; explicit JOIN syntax; GROUP BY ALL; QUALIFY
-- rather than a subquery for window filtering.
--
-- NO DATA VALUES. There are no literals in this file other than the synthetic
-- state placeholder, which is named as such.
-- ============================================================================

WITH orders_raw AS (

    SELECT
        *
    FROM {{ source('jaffle_service', 'orders_raw') }}

)

, deduplicated AS (

    SELECT
          orders_raw.order_id                               AS order_id
        , orders_raw.customer_id                            AS customer_id
        , orders_raw.location_id                            AS location_id
        , orders_raw.order_state                            AS order_state
        , orders_raw.ordered_at                             AS ordered_at
        , orders_raw.subtotal                               AS subtotal
        , orders_raw.order_total                            AS order_total
        , orders_raw._replicated_at                         AS replicated_at
    FROM orders_raw AS orders_raw
    -- Keep the latest replicated row per order. QUALIFY, not a subquery.
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY orders_raw.order_id
        ORDER BY orders_raw._replicated_at DESC
    ) = 1

)

, final AS (

    SELECT
          deduplicated.order_id                             AS order_id
        , deduplicated.customer_id                          AS customer_id
        , deduplicated.location_id                          AS location_id
        , deduplicated.order_state                          AS order_state
        , deduplicated.ordered_at                           AS ordered_at
        , deduplicated.subtotal                             AS subtotal
        , deduplicated.order_total                          AS order_total
        --Derived--
        , deduplicated.order_total - deduplicated.subtotal  AS tax_paid_derived
        , COALESCE(deduplicated.order_state, 'STATE_PLACEHOLDER_UNKNOWN')
                                                            AS order_state_coalesced
    FROM deduplicated AS deduplicated

)

SELECT
    *
FROM final AS final
