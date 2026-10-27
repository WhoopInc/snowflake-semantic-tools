-- ============================================================================
-- models/marts/orders.sql
-- ============================================================================
-- The busiest model in the fixture, and the only mart built on a staging
-- model rather than straight from source -- `stg_orders` already casts and renames.
--
-- The house SQL conventions, the same set `../staging/stg_orders.sql` documents:
-- import CTEs first, one per `source()` or `ref()`; UPPERCASE keywords, functions
-- and types; leading commas; tables always aliased; columns always prefixed with
-- the alias; explicit JOIN syntax.
--
-- NO DATA VALUES. Every literal below is structural -- a cast, a default or a
-- boundary -- never a member value. See `../README.md`.
-- ============================================================================

WITH stg_orders AS (

    SELECT *
    FROM {{ ref('stg_orders') }}

)

, order_item_totals AS (

    SELECT
        oi.order_id
        , SUM(oi.item_price) AS subtotal
    FROM {{ ref('order_items') }} AS oi
    GROUP BY ALL

)

, order_locations AS (

    SELECT
        l.location_id
        , l.tax_rate
    FROM {{ ref('locations') }} AS l

)

SELECT
    o.order_id
    , o.customer_id
    , o.location_id
    , o.ordered_at
    , o.order_state
    , COALESCE(oit.subtotal, 0)::NUMBER(38,2) AS subtotal
    -- Tax is DERIVED here rather than carried from source, so the fixture has a
    -- fact whose value no raw relation supplies -- which is the case a semantic
    -- view's `facts:` block has to be able to express.
    , (COALESCE(oit.subtotal, 0) * COALESCE(ol.tax_rate, 0))::NUMBER(38,2) AS tax_paid
    , (COALESCE(oit.subtotal, 0) * (1 + COALESCE(ol.tax_rate, 0)))::NUMBER(38,2) AS order_total
FROM stg_orders AS o
LEFT JOIN order_item_totals AS oit
    ON o.order_id = oit.order_id
LEFT JOIN order_locations AS ol
    ON o.location_id = ol.location_id
