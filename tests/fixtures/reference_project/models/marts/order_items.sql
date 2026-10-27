-- ============================================================================
-- models/marts/order_items.sql
-- ============================================================================
-- The fan-out grain: one row per line item, so this is the model where a
-- naive join multiplies. `occurred_at` is what makes it the ASOF join's left side.
--
-- The house SQL conventions, the same set `../staging/stg_orders.sql` documents:
-- import CTEs first, one per `source()` or `ref()`; UPPERCASE keywords, functions
-- and types; leading commas; tables always aliased; columns always prefixed with
-- the alias; explicit JOIN syntax.
--
-- NO DATA VALUES. Every literal below is structural -- a cast, a default or a
-- boundary -- never a member value. See `../README.md`.
-- ============================================================================

WITH source_order_items AS (

    SELECT *
    FROM {{ source('jaffle_service', 'order_items_raw') }}

)

, source_products AS (

    SELECT *
    FROM {{ ref('products') }}

)

SELECT
    oi.order_item_id
    , oi.order_id
    , oi.product_id
    , oi.occurred_at
    -- The PRICE AT THE TIME OF THE ORDER, which is why this is a column on the
    -- line item and not a lookup through to `products.list_price`. A product's
    -- list price changes; what a line item cost does not.
    , COALESCE(oi.item_price, p.list_price)::NUMBER(38,2) AS item_price
FROM source_order_items AS oi
LEFT JOIN source_products AS p
    ON oi.product_id = p.product_id
