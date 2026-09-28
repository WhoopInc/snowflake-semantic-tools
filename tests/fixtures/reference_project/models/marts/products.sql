-- ============================================================================
-- models/marts/products.sql
-- ============================================================================
-- The closed catalogue. Ten rows, which is what makes the eval dataset's
-- tier-4 hardcoded count legitimate rather than brittle (`D213`).
--
-- WHOOP SQL conventions, the same set `../staging/stg_orders.sql` documents:
-- import CTEs first, one per `source()` or `ref()`; UPPERCASE keywords, functions
-- and types; leading commas; tables always aliased; columns always prefixed with
-- the alias; explicit JOIN syntax.
--
-- NO DATA VALUES. Every literal below is structural -- a cast, a default or a
-- boundary -- never a member value. See `../README.md`.
-- ============================================================================

WITH source_products AS (

    SELECT *
    FROM {{ source('jaffle_service', 'products_raw') }}

)

SELECT
    p.product_id
    , p.product_name
    , p.product_type
    , p.list_price::NUMBER(38,2) AS list_price
FROM source_products AS p
