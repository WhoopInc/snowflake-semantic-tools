-- ============================================================================
-- models/marts/customers.sql
-- ============================================================================
-- One row per customer. Reads the raw relation directly: there is no
-- `stg_customers`, because the only cleaning this model needs is the aggregation
-- below and a staging layer that does nothing is a file to maintain for no gain.
--
-- WHOOP SQL conventions, the same set `../staging/stg_orders.sql` documents:
-- import CTEs first, one per `source()` or `ref()`; UPPERCASE keywords, functions
-- and types; leading commas; tables always aliased; columns always prefixed with
-- the alias; explicit JOIN syntax.
--
-- NO DATA VALUES. Every literal below is structural -- a cast, a default or a
-- boundary -- never a member value. See `../README.md`.
-- ============================================================================

WITH source_customers AS (

    SELECT *
    FROM {{ source('jaffle_service', 'customers_raw') }}

)

, source_orders AS (

    SELECT *
    FROM {{ ref('orders') }}

)

, customer_orders AS (

    SELECT
        o.customer_id
        , MIN(o.ordered_at)   AS first_ordered_at
        , SUM(o.order_total)  AS lifetime_spend
    FROM source_orders AS o
    GROUP BY ALL

)

SELECT
    c.customer_id
    , c.customer_name
    , c.customer_type
    , co.first_ordered_at
    -- COALESCE, not a bare SUM: a customer with no orders must report zero spend
    -- rather than NULL, because `lifetime_spend` is declared a `fact` and a NULL
    -- fact aggregates differently from a zero one.
    , COALESCE(co.lifetime_spend, 0)::NUMBER(38,2) AS lifetime_spend
FROM source_customers AS c
LEFT JOIN customer_orders AS co
    ON c.customer_id = co.customer_id
