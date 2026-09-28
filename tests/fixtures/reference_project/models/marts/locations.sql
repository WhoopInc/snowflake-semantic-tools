-- ============================================================================
-- models/marts/locations.sql
-- ============================================================================
-- The dimension-only model: no fact of its own is aggregated here, and no
-- metric is declared on it. It exists to be joined.
--
-- WHOOP SQL conventions, the same set `../staging/stg_orders.sql` documents:
-- import CTEs first, one per `source()` or `ref()`; UPPERCASE keywords, functions
-- and types; leading commas; tables always aliased; columns always prefixed with
-- the alias; explicit JOIN syntax.
--
-- NO DATA VALUES. Every literal below is structural -- a cast, a default or a
-- boundary -- never a member value. See `../README.md`.
-- ============================================================================

WITH source_locations AS (

    SELECT *
    FROM {{ source('jaffle_service', 'locations_raw') }}

)

SELECT
    l.location_id
    , l.location_name
    , l.opened_at
    , l.tax_rate::NUMBER(38,4) AS tax_rate
FROM source_locations AS l
