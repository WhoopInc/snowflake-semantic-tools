-- products: the menu, one row per item (ten in the seed), with list_price cast
-- to the NUMBER(38,2) its contract declares.
-- Style: one import CTE per source, UPPERCASE keywords, leading commas,
-- aliased tables and alias-qualified columns.

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
