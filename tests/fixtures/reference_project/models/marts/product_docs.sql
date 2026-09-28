-- ============================================================================
-- models/marts/product_docs.sql
-- ============================================================================
-- THE ONLY MART NO SEMANTIC VIEW REFERENCES, and that is the whole reason it
-- exists as a separate model rather than as four more columns on `products.sql`.
--
-- WHY NOT JUST ADD `product_description` TO `products`? Because
-- `semantic_models/semantic_views/semantic_views.yml` lists TABLES, NOT MEMBERS.
-- Every column on a referenced table is consumed by the semantic layer, so a
-- column added to `products` MUST carry a `column_type` (`SST-VAL308` is an
-- ERROR otherwise) and therefore BECOMES A MEMBER of both `jaffle_menu` and
-- `jaffle_minimal`. A paragraph of prose is a bad dimension: Cortex Analyst can
-- GROUP BY it, its `sample_values` would be sentences, and it would widen two
-- DDL goldens for a column no metric or filter can use.
--
-- SO THE TWO CONSUMERS READ TWO RELATIONS. `products` serves the ANALYST path
-- and carries only what a metric, dimension or filter can use. `product_docs`
-- serves the SEARCH path and carries the prose. Both are built from the same
-- raw relation, so they cannot disagree about the catalogue.
--
-- THIS IS NOT A FIXTURE CONTRIVANCE. It is what production looks like: a
-- retrieval index wants long text and a semantic view wants short, low-
-- cardinality attributes, and forcing one relation to serve both makes the
-- semantic view worse without making the index better.
--
-- WHOOP SQL conventions, the same set `../staging/stg_orders.sql` documents:
-- import CTEs first, one per source() or ref(); UPPERCASE keywords, functions
-- and types; leading commas; tables always aliased; columns always prefixed
-- with the alias.
--
-- NO DATA VALUES. Every literal below is structural. The descriptions
-- themselves live in `../../seeds/products_raw.csv`, which is seed data by
-- definition and is synthetic product copy carrying nothing about any person.
-- ============================================================================

WITH source_products AS (

    SELECT *
    FROM {{ source('jaffle_service', 'products_raw') }}

)

SELECT
      source_products.product_id                        AS product_id
    , source_products.product_name                      AS product_name
    , source_products.product_type                      AS product_type
    , source_products.dietary_class                     AS dietary_class
    , source_products.product_description               AS product_description
FROM source_products AS source_products
