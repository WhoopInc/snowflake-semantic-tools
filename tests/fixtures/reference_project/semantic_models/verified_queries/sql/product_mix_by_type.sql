-- ============================================================================
-- semantic_models/verified_queries/sql/product_mix_by_type.sql
-- ============================================================================
-- THE SIDECAR. Read by `verified_queries.yml`'s `product_mix_by_type` entry via
-- `sql_file: sql/product_mix_by_type.sql`, which is relative to THE DECLARING
-- FILE rather than to the project root.
--
-- `D208` settled that the base-path difference between `sql_file:` and
-- `{{ file() }}` is CORRECT rather than accidental: relative-to-declaring-file is
-- what lets this directory be moved as a unit without rewriting its paths, while
-- `{{ file() }}` normalises against the project root because it is a general
-- resolver callable from any field of any artifact -- and a resolver that meant
-- different things in different files would be the one thing a resolver must not
-- do.
--
-- WHOOP SQL conventions: UPPERCASE keywords, leading commas, tables aliased,
-- columns prefixed, explicit JOIN, GROUP BY ALL.
--
-- NO DATA VALUES. The one literal is the enum member `completed`, which is a
-- closed-set value the dbt model declares with `is_enum: true`.
-- ============================================================================

SELECT
      products.product_type                         AS product_type
    , COUNT(order_items.order_item_id)              AS units_sold
    , SUM(order_items.item_price) / 100             AS line_item_revenue
FROM order_items AS order_items
INNER JOIN products AS products
    ON order_items.product_id = products.product_id
INNER JOIN orders AS orders
    ON order_items.order_id = orders.order_id
WHERE orders.order_state = 'completed'
GROUP BY ALL
ORDER BY units_sold DESC
