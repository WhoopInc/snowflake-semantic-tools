-- ============================================================================
-- skills/jaffle-operations/check-supply-month.sql
-- ============================================================================
-- A SCRIPT, AND IT SITS AT THE FOLDER ROOT BESIDE SKILL.md ON PURPOSE. K001:
-- "`SKILL.md` is at the folder ROOT; scripts live in the same folder." Prose
-- reference material goes under `reference/`; executable material does not. This
-- file is what makes that asymmetry observable in the flattened catalog payload,
-- where a script and a bundled reference file collapse from different origins into
-- the same flat namespace.
--
-- NO DATA VALUES. The one literal is the enum member `completed`.
-- ============================================================================

-- Is a supply month complete? A month is final when every product sold in it has a
-- supply snapshot row. A product with sales and no snapshot understates cost and so
-- overstates margin.

WITH sold_in_month AS (

    SELECT DISTINCT
          order_items.product_id                                AS product_id
        , DATE_TRUNC('month', orders.ordered_at)                AS sales_month
    FROM order_items AS order_items
    INNER JOIN orders AS orders
        ON order_items.order_id = orders.order_id
    WHERE orders.order_state = 'completed'

)

, snapshotted AS (

    SELECT DISTINCT
          supplies.product_id                                   AS product_id
        , DATE_TRUNC('month', supplies.snapshot_month)           AS sales_month
    FROM supplies AS supplies

)

SELECT
      sold_in_month.sales_month                                 AS sales_month
    , sold_in_month.product_id                                  AS product_id_missing_snapshot
FROM sold_in_month AS sold_in_month
LEFT JOIN snapshotted AS snapshotted
    ON  sold_in_month.product_id = snapshotted.product_id
    AND sold_in_month.sales_month = snapshotted.sales_month
WHERE snapshotted.product_id IS NULL
ORDER BY sales_month DESC, product_id_missing_snapshot
