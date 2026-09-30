-- The sidecar of verified query product_mix_by_type, read through sql_file:
-- relative to verified_queries.yml. SST strips this leading comment block, so
-- the rendered SQL starts at SELECT.
-- The one data literal, 'completed', is a member of the order_state enum.

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
