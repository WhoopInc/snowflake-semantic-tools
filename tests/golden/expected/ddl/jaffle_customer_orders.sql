-- jaffle_customer_orders golden: SST_REF_DEV.JAFFLE.JAFFLE_CUSTOMER_ORDERS,
-- rendered from tests/fixtures/scoped_views/semantic_views.yml placed under the
-- reference project's semantic_models/semantic_views/scoped/. The DDL below is
-- what SST renders, and tests/unit/test_golden_ddl.py compares it byte for byte.
-- Unlike the reference goldens, it has not yet been created in Snowflake.
--
-- EXCLUDE MODE. Everything ORDERS and CUSTOMERS attach renders except what the
-- view names:
--   - CUSTOMERS.CUSTOMER_NAME and ORDERS.TAX_PAID are absent from DIMENSIONS
--     and FACTS. The UNIQUE (CUSTOMER_NAME) table constraint stays: it is the
--     model's key, not a member.
--   - TOTAL_TAX_COLLECTED and REVENUE_ON_POLICY are absent from METRICS, so the
--     view needs no TAX_INCLUSIVE variable.
--   - Filters are never scoped: the three LABELS = (FILTER) dimensions render,
--     and IS_LARGE_ORDER reads LARGE_ORDER_CENTS, which the view declares.
--   - Relationships, verified queries and filter prose attach as usual.
--
-- Not comparable raw to GET_DDL; see tests/golden/README.md.

CREATE OR REPLACE SEMANTIC VIEW SST_REF_DEV.JAFFLE.JAFFLE_CUSTOMER_ORDERS
  TABLES (
    ORDERS AS SST_REF_DEV.JAFFLE.ORDERS PRIMARY KEY (ORDER_ID),
    CUSTOMERS AS SST_REF_DEV.JAFFLE.CUSTOMERS PRIMARY KEY (CUSTOMER_ID) UNIQUE (CUSTOMER_NAME)
  )
  RELATIONSHIPS (
    ORDERS_TO_CUSTOMERS AS ORDERS (CUSTOMER_ID) REFERENCES CUSTOMERS (CUSTOMER_ID)
  )
  VARIABLES (
    LARGE_ORDER_CENTS NUMBER DEFAULT 1000 COMMENT = 'Threshold above which an order counts as large.'
  )
  FACTS (
    CUSTOMERS.LIFETIME_SPEND AS CUSTOMERS.LIFETIME_SPEND WITH SYNONYMS ('total spend', 'customer value', 'spend to date') COMMENT = 'Total amount the customer has spent across all orders, in cents.' SAMPLE_VALUES ('1836', '4590'),
    ORDERS.ORDER_TOTAL AS ORDERS.ORDER_TOTAL WITH SYNONYMS ('gross total', 'amount charged', 'order value') COMMENT = 'Order value including tax, in cents.' SAMPLE_VALUES ('918', '1188'),
    ORDERS.SUBTOTAL AS ORDERS.SUBTOTAL WITH SYNONYMS ('pre-tax total', 'net amount', 'amount before tax') COMMENT = 'Order value before tax, in cents.' SAMPLE_VALUES ('850', '1100')
  )
  DIMENSIONS (
    CUSTOMERS.CUSTOMER_ID AS CUSTOMERS.CUSTOMER_ID WITH SYNONYMS ('customer key', 'customer identifier') COMMENT = 'Surrogate key for the customer. An identifier, therefore a dimension and never a fact.',
    CUSTOMERS.CUSTOMER_TYPE AS CUSTOMERS.CUSTOMER_TYPE WITH SYNONYMS ('customer segment', 'new or returning', 'customer cohort') COMMENT = 'Whether the customer has ordered before. A closed set.' SAMPLE_VALUES ('new', 'returning') IS_ENUM,
    CUSTOMERS.FIRST_ORDERED_AT AS CUSTOMERS.FIRST_ORDERED_AT WITH SYNONYMS ('first order date', 'date of first purchase', 'cohort date') COMMENT = 'When the customer placed their first order.' SAMPLE_VALUES ('2024-01-15 09:30:00', '2024-06-02 14:05:00'),
    ORDERS.CUSTOMER_ID AS ORDERS.CUSTOMER_ID WITH SYNONYMS ('customer key', 'who placed the order') COMMENT = 'The customer who placed this order.',
    ORDERS.IS_COMPLETED_ORDER LABELS = (FILTER) AS ORDERS.ORDER_STATE = 'completed' COMMENT = 'Restricts to orders that completed, excluding returns and orders still in flight. The default lens for any revenue question.',
    ORDERS.IS_LARGE_ORDER LABELS = (FILTER) AS ORDERS.ORDER_TOTAL > LARGE_ORDER_CENTS COMMENT = 'Restricts to orders above the view''s large-order threshold.',
    ORDERS.IS_RETURNED_ORDER LABELS = (FILTER) AS ORDERS.ORDER_STATE = 'returned' COMMENT = 'Restricts to orders that were returned. The complement lens, for questions about return rate rather than revenue.',
    ORDERS.LOCATION_ID AS ORDERS.LOCATION_ID WITH SYNONYMS ('shop key', 'where the order was placed') COMMENT = 'The store location where this order was placed.',
    ORDERS.ORDERED_AT AS ORDERS.ORDERED_AT WITH SYNONYMS ('order date', 'purchase time', 'when the order was placed') COMMENT = 'When the order was placed.' SAMPLE_VALUES ('2024-07-04 12:15:00', '2024-07-04 18:42:00'),
    ORDERS.ORDER_ID AS ORDERS.ORDER_ID WITH SYNONYMS ('order key', 'order number') COMMENT = 'Surrogate key for the order. An identifier, therefore a dimension.',
    ORDERS.ORDER_STATE AS ORDERS.ORDER_STATE WITH SYNONYMS ('order status', 'fulfilment state') COMMENT = 'Fulfilment state of the order. A closed set.' SAMPLE_VALUES ('completed', 'returned', 'placed') IS_ENUM
  )
  METRICS (
    CUSTOMERS.CUMULATIVE_CUSTOMER_COUNT AS SUM(CUSTOMERS.CUSTOMER_COUNT) OVER (PARTITION BY EXCLUDING CUSTOMERS.FIRST_ORDERED_AT ORDER BY CUSTOMERS.FIRST_ORDERED_AT ASC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) COMMENT = 'Running count of customers, by the date of their first order.',
    CUSTOMERS.CUSTOMER_COUNT AS COUNT(DISTINCT CUSTOMERS.CUSTOMER_ID) WITH SYNONYMS ('customers', 'buyers') COMMENT = 'Number of distinct customers who placed at least one order.',
    ORDERS.AVG_ORDER_VALUE AS AVG(ORDERS.ORDER_TOTAL) WITH SYNONYMS ('AOV', 'average basket') COMMENT = 'Mean order value including tax, in cents.',
    ORDERS.LARGE_ORDER_COUNT AS COUNT(DISTINCT CASE WHEN ORDERS.ORDER_TOTAL > LARGE_ORDER_CENTS THEN ORDERS.ORDER_ID END) WITH SYNONYMS ('big orders') COMMENT = 'Number of orders whose value exceeds the large-order threshold.',
    ORDERS.ORDER_COUNT AS COUNT(DISTINCT ORDERS.ORDER_ID) WITH SYNONYMS ('orders', 'order volume') COMMENT = 'Number of distinct orders placed.',
    ORDERS.RETURNED_ORDER_COUNT AS COUNT(DISTINCT CASE WHEN ORDERS.ORDER_STATE = 'returned' THEN ORDERS.ORDER_ID END) COMMENT = 'Number of orders that were returned.',
    ORDERS.TOTAL_REVENUE AS SUM(ORDERS.ORDER_TOTAL) WITH SYNONYMS ('revenue', 'gross sales') COMMENT = 'Total order value including tax, in cents.',
    REVENUE_PER_CUSTOMER AS DIV0(ORDERS.TOTAL_REVENUE, CUSTOMERS.CUSTOMER_COUNT) COMMENT = 'Mean revenue per customer over the period in scope.',
    REVENUE_PER_ORDER AS DIV0(ORDERS.TOTAL_REVENUE, ORDERS.ORDER_COUNT) WITH SYNONYMS ('revenue per order') COMMENT = 'Mean revenue per order, derived from two table-scoped metrics.'
  )
  COMMENT = 'Orders and the customers who placed them, without customer names or tax.
Use this view for questions about order volume and value by customer
type, and how many customers order.'
  AI_SQL_GENERATION 'For ORDERS, high_value_threshold_cents is large_order_cents. The order value
above which a sale is treated as high-value in generated narrative. A
threshold, not a predicate.'
  AI_VERIFIED_QUERIES (
    ORDER_COUNT_BY_STATE AS (
      QUESTION 'How many orders are there in each fulfilment state?'
      VERIFIED_AT 1769904000
      ONBOARDING_QUESTION TRUE
      VERIFIED_BY 'daa-platform'
      SQL 'SELECT
      orders.order_state                  AS order_state
    , COUNT(DISTINCT orders.order_id)     AS order_count
FROM orders AS orders
GROUP BY ALL
ORDER BY order_count DESC'
    ),
    TOTAL_REVENUE_ALL_TIME AS (
      QUESTION 'What is our total revenue?'
      SQL 'SELECT
    SUM(orders.order_total) / 100 AS revenue
FROM orders AS orders'
    )
  )
  COPY GRANTS
