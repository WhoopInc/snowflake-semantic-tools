-- jaffle_sales golden: SST_REF_DEV.JAFFLE.JAFFLE_SALES, rendered from
-- semantic_models/semantic_views/semantic_views.yml on the dev target. The DDL
-- below is what SST renders, and tests/unit/test_golden_ddl.py compares it byte
-- for byte. The statement has been created in Snowflake, in a scratch schema.
--
-- The view sets every view-level key; it is the only golden with MAX_STALENESS
-- and WITH TAG. Notable clauses:
--   - PRIMARY KEY and UNIQUE (CUSTOMER_NAME) come from the models'
--     config.meta.sst; a column in both is an error (SST-VAL223).
--   - VARIABLES take COMMENT = '...'; AI_SQL_GENERATION and
--     AI_QUESTION_CATEGORIZATION take no `=`.
--   - SAMPLE_VALUES renders on facts and dimensions. IS_ENUM follows it, and
--     only where is_enum is true.
--   - Three LABELS = (FILTER) dimensions, one per filter with labels: [filter].
--     IS_LARGE_ORDER reads the view variable LARGE_ORDER_CENTS. A boolean
--     filter without labels is an error (SST-VAL405).
--   - CUSTOMERS.CUMULATIVE_CUSTOMER_COUNT is a window function metric: the
--     OVER (...) built from its window: block follows the expression.
--   - Derived metrics sort after table-scoped ones and carry no table prefix;
--     REVENUE_PER_CUSTOMER combines metrics from two tables.
--   - AI_SQL_GENERATION is jaffle_sql_conventions, a blank line, then prose
--     from high_value_threshold_cents, a filter without labels whose
--     expression is not boolean. AI_QUESTION_CATEGORIZATION is
--     jaffle_question_scope.
--   - VERIFIED_AT is epoch seconds whether authored as an integer or as a
--     date string. TOTAL_REVENUE_ALL_TIME carries only QUESTION and SQL.
--   - COPY GRANTS is always rendered.
--
-- Not comparable raw to GET_DDL; see tests/golden/README.md.

CREATE OR REPLACE SEMANTIC VIEW SST_REF_DEV.JAFFLE.JAFFLE_SALES
  TABLES (
    ORDERS AS SST_REF_DEV.JAFFLE.ORDERS PRIMARY KEY (ORDER_ID) WITH SYNONYMS ('sale', 'transaction'),
    CUSTOMERS AS SST_REF_DEV.JAFFLE.CUSTOMERS PRIMARY KEY (CUSTOMER_ID) UNIQUE (CUSTOMER_NAME) WITH SYNONYMS ('buyer', 'guest'),
    LOCATIONS AS SST_REF_DEV.JAFFLE.LOCATIONS PRIMARY KEY (LOCATION_ID)
  )
  RELATIONSHIPS (
    ORDERS_TO_CUSTOMERS AS ORDERS (CUSTOMER_ID) REFERENCES CUSTOMERS (CUSTOMER_ID),
    ORDERS_TO_LOCATIONS AS ORDERS (LOCATION_ID) REFERENCES LOCATIONS (LOCATION_ID)
  )
  VARIABLES (
    LARGE_ORDER_CENTS NUMBER DEFAULT 1000 COMMENT = 'Threshold above which an order counts as large.',
    TAX_INCLUSIVE BOOLEAN DEFAULT FALSE COMMENT = 'Whether revenue metrics should include collected tax.'
  )
  FACTS (
    CUSTOMERS.LIFETIME_SPEND AS CUSTOMERS.LIFETIME_SPEND WITH SYNONYMS ('total spend', 'customer value', 'spend to date') COMMENT = 'Total amount the customer has spent across all orders, in cents.' SAMPLE_VALUES ('1836', '4590'),
    LOCATIONS.TAX_RATE AS LOCATIONS.TAX_RATE WITH SYNONYMS ('sales tax rate', 'tax percentage') COMMENT = 'Local sales tax rate applied at this location.' SAMPLE_VALUES ('0.08', '0.0875'),
    ORDERS.ORDER_TOTAL AS ORDERS.ORDER_TOTAL WITH SYNONYMS ('gross total', 'amount charged', 'order value') COMMENT = 'Order value including tax, in cents.' SAMPLE_VALUES ('918', '1188'),
    ORDERS.SUBTOTAL AS ORDERS.SUBTOTAL WITH SYNONYMS ('pre-tax total', 'net amount', 'amount before tax') COMMENT = 'Order value before tax, in cents.' SAMPLE_VALUES ('850', '1100'),
    ORDERS.TAX_PAID AS ORDERS.TAX_PAID WITH SYNONYMS ('tax amount', 'sales tax charged') COMMENT = 'Tax collected on this order, in cents.' SAMPLE_VALUES ('68', '88')
  )
  DIMENSIONS (
    CUSTOMERS.CUSTOMER_ID AS CUSTOMERS.CUSTOMER_ID WITH SYNONYMS ('customer key', 'customer identifier') COMMENT = 'Surrogate key for the customer. An identifier, therefore a dimension and never a fact.',
    CUSTOMERS.CUSTOMER_NAME AS CUSTOMERS.CUSTOMER_NAME WITH SYNONYMS ('customer full name', 'who the customer is') COMMENT = 'The customer''s display name.',
    CUSTOMERS.CUSTOMER_TYPE AS CUSTOMERS.CUSTOMER_TYPE WITH SYNONYMS ('customer segment', 'new or returning', 'customer cohort') COMMENT = 'Whether the customer has ordered before. A closed set.' SAMPLE_VALUES ('new', 'returning') IS_ENUM,
    CUSTOMERS.FIRST_ORDERED_AT AS CUSTOMERS.FIRST_ORDERED_AT WITH SYNONYMS ('first order date', 'date of first purchase', 'cohort date') COMMENT = 'When the customer placed their first order.' SAMPLE_VALUES ('2024-01-15 09:30:00', '2024-06-02 14:05:00'),
    LOCATIONS.LOCATION_ID AS LOCATIONS.LOCATION_ID WITH SYNONYMS ('location key', 'shop identifier') COMMENT = 'Surrogate key for the location. An identifier, therefore a dimension.',
    LOCATIONS.LOCATION_NAME AS LOCATIONS.LOCATION_NAME WITH SYNONYMS ('shop name', 'store name', 'branch') COMMENT = 'The store''s trading name.' SAMPLE_VALUES ('Philadelphia', 'Brooklyn'),
    LOCATIONS.OPENED_AT AS LOCATIONS.OPENED_AT WITH SYNONYMS ('opening date', 'when the shop opened') COMMENT = 'When the location opened for trade.' SAMPLE_VALUES ('2023-03-01 08:00:00', '2023-09-15 08:00:00'),
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
    ORDERS.REVENUE_ON_POLICY AS SUM(CASE WHEN TAX_INCLUSIVE THEN ORDERS.ORDER_TOTAL ELSE ORDERS.SUBTOTAL END) COMMENT = 'Revenue measured with or without collected tax, per the view''s tax policy.',
    ORDERS.TOTAL_REVENUE AS SUM(ORDERS.ORDER_TOTAL) WITH SYNONYMS ('revenue', 'gross sales') COMMENT = 'Total order value including tax, in cents.',
    ORDERS.TOTAL_TAX_COLLECTED AS SUM(ORDERS.TAX_PAID) COMMENT = 'Total tax collected across all orders, in cents.',
    REVENUE_PER_CUSTOMER AS DIV0(ORDERS.TOTAL_REVENUE, CUSTOMERS.CUSTOMER_COUNT) COMMENT = 'Mean revenue per customer over the period in scope.',
    REVENUE_PER_ORDER AS DIV0(ORDERS.TOTAL_REVENUE, ORDERS.ORDER_COUNT) WITH SYNONYMS ('revenue per order') COMMENT = 'Mean revenue per order, derived from two table-scoped metrics.'
  )
  COMMENT = 'Orders, the customers who placed them, and the locations they were placed at.
Use this view for questions about order volume, order value, tax collected,
and how revenue and customer counts break down by location or by whether a
customer is new or returning.

Does NOT cover what was ON an order -- line items, products and supply costs
live in `jaffle_menu`. Does not cover menu catalogue detail at all.'
  AI_SQL_GENERATION 'Round every monetary amount to two decimal places.
Monetary columns are stored in cents, so divide by one hundred before
presenting a currency figure.
Express a share or a rate as a decimal between zero and one, not as a
percentage.
When dividing, guard the denominator with DIV0 so an empty population
returns zero rather than an error.
Treat an order as complete using the is_completed_order filter rather than
comparing state by hand, and treat a return using is_returned_order.
Interpret a pricing period as half-open: the start is inclusive and the end
is exclusive.

For ORDERS, high_value_threshold_cents is large_order_cents. The order value
above which a sale is treated as high-value in generated narrative. A
threshold, not a predicate.'
  AI_QUESTION_CATEGORIZATION 'These views answer questions about orders, the customers who placed them,
the store locations they were placed at, the menu items sold, and the cost
of supplying those items.
A question about order volume, order value, tax collected, product mix or
margin is in scope.
A question about an individual named customer is OUT OF SCOPE. These views
are for aggregate analysis and carry no person-level narrative.
A question about staffing, payroll, supplier contracts or delivery logistics
is OUT OF SCOPE -- no table here carries any of it.
A question that needs both customer attributes and product detail spans two
views and cannot be answered from either alone; say so rather than answering
from whichever view is to hand.'
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
    REVENUE_BY_LOCATION AS (
      QUESTION 'What is total revenue by store location?'
      VERIFIED_AT 1767225600
      ONBOARDING_QUESTION TRUE
      VERIFIED_BY 'daa-platform'
      SQL 'SELECT
      locations.location_name                       AS location_name
    , SUM(orders.order_total) / 100                 AS revenue
FROM orders AS orders
INNER JOIN locations AS locations
    ON orders.location_id = locations.location_id
WHERE orders.order_state = ''completed''
GROUP BY ALL
ORDER BY revenue DESC'
    ),
    TOTAL_REVENUE_ALL_TIME AS (
      QUESTION 'What is our total revenue?'
      SQL 'SELECT
    SUM(orders.order_total) / 100 AS revenue
FROM orders AS orders'
    )
  )
  MAX_STALENESS = '300 seconds'
  WITH TAG (
      SST_REF_DEV.JAFFLE.COST_CENTER = 'analytics'
    , SST_REF_DEV.JAFFLE.DATA_DOMAIN = 'jaffle_sales'
  )
  COPY GRANTS
