-- =============================================================================
-- expected/ddl/jaffle_sales.sql -- GOLDEN
-- =============================================================================
-- Orders, the customers who placed them, and the locations they were placed at.
-- Rendered from `project/` on `--target dev`, so every relation resolves under
-- `SST_REF_DEV.JAFFLE` (`profiles.yml` sets `target: dev`).
--
-- *** EVERY CLAUSE SHAPE IN THIS FILE WAS PUBLISHED TO SNOWFLAKE BEFORE BEING
-- *** WRITTEN HERE. `TESTED` 2026-09-24 in an isolated scratch schema against
-- *** stand-in tables of the same shape. Three of those probes FAILED FIRST;
-- *** they are recorded as drift findings 3a-3c.
--
-- CLAUSE ORDER IS FIXED AND VERIFIED:
--   TABLES -> RELATIONSHIPS -> VARIABLES -> FACTS -> DIMENSIONS -> METRICS
--   -> COMMENT -> AI_SQL_GENERATION -> AI_QUESTION_CATEGORIZATION
--   -> AI_VERIFIED_QUERIES -> MAX_STALENESS -> COPY GRANTS
--
--   `VARIABLES` SITS BETWEEN `RELATIONSHIPS` AND `FACTS`. That is not a guess;
--   it is the documented position and it is what created. An earlier draft of
--   this plan assumed `MAX_STALENESS` followed `COMMENT` -- it does not, it comes
--   after `AI_QUESTION_CATEGORIZATION`, and only executing the statement settled
--   which of the two orderings was real.
--
-- WHAT THIS FILE ASSERTS
--
--   1. NO `WITH EXTENSION (CA=...)` CLAUSE. Decision `D220` deletes it. Sample
--      values and enum flags are NATIVE clauses below; the `time_dimensions`
--      provenance array is gone as redundant with DATA_TYPE (`D219`); and a
--      boolean standalone filter -- the one shape that needed the payload -- is
--      now an ERROR under `F007`. Drift finding 3 is SCOPED
--      by that decision, NOT overturned: it was `TESTED`, and its observed
--      payload was a BOOLEAN standalone filter, which is exactly the shape
--      `F007` now forbids.
--
--   2. THREE `LABELS = (FILTER)` DIMENSIONS, one of which READS A VIEW VARIABLE.
--      This is the native, Snowflake-recommended filter form, and it is what
--      every boolean filter compiles to. `IS_LARGE_ORDER` reads
--      `LARGE_ORDER_CENTS`, which proves variable substitution reaches a filter
--      and not only a metric.
--
--      > PREVIOUSLY THIS FILE WAS WRONG ABOUT ALL THREE. It asserted
--      > `IS_COMPLETED_ORDER AS ORDERS.ENDED_AT IS NOT NULL` and
--      > `IS_RETURNED_ORDER AS ORDERS.STATUS = 'STATUS_PLACEHOLDER_A'` -- naming
--      > two columns that NO LONGER EXIST and a pre-`D213` placeholder value --
--      > and omitted `IS_LARGE_ORDER` entirely. The input declares
--      > `order_state = 'completed'` and `= 'returned'`. The payload on the last
--      > line had ALREADY been rebuilt to the new column while the DIMENSIONS
--      > clause had not, so one file disagreed with itself.
--
--   3. NATIVE `SAMPLE_VALUES` ON FACTS AS WELL AS DIMENSIONS. `SAMPLE_VALUES` is
--      valid on both; `IS_ENUM` is DIMENSIONS-ONLY, takes no value, and must
--      FOLLOW `SAMPLE_VALUES`. `is_enum: false` emits NOTHING -- which is why
--      `CUSTOMER_TYPE` and `ORDER_STATE` carry `IS_ENUM` and the seven
--      `is_enum: false` columns do not.
--
--   4. `VARIABLES` WITH `COMMENT = `, AND THE `=` IS THE ASSERTION. The published
--      `variableDef` grammar shows `COMMENT '<description>'` with NO equals sign,
--      and that form is a SYNTAX ERROR -- drift finding 3b. Note the inconsistency
--      this file therefore contains and must contain: `VARIABLES` needs
--      `COMMENT = `, while `AI_SQL_GENERATION` and `AI_QUESTION_CATEGORIZATION`
--      take NO `=` and are a syntax error with one. Three clauses, two
--      conventions.
--
--   5. TWO DERIVED METRICS, RENDERED WITHOUT A TABLE PREFIX.
--      `REVENUE_PER_CUSTOMER` combines metrics from two different logical tables,
--      which is the case a table-scoped metric cannot express.
--
--   6. `UNIQUE (CUSTOMER_NAME)` ALONGSIDE `PRIMARY KEY (CUSTOMER_ID)` on one
--      table, from `customers.yml`'s `config.meta.sst.unique_keys` -- the MODEL,
--      not this view. `D231`. `V026` is what forbids the two lists overlapping.
--
--   7. ONE `AI_SQL_GENERATION` CLAUSE CARRYING TWO SOURCES. The view attaches
--      `jaffle_sql_conventions`, and the standalone filter's prose is appended
--      after it. Snowflake accepts exactly ONE clause per channel, so
--      composition happens before emit: blocks first in the order the view lists
--      them, filter prose LAST, separated by exactly one blank line.
--
--   8. THE STANDALONE FILTER'S PROSE IN ITS 1.0 FORM. `high_value_threshold_cents`
--      is NON-boolean, carries no `labels:`, and reaches `AI_SQL_GENERATION`
--      because `filters_to_instructions` is true. The 0.3 template -- "apply X
--      unless the user explicitly requests unfiltered data" -- is DEAD: it is
--      written for a predicate, and after `F007` became an ERROR no boolean
--      standalone filter can exist, so only non-predicates reach this path.
--      `D225` replaces it with the value-stating form below.
--
--   9. TWO `verified_at` INPUT FORMS CONVERGING ON INTEGERS. The input authors a
--      bare integer `1769904000` on one query and the string `"2026-01-01"` on
--      another; both emit as epoch seconds. `TESTED`: `1769904000` is
--      2026-02-01 and `2026-01-01` is `1767225600`.
--
--  10. A MINIMAL VERIFIED QUERY. `TOTAL_REVENUE_ALL_TIME` omits `VERIFIED_AT`,
--      `VERIFIED_BY` and `ONBOARDING_QUESTION` -- all three keys are ABSENT in
--      the input, not false -- and `TESTED` confirms a verified query carrying
--      only `QUESTION` and `SQL` is accepted. `VERIFIED_BY` also takes a BARE
--      string here; the reference shows only the `'(purpose = contact)'` form.
--
--  11. SINGLE-QUOTE DOUBLING, in three member comments and one instruction
--      channel: `customer''s`, `store''s`, `view''s`.
--
-- *** NOT COMPARABLE RAW TO `GET_DDL`. Finding 3a: read-back lowercases every
-- *** keyword, rewrites `WITH SYNONYMS ('a', 'b')` to `with synonyms=('a','b')`
-- *** while leaving `sample_values ('x', 'y')` spaced, and omits `COPY GRANTS`.
-- =============================================================================

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
