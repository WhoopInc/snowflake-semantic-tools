-- =============================================================================
-- expected/ddl/jaffle_menu.sql -- GOLDEN
-- =============================================================================
-- Order line items, the products sold on them, the order they belong to, the
-- pricing period in effect at the time, and the monthly supply cost of each
-- product. Rendered from `project/` on `--target dev`.
--
-- *** EVERY CLAUSE SHAPE IN THIS FILE WAS PUBLISHED TO SNOWFLAKE BEFORE BEING
-- *** WRITTEN HERE. `TESTED` 2026-09-24 in an isolated scratch schema. This file
-- *** carries the four hardest shapes in the fixture, and each was probed
-- *** individually rather than assumed from the reference.
--
-- THIS IS THE JOIN-SHAPE GOLDEN. The other two assert member rendering; this one
-- asserts that the relationship grammar works:
--
--   1. AN ASOF JOIN, from a COMPOSITE relationship. `order_items_to_orders`
--      authors TWO conditions -- an equality on `order_id` and a `>=` on
--      `occurred_at` against `ordered_at`. The `>=` half becomes `ASOF`, which
--      Snowflake compiles to `ASOF JOIN ... MATCH_CONDITION(... >= ...)`. Only
--      `>=` is supported; no other operator has an ASOF form.
--
--      > `TESTED` NEGATIVELY TOO, AND THE NEGATIVE CORRECTED A WRONG ADDITION.
--      > The first probe declared `UNIQUE (ORDER_ID, ORDERED_AT)` on `ORDERS`, on
--      > the assumption that an ASOF reference needs its referenced columns to be
--      > unique -- the reference's own ASOF example declares
--      > `customer_address UNIQUE (ca_cust_id, ca_start_date)`, which invites
--      > exactly that inference. A second probe REMOVED the UNIQUE and still
--      > created. So the key is NOT required, the bare `primary_key: [order_id]`
--      > on `orders.yml` (the MODEL -- `D231`) is already correct, and the
--      > golden does not carry a constraint the input never declared. Copying
--      > the example's shape would have put a phantom key in the contract.
--
--   2. A RANGE JOIN, with the CONSTRAINT that makes it legal. `pricing_periods`
--      declares `distinct_range`, which renders as
--      `CONSTRAINT <name> DISTINCT RANGE BETWEEN <start> AND <end> EXCLUSIVE` on
--      the TABLE, and the relationship renders
--      `REFERENCES PRICING_PERIODS (BETWEEN <start> AND <end> EXCLUSIVE)`.
--      Both halves are required: the constraint asserts no two ranges overlap,
--      which is the precondition the join relies on.
--
--   3. `NON ADDITIVE BY`, from `non_additive_dimensions`. `total_supply_cost`
--      restates cost per product per month, so summing it across months
--      double-counts. The clause sits BEFORE `AS`. The authored
--      `window_function: LAST_VALUE` maps to the DEFAULT behaviour and emits
--      NOTHING -- ascending order already takes the last value, which for a time
--      dimension is the latest. `DESC` would be the non-default and would emit.
--
--   4. `USING (...)`, from `using_relationships`. `line_item_count` is the ONLY
--      member in the fixture that pins a relationship path, and the clause sits
--      before `AS`. The relationship named must START from the metric's own
--      table, which `order_items_to_orders` does.
--
--   6. A DERIVED METRIC REFERENCING A SEMI-ADDITIVE ONE. `GROSS_MARGIN` uses
--      `TOTAL_SUPPLY_COST`, which carries `NON ADDITIVE BY`. That is legal ONLY
--      because `GROSS_MARGIN` is derived -- a non-derived metric may not
--      reference a semi-additive metric. `GROSS_MARGIN_RATE` is derived from
--      `GROSS_MARGIN`, which is itself derived: two levels, which is what
--      exercises the depth guard.
--
--   7. THE MODEL ALIAS SURVIVES INTO THE RELATION NAME. `pricing_periods`
--      declares `alias: pricing_calendar`, the only alias in the project. Every
--      semantic-layer reference is written `pricing_periods`; the emitted logical
--      table and relation are `PRICING_CALENDAR`. The alias is resolved from the
--      manifest, not from the semantic layer, which is why no member file
--      mentions it.
--
--   8. ONE `AI_SQL_GENERATION` CLAUSE CARRYING THREE SOURCES, IN A STATED ORDER.
--      This view attaches TWO instruction blocks to the same channel --
--      `jaffle_sql_conventions` then `jaffle_margin_guidance` -- and the
--      standalone filter's prose is appended after both. Snowflake accepts
--      exactly ONE clause per channel.
--
--      > THIS FILE IS WHY THE ORDERING RULE EXISTS. `custom_instructions.md`
--      > section 4 previously said only that blocks are "CONCATENATED with
--      > newlines", naming neither the order nor the number of newlines -- so
--      > this clause had no assertable form and two engines could both be
--      > conformant while emitting different bytes. The rule is now: blocks in
--      > the order THE VIEW lists them, filter prose LAST, separated by exactly
--      > one blank line. Authored order was chosen because it is the only order
--      > visible from the view being edited, and it puts the specific text
--      > (margin) after the general text (conventions), where it reads as a
--      > qualification rather than being qualified.
--
--   9. NO `AI_QUESTION_CATEGORIZATION`, AND THAT IS AN ATTACHMENT FACT.
--      `jaffle_question_scope` is the only block carrying that channel and this
--      view does not list it. Custom instructions are the ONE member type that
--      does NOT attach by table membership -- the view names them explicitly --
--      so this absence cannot be changed by adding a table.
--
--  10. NO `MAX_STALENESS`, NO `VARIABLES`-DRIVEN TAGS, NO `TAG`. This view
--      declares no `max_staleness` and no `tags`, so neither clause is emitted.
--      An absent clause is correct: `METRICS ()` is a syntax error, so an empty
--      clause is never emitted.
--
--  11. `ORDER_ITEMS` CONTRIBUTES FOUR BARE DIMENSIONS AND ONE BARE FACT.
--      Every `order_items` column carries `column_type` and NOTHING else -- no
--      synonyms, no sample values, no enum flag. That sparseness is the
--      assertion, and it now asserts something stronger than it used to: with
--      the `CA=` payload gone, a member with no optional metadata emits a
--      MINIMAL member rather than an absent payload entry.
--
--  12. NO `WITH EXTENSION (CA=...)`. Decision `D220`. The payload previously on
--      this file's last line named `PRODUCTS`, `PRICING_CALENDAR` and omitted
--      `ORDER_ITEMS`; all of it is now native or deleted.
--
-- VARIABLES ARE DECLARED HERE, AND THE ABSENCE USED TO BE A VIEW THAT COULD NOT
-- BE CREATED. `orders` is shared with `jaffle_sales`, so `large_order_count`,
-- `revenue_on_policy` and `is_large_order` all attach here by table membership
-- while reading variables only `jaffle_sales` declared. Snowflake would have
-- rejected the whole statement with `invalid identifier`. `V028` / `SST-VAL326`
-- now catch it locally; decision `D223`.
--
-- *** NOT COMPARABLE RAW TO `GET_DDL` -- drift finding 3a.
-- =============================================================================

CREATE OR REPLACE SEMANTIC VIEW SST_REF_DEV.CORE.JAFFLE_MENU
  TABLES (
    ORDER_ITEMS AS SST_REF_DEV.JAFFLE.ORDER_ITEMS PRIMARY KEY (ORDER_ITEM_ID),
    PRODUCTS AS SST_REF_DEV.JAFFLE.PRODUCTS PRIMARY KEY (PRODUCT_ID) WITH SYNONYMS ('menu item', 'jaffle'),
    ORDERS AS SST_REF_DEV.JAFFLE.ORDERS PRIMARY KEY (ORDER_ID),
    PRICING_PERIODS AS SST_REF_DEV.JAFFLE.PRICING_CALENDAR PRIMARY KEY (PRICING_PERIOD_ID) CONSTRAINT PRICING_PERIODS_DISTINCT_RANGE DISTINCT RANGE BETWEEN EFFECTIVE_START_AT AND EFFECTIVE_END_AT EXCLUSIVE,
    SUPPLIES AS SST_REF_DEV.JAFFLE.SUPPLIES PRIMARY KEY (SUPPLY_ID)
  )
  RELATIONSHIPS (
    ORDERS_TO_PRICING_PERIODS AS ORDERS (ORDERED_AT) REFERENCES PRICING_PERIODS (BETWEEN EFFECTIVE_START_AT AND EFFECTIVE_END_AT EXCLUSIVE),
    ORDER_ITEMS_TO_ORDERS AS ORDER_ITEMS (ORDER_ID, OCCURRED_AT) REFERENCES ORDERS (ORDER_ID, ASOF ORDERED_AT),
    ORDER_ITEMS_TO_PRODUCTS AS ORDER_ITEMS (PRODUCT_ID) REFERENCES PRODUCTS (PRODUCT_ID),
    SUPPLIES_TO_PRODUCTS AS SUPPLIES (PRODUCT_ID) REFERENCES PRODUCTS (PRODUCT_ID)
  )
  VARIABLES (
    LARGE_ORDER_CENTS NUMBER DEFAULT 1000 COMMENT = 'Threshold above which an order counts as large.',
    TAX_INCLUSIVE BOOLEAN DEFAULT FALSE COMMENT = 'Whether revenue metrics should include collected tax.'
  )
  FACTS (
    ORDERS.ORDER_TOTAL AS ORDERS.ORDER_TOTAL WITH SYNONYMS ('gross total', 'amount charged', 'order value') COMMENT = 'Order value including tax, in cents.' SAMPLE_VALUES ('918', '1188'),
    ORDERS.SUBTOTAL AS ORDERS.SUBTOTAL WITH SYNONYMS ('pre-tax total', 'net amount', 'amount before tax') COMMENT = 'Order value before tax, in cents.' SAMPLE_VALUES ('850', '1100'),
    ORDERS.TAX_PAID AS ORDERS.TAX_PAID WITH SYNONYMS ('tax amount', 'sales tax charged') COMMENT = 'Tax collected on this order, in cents.' SAMPLE_VALUES ('68', '88'),
    ORDER_ITEMS.ITEM_PRICE AS ORDER_ITEMS.ITEM_PRICE COMMENT = 'Price charged for this line item, in cents.',
    PRICING_PERIODS.BASE_PRICE_MULTIPLIER AS PRICING_PERIODS.BASE_PRICE_MULTIPLIER WITH SYNONYMS ('price multiplier', 'rate factor') COMMENT = 'Multiplier applied to list price during this period.' SAMPLE_VALUES ('1.0', '1.15'),
    PRICING_PERIODS.DISCOUNT_PCT AS PRICING_PERIODS.DISCOUNT_PCT WITH SYNONYMS ('discount rate', 'percent off') COMMENT = 'Percentage discount applied during this period.' SAMPLE_VALUES ('0.0', '0.1'),
    PRODUCTS.LIST_PRICE AS PRODUCTS.LIST_PRICE WITH SYNONYMS ('menu price', 'catalogue price', 'sticker price') COMMENT = 'Menu price for one unit, in cents.' SAMPLE_VALUES ('1100', '700'),
    SUPPLIES.SUPPLY_COST AS SUPPLIES.SUPPLY_COST WITH SYNONYMS ('cost of supply', 'unit supply cost') COMMENT = 'Cost to supply one unit of the product in this month, in cents.' SAMPLE_VALUES ('100', '250')
  )
  DIMENSIONS (
    ORDERS.CUSTOMER_ID AS ORDERS.CUSTOMER_ID WITH SYNONYMS ('customer key', 'who placed the order') COMMENT = 'The customer who placed this order.',
    ORDERS.IS_COMPLETED_ORDER LABELS = (FILTER) AS ORDERS.ORDER_STATE = 'completed' COMMENT = 'Restricts to orders that completed, excluding returns and orders still in flight. The default lens for any revenue question.',
    ORDERS.IS_LARGE_ORDER LABELS = (FILTER) AS ORDERS.ORDER_TOTAL > LARGE_ORDER_CENTS COMMENT = 'Restricts to orders above the view''s large-order threshold.',
    ORDERS.IS_RETURNED_ORDER LABELS = (FILTER) AS ORDERS.ORDER_STATE = 'returned' COMMENT = 'Restricts to orders that were returned. The complement lens, for questions about return rate rather than revenue.',
    ORDERS.LOCATION_ID AS ORDERS.LOCATION_ID WITH SYNONYMS ('shop key', 'where the order was placed') COMMENT = 'The store location where this order was placed.',
    ORDERS.ORDERED_AT AS ORDERS.ORDERED_AT WITH SYNONYMS ('order date', 'purchase time', 'when the order was placed') COMMENT = 'When the order was placed.' SAMPLE_VALUES ('2024-07-04 12:15:00', '2024-07-04 18:42:00'),
    ORDERS.ORDER_ID AS ORDERS.ORDER_ID WITH SYNONYMS ('order key', 'order number') COMMENT = 'Surrogate key for the order. An identifier, therefore a dimension.',
    ORDERS.ORDER_STATE AS ORDERS.ORDER_STATE WITH SYNONYMS ('order status', 'fulfilment state') COMMENT = 'Fulfilment state of the order. A closed set.' SAMPLE_VALUES ('completed', 'returned', 'placed') IS_ENUM,
    ORDER_ITEMS.OCCURRED_AT AS ORDER_ITEMS.OCCURRED_AT COMMENT = 'When the line item was recorded.',
    ORDER_ITEMS.ORDER_ID AS ORDER_ITEMS.ORDER_ID COMMENT = 'The order this line item belongs to.',
    ORDER_ITEMS.ORDER_ITEM_ID AS ORDER_ITEMS.ORDER_ITEM_ID COMMENT = 'Surrogate key for the line item. An identifier, therefore a dimension.',
    ORDER_ITEMS.PRODUCT_ID AS ORDER_ITEMS.PRODUCT_ID COMMENT = 'The product sold on this line item.',
    PRICING_PERIODS.EFFECTIVE_END_AT AS PRICING_PERIODS.EFFECTIVE_END_AT WITH SYNONYMS ('period end', 'when the rate stopped applying') COMMENT = 'Exclusive end of the pricing period.' SAMPLE_VALUES ('2024-09-01 00:00:00', '2024-12-01 00:00:00'),
    PRICING_PERIODS.EFFECTIVE_START_AT AS PRICING_PERIODS.EFFECTIVE_START_AT WITH SYNONYMS ('period start', 'when the rate took effect') COMMENT = 'Inclusive start of the pricing period.' SAMPLE_VALUES ('2024-06-01 00:00:00', '2024-09-01 00:00:00'),
    PRICING_PERIODS.PERIOD_NAME AS PRICING_PERIODS.PERIOD_NAME WITH SYNONYMS ('pricing window', 'promo name', 'rate period') COMMENT = 'Human-readable label for the pricing period.' SAMPLE_VALUES ('summer promo', 'standard') IS_ENUM,
    PRICING_PERIODS.PRICING_PERIOD_ID AS PRICING_PERIODS.PRICING_PERIOD_ID WITH SYNONYMS ('pricing period key', 'rate period identifier') COMMENT = 'Surrogate key for the pricing period. An identifier, therefore a dimension.',
    PRODUCTS.PRODUCT_ID AS PRODUCTS.PRODUCT_ID WITH SYNONYMS ('product key', 'menu item identifier') COMMENT = 'Surrogate key for the product. An identifier, therefore a dimension.',
    PRODUCTS.PRODUCT_NAME AS PRODUCTS.PRODUCT_NAME WITH SYNONYMS ('menu item', 'item name', 'dish') COMMENT = 'The menu item''s name as printed on the receipt.' SAMPLE_VALUES ('nutellaphone who dis', 'doctor stew'),
    PRODUCTS.PRODUCT_TYPE AS PRODUCTS.PRODUCT_TYPE WITH SYNONYMS ('item category', 'menu category', 'food or drink') COMMENT = 'Whether the item is food or drink. A closed set.' SAMPLE_VALUES ('jaffle', 'beverage') IS_ENUM,
    SUPPLIES.IS_PERISHABLE AS SUPPLIES.IS_PERISHABLE WITH SYNONYMS ('perishable flag', 'needs refrigeration') COMMENT = 'Whether the supply spoils and must be reordered on a schedule.' SAMPLE_VALUES ('true', 'false') IS_ENUM,
    SUPPLIES.PRODUCT_ID AS SUPPLIES.PRODUCT_ID WITH SYNONYMS ('product key', 'which item is supplied') COMMENT = 'The product this supply cost belongs to.',
    SUPPLIES.SNAPSHOT_MONTH AS SUPPLIES.SNAPSHOT_MONTH WITH SYNONYMS ('snapshot month', 'as-of month', 'restatement month') COMMENT = 'The month this supply cost was restated for.' SAMPLE_VALUES ('2024-07-01 00:00:00', '2024-08-01 00:00:00'),
    SUPPLIES.SUPPLY_ID AS SUPPLIES.SUPPLY_ID WITH SYNONYMS ('supply key', 'supply snapshot identifier') COMMENT = 'Surrogate key for the supply snapshot row. An identifier, therefore a dimension.'
  )
  METRICS (
    ORDERS.AVG_ORDER_VALUE AS AVG(ORDERS.ORDER_TOTAL) WITH SYNONYMS ('AOV', 'average basket') COMMENT = 'Mean order value including tax, in cents.',
    ORDERS.LARGE_ORDER_COUNT AS COUNT(DISTINCT CASE WHEN ORDERS.ORDER_TOTAL > LARGE_ORDER_CENTS THEN ORDERS.ORDER_ID END) WITH SYNONYMS ('big orders') COMMENT = 'Number of orders whose value exceeds the large-order threshold.',
    ORDERS.ORDER_COUNT AS COUNT(DISTINCT ORDERS.ORDER_ID) WITH SYNONYMS ('orders', 'order volume') COMMENT = 'Number of distinct orders placed.',
    ORDERS.RETURNED_ORDER_COUNT AS COUNT(DISTINCT CASE WHEN ORDERS.ORDER_STATE = 'returned' THEN ORDERS.ORDER_ID END) COMMENT = 'Number of orders that were returned.',
    ORDERS.REVENUE_ON_POLICY AS SUM(CASE WHEN TAX_INCLUSIVE THEN ORDERS.ORDER_TOTAL ELSE ORDERS.SUBTOTAL END) COMMENT = 'Revenue measured with or without collected tax, per the view''s tax policy.',
    ORDERS.TOTAL_REVENUE AS SUM(ORDERS.ORDER_TOTAL) WITH SYNONYMS ('revenue', 'gross sales') COMMENT = 'Total order value including tax, in cents.',
    ORDERS.TOTAL_TAX_COLLECTED AS SUM(ORDERS.TAX_PAID) COMMENT = 'Total tax collected across all orders, in cents.',
    ORDER_ITEMS.LINE_ITEM_COUNT USING (ORDER_ITEMS_TO_ORDERS) AS COUNT(ORDER_ITEMS.ORDER_ITEM_ID) WITH SYNONYMS ('units sold', 'items') COMMENT = 'Number of line items sold.',
    ORDER_ITEMS.TOTAL_LINE_ITEM_REVENUE AS SUM(ORDER_ITEMS.ITEM_PRICE) COMMENT = 'Total line-item value, in cents.',
    PRODUCTS.PRODUCT_COUNT AS COUNT(DISTINCT PRODUCTS.PRODUCT_ID) COMMENT = 'Number of distinct products sold.',
    SUPPLIES.TOTAL_SUPPLY_COST NON ADDITIVE BY (SNAPSHOT_MONTH) AS SUM(SUPPLIES.SUPPLY_COST) COMMENT = 'Cost of supplying the products sold, in cents. Not additive across months.',
    GROSS_MARGIN AS ORDER_ITEMS.TOTAL_LINE_ITEM_REVENUE - SUPPLIES.TOTAL_SUPPLY_COST COMMENT = 'Line-item revenue less the cost of supplying what was sold, in cents.',
    GROSS_MARGIN_RATE AS DIV0(GROSS_MARGIN, ORDER_ITEMS.TOTAL_LINE_ITEM_REVENUE) WITH SYNONYMS ('margin rate', 'margin percentage') COMMENT = 'Gross margin as a share of line-item revenue.',
    REVENUE_PER_ORDER AS DIV0(ORDERS.TOTAL_REVENUE, ORDERS.ORDER_COUNT) WITH SYNONYMS ('revenue per order') COMMENT = 'Mean revenue per order, derived from two table-scoped metrics.'
  )
  COMMENT = 'Order line items, the products sold on them, the order they belong to, the
pricing period in effect at the time, and the monthly supply cost of each
product. Use this view for questions about product mix, units sold, line-item
revenue, margin against supply cost, and how pricing periods affected price.

Does NOT cover customer attributes -- there is no join to customers here, so
a question about new versus returning belongs in `jaffle_sales`.'
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

Supply cost is restated per product per month, so it must never be summed
across months -- use the total_supply_cost metric, which declares
snapshot_month as non-additive, rather than aggregating the column directly.
Margin is line-item revenue less supply cost; express margin rate as a
decimal share of line-item revenue.
A margin question scoped to a single month is answerable; a margin question
spanning months is answerable only through the metric, never by summing.

For ORDERS, high_value_threshold_cents is large_order_cents. The order value
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
    PRODUCT_MIX_BY_TYPE AS (
      QUESTION 'How many units did we sell of food versus drink?'
      SQL 'SELECT
      products.product_type                         AS product_type
    , COUNT(order_items.order_item_id)              AS units_sold
    , SUM(order_items.item_price) / 100             AS line_item_revenue
FROM order_items AS order_items
INNER JOIN products AS products
    ON order_items.product_id = products.product_id
INNER JOIN orders AS orders
    ON order_items.order_id = orders.order_id
WHERE orders.order_state = ''completed''
GROUP BY ALL
ORDER BY units_sold DESC'
    ),
    TOTAL_REVENUE_ALL_TIME AS (
      QUESTION 'What is our total revenue?'
      SQL 'SELECT
    SUM(orders.order_total) / 100 AS revenue
FROM orders AS orders'
    )
  )
  COPY GRANTS
