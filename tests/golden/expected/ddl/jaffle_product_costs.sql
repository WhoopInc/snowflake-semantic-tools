-- jaffle_product_costs golden: SST_REF_DEV.JAFFLE.JAFFLE_PRODUCT_COSTS, rendered
-- from tests/fixtures/scoped_views/semantic_views.yml placed under the reference
-- project's semantic_models/semantic_views/scoped/. The DDL below is what SST
-- renders, and tests/unit/test_golden_ddl.py compares it byte for byte. Unlike
-- the reference goldens, it has not yet been created in Snowflake.
--
-- INCLUDE MODE. The view lists its columns, metrics and relationships, and only
-- those render:
--   - FACTS and DIMENSIONS hold the nine listed columns; LIST_PRICE, SUPPLY_ID,
--     IS_PERISHABLE, ORDER_ID and OCCURRED_AT are left out.
--   - METRICS holds the five listed metrics. LINE_ITEM_COUNT attaches through
--     ORDER_ITEMS but is not listed; it would otherwise need USING
--     (ORDER_ITEMS_TO_ORDERS), which this view cannot hold (SST-VAL332).
--   - TOTAL_SUPPLY_COST sorts by SNAPSHOT_MONTH, so that column is listed;
--     leaving it out is SST-VAL318.
--   - Both listed relationships render; every relationship these tables hold is
--     listed.
--
-- Not comparable raw to GET_DDL; see tests/golden/README.md.

CREATE OR REPLACE SEMANTIC VIEW SST_REF_DEV.JAFFLE.JAFFLE_PRODUCT_COSTS
  TABLES (
    ORDER_ITEMS AS SST_REF_DEV.JAFFLE.ORDER_ITEMS PRIMARY KEY (ORDER_ITEM_ID),
    PRODUCTS AS SST_REF_DEV.JAFFLE.PRODUCTS PRIMARY KEY (PRODUCT_ID),
    SUPPLIES AS SST_REF_DEV.JAFFLE.SUPPLIES PRIMARY KEY (SUPPLY_ID)
  )
  RELATIONSHIPS (
    ORDER_ITEMS_TO_PRODUCTS AS ORDER_ITEMS (PRODUCT_ID) REFERENCES PRODUCTS (PRODUCT_ID),
    SUPPLIES_TO_PRODUCTS AS SUPPLIES (PRODUCT_ID) REFERENCES PRODUCTS (PRODUCT_ID)
  )
  FACTS (
    ORDER_ITEMS.ITEM_PRICE AS ORDER_ITEMS.ITEM_PRICE COMMENT = 'Price charged for this line item, in cents.',
    SUPPLIES.SUPPLY_COST AS SUPPLIES.SUPPLY_COST WITH SYNONYMS ('cost of supply', 'unit supply cost') COMMENT = 'Cost to supply one unit of the product in this month, in cents.' SAMPLE_VALUES ('100', '250')
  )
  DIMENSIONS (
    ORDER_ITEMS.ORDER_ITEM_ID AS ORDER_ITEMS.ORDER_ITEM_ID COMMENT = 'Surrogate key for the line item. An identifier, therefore a dimension.',
    ORDER_ITEMS.PRODUCT_ID AS ORDER_ITEMS.PRODUCT_ID COMMENT = 'The product sold on this line item.',
    PRODUCTS.PRODUCT_ID AS PRODUCTS.PRODUCT_ID WITH SYNONYMS ('product key', 'menu item identifier') COMMENT = 'Surrogate key for the product. An identifier, therefore a dimension.',
    PRODUCTS.PRODUCT_NAME AS PRODUCTS.PRODUCT_NAME WITH SYNONYMS ('menu item', 'item name', 'dish') COMMENT = 'The menu item''s name as printed on the receipt.' SAMPLE_VALUES ('nutellaphone who dis', 'doctor stew'),
    PRODUCTS.PRODUCT_TYPE AS PRODUCTS.PRODUCT_TYPE WITH SYNONYMS ('item category', 'menu category', 'food or drink') COMMENT = 'Whether the item is food or drink. A closed set.' SAMPLE_VALUES ('jaffle', 'beverage') IS_ENUM,
    SUPPLIES.PRODUCT_ID AS SUPPLIES.PRODUCT_ID WITH SYNONYMS ('product key', 'which item is supplied') COMMENT = 'The product this supply cost belongs to.',
    SUPPLIES.SNAPSHOT_MONTH AS SUPPLIES.SNAPSHOT_MONTH WITH SYNONYMS ('snapshot month', 'as-of month', 'restatement month') COMMENT = 'The month this supply cost was restated for.' SAMPLE_VALUES ('2024-07-01 00:00:00', '2024-08-01 00:00:00')
  )
  METRICS (
    ORDER_ITEMS.TOTAL_LINE_ITEM_REVENUE AS SUM(ORDER_ITEMS.ITEM_PRICE) COMMENT = 'Total line-item value, in cents.',
    PRODUCTS.PRODUCT_COUNT AS COUNT(DISTINCT PRODUCTS.PRODUCT_ID) COMMENT = 'Number of distinct products sold.',
    SUPPLIES.TOTAL_SUPPLY_COST NON ADDITIVE BY (SNAPSHOT_MONTH) AS SUM(SUPPLIES.SUPPLY_COST) COMMENT = 'Cost of supplying the products sold, in cents. Not additive across months.',
    GROSS_MARGIN AS ORDER_ITEMS.TOTAL_LINE_ITEM_REVENUE - SUPPLIES.TOTAL_SUPPLY_COST COMMENT = 'Line-item revenue less the cost of supplying what was sold, in cents.',
    GROSS_MARGIN_RATE AS DIV0(GROSS_MARGIN, ORDER_ITEMS.TOTAL_LINE_ITEM_REVENUE) WITH SYNONYMS ('margin rate', 'margin percentage') COMMENT = 'Gross margin as a share of line-item revenue.'
  )
  COMMENT = 'Line-item revenue against the monthly cost of supplying each product.
Use this view for questions about product margin and supply cost by
product or product type.'
  COPY GRANTS
