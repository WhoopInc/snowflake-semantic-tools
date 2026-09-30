-- jaffle_minimal golden: SST_REF_DEV.JAFFLE.JAFFLE_MINIMAL, rendered from
-- semantic_models/semantic_views/semantic_views.yml on the dev target. The DDL
-- below is what SST renders, and tests/unit/test_golden_ddl.py compares it byte
-- for byte. The statement has been created in Snowflake, in a scratch schema.
--
-- The view authors only name, description and one table, so the rest comes
-- from the products model or attaches through `products`:
--   - PRIMARY KEY (PRODUCT_ID) comes from the model's config.meta.sst.
--   - PRODUCT_COUNT attaches because its only table is products. No derived
--     metric attaches: each depends on a metric from a table this view lacks.
--   - No RELATIONSHIPS: the view has one table.
--   - No AI_VERIFIED_QUERIES: product_mix_by_type also needs order_items and
--     orders, and a member attaches only where all its tables are present.
--   - No filters: all four are on orders.
--   - No VARIABLES, AI_SQL_GENERATION, AI_QUESTION_CATEGORIZATION,
--     MAX_STALENESS or WITH TAG: the view declares none, and custom
--     instructions attach only to views that name them.
--   - SAMPLE_VALUES renders on the fact LIST_PRICE as well as on dimensions;
--     IS_ENUM follows it, only where is_enum is true.
--   - COPY GRANTS is always rendered.
--
-- The fixture loads with no diagnostics. Not comparable raw to GET_DDL; see
-- tests/golden/README.md.

CREATE OR REPLACE SEMANTIC VIEW SST_REF_DEV.JAFFLE.JAFFLE_MINIMAL
  TABLES (
    PRODUCTS AS SST_REF_DEV.JAFFLE.PRODUCTS PRIMARY KEY (PRODUCT_ID)
  )
  FACTS (
    PRODUCTS.LIST_PRICE AS PRODUCTS.LIST_PRICE WITH SYNONYMS ('menu price', 'catalogue price', 'sticker price') COMMENT = 'Menu price for one unit, in cents.' SAMPLE_VALUES ('1100', '700')
  )
  DIMENSIONS (
    PRODUCTS.PRODUCT_ID AS PRODUCTS.PRODUCT_ID WITH SYNONYMS ('product key', 'menu item identifier') COMMENT = 'Surrogate key for the product. An identifier, therefore a dimension.',
    PRODUCTS.PRODUCT_NAME AS PRODUCTS.PRODUCT_NAME WITH SYNONYMS ('menu item', 'item name', 'dish') COMMENT = 'The menu item''s name as printed on the receipt.' SAMPLE_VALUES ('nutellaphone who dis', 'doctor stew'),
    PRODUCTS.PRODUCT_TYPE AS PRODUCTS.PRODUCT_TYPE WITH SYNONYMS ('item category', 'menu category', 'food or drink') COMMENT = 'Whether the item is food or drink. A closed set.' SAMPLE_VALUES ('jaffle', 'beverage') IS_ENUM
  )
  METRICS (
    PRODUCTS.PRODUCT_COUNT AS COUNT(DISTINCT PRODUCTS.PRODUCT_ID) COMMENT = 'Number of distinct products sold.'
  )
  COMMENT = 'Menu products only. The smallest valid semantic view in this fixture.'
  COPY GRANTS
