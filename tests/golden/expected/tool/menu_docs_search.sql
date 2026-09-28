CREATE OR REPLACE CORTEX SEARCH SERVICE SST_REF_DEV.JAFFLE.MENU_DOCS_SEARCH
  ON PRODUCT_DESCRIPTION
  ATTRIBUTES PRODUCT_TYPE, DIETARY_CLASS
  WAREHOUSE = SST_REF_WH
  TARGET_LAG = '1 hour'
  EMBEDDING_MODEL = 'snowflake-arctic-embed-m-v1.5'
  COMMENT = 'Full-text search over menu item DESCRIPTIONS -- the prose saying what is in each item. Answers what an item contains, how items differ, and which items suit a diet. Filter on `dietary_class` for vegan, vegetarian or omnivore rather than searching the prose for the word vegetarian. Does NOT answer questions about how many were sold -- that is an order question and belongs to the Analyst tools.'
  AS SELECT PRODUCT_ID, PRODUCT_NAME, PRODUCT_TYPE, DIETARY_CLASS, PRODUCT_DESCRIPTION FROM SST_REF_DEV.JAFFLE.PRODUCT_DOCS