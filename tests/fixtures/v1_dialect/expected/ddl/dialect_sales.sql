CREATE OR REPLACE SEMANTIC VIEW SST_REF_DEV.JAFFLE.DIALECT_SALES
  TABLES (
    ORDERS AS SST_REF_DEV.JAFFLE.ORDERS PRIMARY KEY (ORDER_ID),
    CUSTOMERS AS SST_REF_DEV.JAFFLE.CUSTOMERS PRIMARY KEY (CUSTOMER_ID) UNIQUE (CUSTOMER_NAME),
    LOCATIONS AS SST_REF_DEV.JAFFLE.LOCATIONS PRIMARY KEY (LOCATION_ID)
  )
  RELATIONSHIPS (
    ORDERS_TO_CUSTOMERS AS ORDERS (CUSTOMER_ID) REFERENCES CUSTOMERS (CUSTOMER_ID),
    ORDERS_TO_LOCATIONS AS ORDERS (LOCATION_ID) REFERENCES LOCATIONS (LOCATION_ID)
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
    ORDERS.DIALECT_COMPLETED_ORDER LABELS = (FILTER) AS ORDERS.ORDER_STATE = 'completed' COMMENT = 'Orders that completed.',
    ORDERS.LOCATION_ID AS ORDERS.LOCATION_ID WITH SYNONYMS ('shop key', 'where the order was placed') COMMENT = 'The store location where this order was placed.',
    ORDERS.ORDERED_AT AS ORDERS.ORDERED_AT WITH SYNONYMS ('order date', 'purchase time', 'when the order was placed') COMMENT = 'When the order was placed.' SAMPLE_VALUES ('2024-07-04 12:15:00', '2024-07-04 18:42:00'),
    ORDERS.ORDER_ID AS ORDERS.ORDER_ID WITH SYNONYMS ('order key', 'order number') COMMENT = 'Surrogate key for the order. An identifier, therefore a dimension.',
    ORDERS.ORDER_STATE AS ORDERS.ORDER_STATE WITH SYNONYMS ('order status', 'fulfilment state') COMMENT = 'Fulfilment state of the order. A closed set.' SAMPLE_VALUES ('completed', 'returned', 'placed') IS_ENUM
  )
  METRICS (
    ORDERS.DIALECT_ORDER_COUNT AS COUNT(DISTINCT ORDERS.ORDER_ID) COMMENT = 'Distinct orders placed.',
    ORDERS.DIALECT_REVENUE AS SUM(ORDERS.ORDER_TOTAL) COMMENT = 'Order value including tax, in cents.'
  )
  COMMENT = 'Orders and the customers and locations behind them, authored in the 0.3
dialect.'
  AI_SQL_GENERATION 'Monetary columns are stored in cents; divide by one hundred for currency.'
  AI_QUESTION_CATEGORIZATION 'Decline questions about an individual named customer.'
  COPY GRANTS;
