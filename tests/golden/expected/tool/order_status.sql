CREATE OR REPLACE PROCEDURE SST_REF_DEV.JAFFLE.ORDER_STATUS(ORDER_ID VARCHAR)
  COPY GRANTS
  RETURNS VARCHAR
  LANGUAGE SQL
  COMMENT = 'Returns the status of one order, given its id. Answers where an order is, not how many orders there were -- that is a question for the Analyst tools.'
  EXECUTE AS CALLER
  AS $$
BEGIN
  RETURN (
    SELECT MAX(orders.order_state)
    FROM SST_REF_DEV.JAFFLE.ORDERS AS orders
    WHERE orders.order_id = :order_id
  );
END
$$
