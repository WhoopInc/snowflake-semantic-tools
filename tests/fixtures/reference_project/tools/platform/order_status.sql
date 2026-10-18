BEGIN
  RETURN (
    SELECT MAX(orders.order_state)
    FROM {{ ref('orders') }} AS orders
    WHERE orders.order_id = :order_id
  );
END
