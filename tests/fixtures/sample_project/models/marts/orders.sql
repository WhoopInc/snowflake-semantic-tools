with orders as (
    select
        o.order_id,
        o.customer_id,
        o.product_id,
        o.ordered_at,
        o.quantity,
        o.unit_price,
        o.quantity * o.unit_price as order_total,
        o.status
    from {{ ref('stg_orders') }} o
)

select * from orders
