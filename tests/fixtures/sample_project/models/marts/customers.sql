with customer_orders as (
    select
        customer_id,
        min(ordered_at) as first_ordered_at,
        max(ordered_at) as last_ordered_at,
        sum(quantity * unit_price) as lifetime_spend,
        count(distinct order_id) as order_count
    from {{ ref('stg_orders') }}
    group by customer_id
),

customers as (
    select
        c.customer_id,
        c.customer_name,
        c.email,
        co.first_ordered_at,
        co.last_ordered_at,
        coalesce(co.lifetime_spend, 0) as lifetime_spend,
        coalesce(co.order_count, 0) as order_count,
        case
            when co.order_count > 1 then 'returning'
            else 'new'
        end as customer_type
    from {{ ref('stg_customers') }} c
    left join customer_orders co on c.customer_id = co.customer_id
)

select * from customers
