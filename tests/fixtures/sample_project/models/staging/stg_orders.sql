with source as (
    select * from {{ source('raw', 'orders') }}
),

staged as (
    select
        order_id,
        customer_id,
        product_id,
        quantity,
        unit_price,
        ordered_at,
        status
    from source
)

select * from staged
