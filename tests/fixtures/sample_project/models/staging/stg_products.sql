with source as (
    select * from {{ source('raw', 'products') }}
),

staged as (
    select
        product_id,
        product_name,
        product_category,
        product_price,
        created_at
    from source
)

select * from staged
