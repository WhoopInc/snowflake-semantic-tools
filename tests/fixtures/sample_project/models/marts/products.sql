with products as (
    select
        product_id,
        product_name,
        product_category,
        product_price,
        created_at
    from {{ ref('stg_products') }}
)

select * from products
