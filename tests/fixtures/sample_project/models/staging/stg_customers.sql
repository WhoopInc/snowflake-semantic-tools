with source as (
    select * from {{ source('raw', 'customers') }}
),

staged as (
    select
        customer_id,
        customer_name,
        email,
        created_at,
        updated_at
    from source
)

select * from staged
