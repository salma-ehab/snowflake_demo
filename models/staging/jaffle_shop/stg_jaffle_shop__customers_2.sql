with 

base_customers as
(
    select * from {{ source('jaffle_shop', 'customers') }}
),

customers as
(
    select 

    id as customer_id,
    first_name as customer_first_name,
    last_name as customer_last_name
    
    from base_customers
)

select * from customers