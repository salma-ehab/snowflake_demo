with 

base_orders as
(
    select * from {{ source('jaffle_shop', 'orders') }}
),

orders as
(
    select 

    id as order_id,
    user_id as customer_id,
    status as order_status,
    order_date as order_placed_at

    from base_orders
)

select * from orders