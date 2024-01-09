with 

customers as 
(
    select * from {{ ref('stg_jaffle_shop__customers_2') }}
),

orders as
(
    select * from  {{ ref('stg_jaffle_shop__orders_2') }}
),


customer_order_details as 
(
    select 
    
    customers.customer_id,
    customers.customer_first_name,
    customers.customer_last_name,
    min(orders.order_placed_at) as fdos, 
    max(orders.order_placed_at),  
    count(orders.order_id) 

    from customers 

    left join orders
    on  customers.customer_id = orders.customer_id
    
    group by 1,2,3
)

select * from customer_order_details