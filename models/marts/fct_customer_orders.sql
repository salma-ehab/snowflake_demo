with 

customers as
(
    select * from {{ ref('stg_jaffle_shop__customers') }}
),

payments_orders as
(
    select * from {{ ref('int_payments_orders') }}
),


customer_order_history as
(
    select 

    customers.*,

    min(payments_orders.order_date) as first_order_date,
    min(payments_orders.valid_order) as first_non_returned_order_date,
    max(payments_orders.valid_order) as most_recent_non_returned_order_date,
    count(*) as order_count,
    sum (nvl2(payments_orders.valid_order,1,0)) as non_returned_order_count,
    sum(nvl2(payments_orders.valid_order,payments_orders.order_value_dollars,0))as total_lifetime_value,

    total_lifetime_value / non_returned_order_count as avg_non_returned_order_value,
    array_agg(distinct payments_orders.order_id) as order_ids

    from payments_orders
    
    join customers
    on payments_orders.customer_id= customers.customer_id

    group by customers.customer_id, customers.full_name, customers.surname, customers.givenname
),

final as
( 
    select

    payments_orders.order_id,
    customer_order_history.customer_id,
    customer_order_history.surname,
    customer_order_history.givenname,
    customer_order_history.first_order_date,
    customer_order_history.order_count,
    customer_order_history.total_lifetime_value,
    payments_orders.order_value_dollars,
    payments_orders.order_status,
    payments_orders.payment_status

    from customer_order_history
    inner join payments_orders 
    on customer_order_history.customer_id = payments_orders.customer_id

)

select * from final

