with

orders as
(
    select * from {{ ref('stg_jaffle_shop__orders') }}
    where order_status NOT IN ('pending')
),

payments as 
(
    select * from {{ ref('stg_stripe__payments') }}
    where payment_status != 'fail'
),

order_total_amount as 
(

    select

    order_id,
    payment_status,
    sum(payment_value_dollars) as order_value_dollars

    from payments
    group by 1, 2

),

order_payment_details as
(
   select 

   orders.*,
   order_total_amount.payment_status,
   order_total_amount.order_value_dollars

   from orders
   left join order_total_amount
   on orders.order_id = order_total_amount.order_id

)

select * from order_payment_details