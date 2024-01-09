with 

orders as
(
    select * from  {{ ref('stg_jaffle_shop__orders_2') }}
),

payments as 
(
    select * from {{ ref('stg_stripe__payments_2') }}
    where payment_status <> 'fail'
),


payment_max_date_total_amount as
(
     select 
    
     order_id, 
     sum(amount_paid) as total_amount_paid,
     max(payment_date) as payment_finalized_date

     from payments

     group by 1
),

payment_order_details as
(
    select 
    
    orders.*, 
    payment_max_date_total_amount.total_amount_paid,
    payment_max_date_total_amount.payment_finalized_date

    from orders

    left join payment_max_date_total_amount 
    on orders.order_id = payment_max_date_total_amount.order_id
)

select * from payment_order_details

