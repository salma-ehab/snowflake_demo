with 

base_payments as
(
    select * from {{ source('stripe', 'payment') }}
),

payments as
(
    select 

    id as payment_id,
    orderid as order_id,
    status as payment_status,
    amount/100 as amount_paid,
    created as payment_date

    from base_payments
)

select * from payments