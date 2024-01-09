with 

customer_order_details as 
(
    select * from {{ ref('int_customer_order_details_2') }}
),

payment_order_details as
(
    select * from  {{ ref('int_payment_order_details_2') }}
),


final as
(
    select
      
    customer_order_details.customer_id,
    payment_order_details.order_id,
    payment_order_details.order_placed_at,
    payment_order_details.order_status,
    payment_order_details.total_amount_paid,
    payment_order_details.payment_finalized_date,
    customer_order_details.customer_first_name,
    customer_order_details.customer_last_name,

    row_number() over (order by payment_order_details.order_id) as transaction_seq,
    row_number() over (partition by  customer_order_details.customer_id order by payment_order_details.order_id) as customer_sales_seq,

    {# In case the customer places multiple orders on the initial day #}
    case when  customer_sales_seq = 1
    then 'new'
    else 'return' 
    end as nvsr,

    sum (payment_order_details.total_amount_paid)
         over (partition by customer_order_details.customer_id
         order by payment_order_details.order_placed_at, payment_order_details.order_id)
         as customer_lifetime_value,

    customer_order_details.fdos

    from payment_order_details 

    left join customer_order_details
    on customer_order_details.customer_id = payment_order_details.customer_id
)

select * from final