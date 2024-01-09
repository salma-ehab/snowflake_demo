

  

  











with a as (

    
select

  

   
    "CUSTOMER_ID" 
    
      , 
     
   
    "ORDER_ID" 
    
      , 
     
   
    "ORDER_PLACED_AT" 
    
      , 
     
   
    "ORDER_STATUS" 
    
      , 
     
   
    "TOTAL_AMOUNT_PAID" 
    
      , 
     
   
    "PAYMENT_FINALIZED_DATE" 
    
      , 
     
   
    "CUSTOMER_FIRST_NAME" 
    
      , 
     
   
    "CUSTOMER_LAST_NAME" 
    
      , 
     
   
    "TRANSACTION_SEQ" 
    
      , 
     
   
    "CUSTOMER_SALES_SEQ" 
    
      , 
     
   
    "NVSR" 
    
      , 
     
   
    "CUSTOMER_LIFETIME_VALUE" 
    
      , 
     
   
    "FDOS" 
     
  



from analytics.dbt_sehab.customer_orders_2


),

b as (

    
select

  

   
    "CUSTOMER_ID" 
    
      , 
     
   
    "ORDER_ID" 
    
      , 
     
   
    "ORDER_PLACED_AT" 
    
      , 
     
   
    "ORDER_STATUS" 
    
      , 
     
   
    "TOTAL_AMOUNT_PAID" 
    
      , 
     
   
    "PAYMENT_FINALIZED_DATE" 
    
      , 
     
   
    "CUSTOMER_FIRST_NAME" 
    
      , 
     
   
    "CUSTOMER_LAST_NAME" 
    
      , 
     
   
    "TRANSACTION_SEQ" 
    
      , 
     
   
    "CUSTOMER_SALES_SEQ" 
    
      , 
     
   
    "NVSR" 
    
      , 
     
   
    "CUSTOMER_LIFETIME_VALUE" 
    
      , 
     
   
    "FDOS" 
     
  



from analytics.dbt_sehab.fct_customer_orders_2


),

a_intersect_b as (

    select * from a
    

    intersect


    select * from b

),

a_except_b as (

    select * from a
    

    except


    select * from b

),

b_except_a as (

    select * from b
    

    except


    select * from a

),

all_records as (

    select
        *,
        true as in_a,
        true as in_b
    from a_intersect_b

    union all

    select
        *,
        true as in_a,
        false as in_b
    from a_except_b

    union all

    select
        *,
        false as in_a,
        true as in_b
    from b_except_a

)

{#summary_stats as (

    select

        in_a,
        in_b,
        count(*) as count

    from all_records
    group by 1, 2

),

final as (

    select

        *,
        round(100.0 * count / sum(count) over (), 2) as percent_of_total

    from summary_stats
    order by in_a desc, in_b desc

)#}

select * from all_records



