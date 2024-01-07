select
    id as order_id,
    user_id as customer_id,
    order_date,
    status
from {{ source('jaffle_shop', 'orders') }}

{{ limit_data(time_stamp_column = 'order_date',time_selection= 'year',dev_years_of_data = 7) }}