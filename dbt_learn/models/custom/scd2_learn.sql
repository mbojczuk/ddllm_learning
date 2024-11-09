{{
  config(
    materialized = 'simple_scd2'
    )
}}

select
    account_id,
    first_name,
    last_name,
    address,
    email,
    mobile,
    start_date
from {{ source('raw', 'accounts') }}