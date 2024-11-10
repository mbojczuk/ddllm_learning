{{
  config(
    materialized = 'simple_scd2',
    unique_key = 'account_id',
    strategy = 'merge'
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
where start_date = date('{{ var('business_date') }}')