{# run as dbt run -s scd2_learn --vars "{'business_date' : '2024-10-13'}"  #}

{{
  config(
    materialized = 'simple_scd2',
    unique_key = 'account_id',
    updated_at = 'start_date'
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