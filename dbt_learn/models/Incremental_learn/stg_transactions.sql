{# 
    Name: stg_transactions
    Description: Incremental model for transactions for customers (learning)
    source: public.transactions 
    Run: dbt run -s stg_transactions --vars '{"business_date" : "2024-11-04"}'
#}

{{ config(
    materialized = 'incremental',
    unique_key = 'transaction_id',
    incremental_strategy='delete+insert',
    file_format = 'parquet'
) }}

-- Pre-hook to create table with partitions

-- Conditionally create the table if it doesn't exist, using the macro
{% if is_incremental() == false %}
    {{ create_incremental_table_if_not_exists() }}
{% endif %}

-- Model logic
with new_transactions as (
  Select
    transaction_id,
    account_id,
    transaction_date,
    amount,
    transaction_type,
    description,
    date(start_time) as start_time
  from {{ source('raw', 'transactions') }}

  {% if is_incremental() %}
    where date(load_timestamp) = date('{{ var('business_date') }}')
  {% endif %}
)

Select *
from new_transactions
