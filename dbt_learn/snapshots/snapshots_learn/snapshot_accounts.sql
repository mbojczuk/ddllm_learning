{# 
    Name: snapshots_accounts
    Description: SCD2 type table for account information
    source: public.accounts 
    Run: dbt snapshot -s snapshot_accounts --vars '{"business_date" : "2024-11-04"}'
#}

{% snapshot snapshot_accounts %}

{{
    config(
        target_schema='dbt_snapshots',   
        unique_key='account_id',      
        strategy='timestamp',
        updated_at='start_time',
        invalidate_hard_deletes=True,
        tags=['accounts', 'customers']
        )   
}}

select
    account_id,
    first_name,
    last_name,
    address,
    email,
    mobile,
    start_time,
    end_time,
    delete_flag,
    load_timestamp
from {{ source('raw', 'accounts') }}
where date(load_timestamp) = date('{{ var('business_date') }}')

{% endsnapshot %}
