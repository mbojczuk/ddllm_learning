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
        updated_at='start_date',
        invalidate_hard_deletes=True
        )   
}}

select
    account_id,
    first_name,
    last_name,
    address,
    email,
    mobile,
    start_date,
    end_date,
    load_timestamp
from {{ source('raw', 'accounts') }}
where date(load_timestamp) = date('{{ var('business_date') }}')

{% endsnapshot %}


{# -- SCD Type 2 Update using MERGE
MERGE INTO delta.default.accounts AS target
USING (
    SELECT 
        source.account_id,
        date_add('day', -1, source.start_date) AS new_end_date
    FROM postgresql.public.accounts AS source
    JOIN delta.default.accounts AS target
        ON source.account_id = target.account_id
        AND target.end_date = date('9999-12-31')
        AND source.start_date > target.start_date
) AS updates
ON target.account_id = updates.account_id
    AND target.end_date = date('9999-12-31')
WHEN MATCHED THEN
    UPDATE SET end_date = updates.new_end_date; #}