{# A macro for creating a transaction table and setting up the partitioning #}

{% macro create_incremental_table_if_not_exists() %}
    CREATE TABLE IF NOT EXISTS "delta"."default"."stg_transactions" (
        transaction_id BIGINT,
        account_id BIGINT,
        transaction_date DATE,
        amount DECIMAL,
        transaction_type VARCHAR,
        description VARCHAR,
        start_time DATE
    )
    WITH (
        partitioned_by = ARRAY['start_time'],
    )
{% endmacro %}
