{% materialization simple_scd2, adapter='default' %}

    {# sets config to the config attribute of the model dictionary, 
    which contains configuration parameters for the dbt model being processed #}
    {%- set config = model['config'] -%}
    {# sets target_table to either the alias of the model
    or the name of the model, this determines the final name
    of the table in the database #}
    {%- set target_table = model.get('alias', model.get('name')) -%}
    {%- set unique_key = config.get('unique_key') %}
    {%- set updated_at = config.get('updated_at') -%}

    {{ log(unique_key, info=True) }}
    {{ log(updated_at, info=True) }}
    {{ log(target_table, info=True) }}
    {{ log(config, info=True) }}
    {{ log(model, info=True) }}
    {{ log(this, info=True) }}

    {# The code attempts to retrieve the target table where the snapshot data will be stored #}
    {% set target_relation_exists, target_relation = get_or_create_relation(
            database=model.database,
            schema=model.schema,
            identifier=target_table,
            type='table') -%}

    {% set temp_relation = make_temp_relation(target_relation) %}

    {# Drop the temp table if it exists - pre-hook incase it is randomly lingering from before #}
    {% set temp_relation_exists = adapter.get_relation(
        database=temp_relation.database,
        schema=temp_relation.schema,
        identifier=temp_relation.identifier
    ) is not none %}

    {% if temp_relation_exists %}
        {{ adapter.drop_relation(temp_relation) }}
    {% endif %} 

    {# logging compiled code #}
    {{ log(model['compiled_code'], info=True)  }}

    {%- if not target_relation.is_table -%}
        {% do exceptions.relation_wrong_type(target_relation, 'table') %}
    {%- endif -%}

    {# inside_transaction=False: Runs hooks outside of the transaction scope, allowing for setup tasks that should persist even if the snapshot transaction fails. #}
    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    {# inside_transaction=True: Runs hooks inside the transaction, ensuring that any pre-setup actions are rolled back if the snapshot fails. #}
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {{ log(temp_relation, info=True) }}

    {# If the table does not yet exist #}
    {% if not target_relation_exists %}
        {%- set build_sql = create_trino_table(target_relation, model['compiled_code'], updated_at, unique_key) -%}
    {% else %}
        {%- set build_sql = scd2_merge(temp_relation, target_relation, model['compiled_code'], updated_at, unique_key) -%}
    {% endif %}

    {# literally a main function call once we set what to do when the variable is called was called #}
    {% call statement('main') %}
      {{ build_sql }}
    {% endcall %}

    {# Run post-hooks and ensure temp table cleanup #}
    {{ run_hooks(post_hooks, inside_transaction=True) }}
    {{ run_hooks(post_hooks, inside_transaction=False) }}
    {{ adapter.drop_relation(temp_relation) }}

    {{- return({'relations':[target_relation]}) -}}

{% endmaterialization %}

{% macro create_trino_table(target_relation, sql, updated_at, unique_key) %}
    {% set create_table %}
        SELECT
            *,
            {{ updated_at }} as dbt_valid_from,
            date '9999-12-31' as dbt_valid_to,
            to_hex(md5(cast(cast({{ unique_key }} as varchar) as varbinary))) as dbt_scd_id,
            0 as dbt_dlet_flag
        FROM (
            {{ sql }}
        ) sbq
    {% endset %}

    {# once you set the sql this function will run table creation #}
    {{ get_create_table_as_sql(False, target_relation, create_table) }}
{% endmacro %}


{% macro scd2_merge(temp_relation, target_relation, sql, updated_at, unique_key) %}
    {# Step 1: Create the temp table with initial data, temp relation is the ___dbt_temp suffixed table #}
    {{ get_create_table_as_sql(True, temp_relation, sql) }}

    {# Step 2: Define the MERGE statement to update existing records or insert new ones #}
    MERGE INTO {{ target_relation }} AS target
    USING (
        WITH cte as (
            SELECT 
                *,
                {{ updated_at }} as dbt_valid_from,
                date '9999-12-31' as dbt_valid_to,
                to_hex(md5(cast(cast({{ unique_key }} as varchar) as varbinary))) as dbt_scd_id,
                0 as dbt_dlet_flag
            FROM {{ temp_relation }}
        )
        SELECT 
            source.*
        FROM cte AS source
        JOIN {{ target_relation }} AS target
            ON source.dbt_scd_id = target.dbt_scd_id
            AND target.dbt_valid_to = date'9999-12-31'
            AND source.dbt_valid_from > target.dbt_valid_from
    ) AS updates
    ON target.dbt_scd_id = updates.dbt_scd_id
        AND target.dbt_valid_to = date'9999-12-31'
    WHEN MATCHED THEN
        UPDATE SET dbt_valid_to = date_add('day', -1, updates.{{ updated_at }});

    INSERT INTO {{ target_relation }}
    SELECT * FROM cte;
{% endmacro %}