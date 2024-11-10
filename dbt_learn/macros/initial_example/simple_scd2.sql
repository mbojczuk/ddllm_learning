{% materialization simple_scd2, adapter='default' %}

    {# sets config to the config attribute of the model dictionary, 
    which contains configuration parameters for the dbt model being processed #}
    {%- set config = model['config'] -%}
    {# sets target_table to either the alias of the model
    or the name of the model, this determines the final name
    of the table in the database #}
    {%- set target_table = model.get('alias', model.get('name')) -%}
    {%- set unique_key = config.get('unique_key') %}
    {%- set strategy_name = config.get('strategy') -%}

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

    {# get all column names from this relation #}
    {% set column_list = adapter.get_columns_in_relation(this) %}

    {%- if not target_relation.is_table -%}
        {% do exceptions.relation_wrong_type(target_relation, 'table') %}
    {%- endif -%}

    {# inside_transaction=False: Runs hooks outside of the transaction scope, allowing for setup tasks that should persist even if the snapshot transaction fails. #}
    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    {# inside_transaction=True: Runs hooks inside the transaction, ensuring that any pre-setup actions are rolled back if the snapshot fails. #}
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {# If the table does not yet exist #}
    {% if not target_relation_exists %}
        {%- set build_sql = create_trino_table(temp_relation, target_relation, sql) -%}
    {% else %}
        {%- set build_sql = scd2_merge(temp_relation, target_relation, sql) -%}
    {% endif %}

    {% call statement('main') %}
      {{ build_sql }}
    {% endcall %}

    {# Run post-hooks and ensure temp table cleanup #}
    {{ run_hooks(post_hooks, inside_transaction=True) }}
    {{ run_hooks(post_hooks, inside_transaction=False) }}
    {{ adapter.drop_relation(temp_relation) }}

    {{- return({'relations':[target_relation]}) -}}

{% endmaterialization %}

{% macro create_trino_table(temp_relation, target_relation, sql) %}
    {{- get_create_table_as_sql(True, temp_relation, sql) -}}
    {% set create_table %}
        select
            {%- for column in columns -%}
                {{ column.name }}{% if not loop.last %}, {% endif %}
            {%- endfor -%},
            date'9999-12-31' as end_date
        from {{ temp_relation }}
    {% endset %}

    {{- get_create_table_as_sql(False, target_relation , create_table) -}}
{% endmacro %}

{% macro scd2_merge(temp_relation, target_relation, sql) %}
    {# Step 1: Create the temp table with initial data #}
    {{ get_create_table_as_sql(True, temp_relation, sql) }}

    {# Step 2: Define the MERGE statement to update existing records or insert new ones #}
    MERGE INTO {{ target_relation }} AS target
    USING (
        SELECT 
            source.account_id,
            date_add('day', -1, source.start_date) AS new_end_date,
            source.first_name, 
            source.last_name, 
            source.address, 
            source.email, 
            source.mobile, 
            source.start_date
        FROM {{ temp_relation }} AS source
        JOIN {{ target_relation }} AS target
            ON source.account_id = target.account_id
            AND target.end_date = date'9999-12-31'
            AND source.start_date > target.start_date
    ) AS updates
    ON target.account_id = updates.account_id
        AND target.end_date = date'9999-12-31'
    WHEN MATCHED THEN
        UPDATE SET end_date = updates.new_end_date;

    INSERT INTO {{ target_relation }}
    (
	select 
		{{ temp_relation }}.*,
		date('9999-12-31') as end_date
	from {{ temp_relation }}
    );
{% endmacro %}