{%- macro edts_pit(tracked_entity, hashkey, sat_names, snapshot_relation, dimension_key, edts=none,snapshot_trigger_column=none, ldts=none, custom_rsrc=none, ledts=none, sdts=none, pit_type=none) -%}

    {# Applying the default aliases as stored inside the global variables, if ldts, sdts and ledts are not set. #}

    {%- set ldts = datavault4dbt.replace_standard(ldts, 'datavault4dbt.ldts_alias', 'ldts') -%}
    {%- set ledts = datavault4dbt.replace_standard(ledts, 'datavault4dbt.ledts_alias', 'ledts') -%}
    {%- set sdts = datavault4dbt.replace_standard(sdts, 'datavault4dbt.sdts_alias', 'sdts') -%}

    {{ return(adapter.dispatch('edts_pit')(pit_type=pit_type,
                                                        tracked_entity=tracked_entity,
                                                        hashkey=hashkey,
                                                        sat_names=sat_names,
                                                        ldts=ldts,
                                                        sdts=sdts,
                                                        custom_rsrc=custom_rsrc,
                                                        ledts=ledts,
                                                        snapshot_relation=snapshot_relation,
                                                        snapshot_trigger_column=snapshot_trigger_column,
                                                        dimension_key=dimension_key,
                                                        edts=edts)) }}

{%- endmacro -%}

{%- macro snowflake__edts_pit(tracked_entity, hashkey, sat_names, ldts, ledts, sdts, snapshot_relation, dimension_key,edts=none,snapshot_trigger_column=none, custom_rsrc=none, pit_type=none) -%}

{%- set hash = var('datavault4dbt.hash', 'MD5') -%}
{%- set hash_dtype = var('datavault4dbt.hash_datatype', 'STRING') -%}
{%- set hash_default_values = fromjson(datavault4dbt.hash_default_values(hash_function=hash,hash_datatype=hash_dtype)) -%}
{%- set hash_alg = hash_default_values['hash_alg'] -%}
{%- set unknown_key = hash_default_values['unknown_key'] -%}
{%- set error_key = hash_default_values['error_key'] -%}

{%- set rsrc = var('datavault4dbt.rsrc_alias', 'rsrc') -%}

{%- set beginning_of_all_times = datavault4dbt.beginning_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}
{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{% if edts is not none %}
    {%- set sn_col = [edts] -%}
{%- else -%}
    {%- set sn_col = [sdts] -%}
{%- endif -%}

{%- if datavault4dbt.is_something(pit_type) -%}
    {%- set quote = "'" -%}
    {%- set pit_type_quoted = quote + pit_type + quote -%}
    {%- set hashed_cols = [pit_type_quoted, datavault4dbt.prefix([hashkey],'te'), datavault4dbt.prefix(sn_col, 'snap')] -%}
{%- else -%}
    {%- set hashed_cols = [datavault4dbt.prefix([hashkey],'te'), datavault4dbt.prefix(sn_col, 'snap')] -%}
{%- endif -%}

{{ datavault4dbt.prepend_generated_by() }}
--custom pit
WITH

{%- if is_incremental() %}

existing_dimension_keys AS (

    SELECT
        {{ dimension_key }}
    FROM {{ this }}

),

{%- endif %}

{% if edts is not none %}
effective_snapshot AS (
    SELECT b.sdts AS {{edts}}, l.*
	FROM {{ ref(snapshot_relation) }} b
	CROSS JOIN  {{ ref(snapshot_relation) }} l
	WHERE b.is_active_edts
	AND l.is_active
	AND l.is_latest
),
{% endif %}
{% for satellite in sat_names %}
--{{ satellite}}
{%- set sat_columns = datavault4dbt.source_columns(ref(satellite)) %}
--{{sat_columns}}
{###### if ldts already in table ignore (will be handled later) #######}
cte_{{satellite}} as
(
    {% if edts is not none %}
    with edts_dom as
    (
                SELECT distinct 
                {{ hashkey}},
                {{edts}}
            FROM {{ ref(satellite) }}
    ),
    edts_end as 
    (
        SELECT 
            {{ hashkey}},
            {{edts}},
            COALESCE(LEAD({{edts}} - INTERVAL '1 MICROSECOND') OVER (PARTITION BY {{ hashkey }} ORDER BY {{edts}}),TO_TIMESTAMP('8888-12-31T23:59:59', 'YYYY-MM-DDTHH24:MI:SS')) AS eedts
        FROM edts_dom
    )
    {% endif %}
    SELECT
        {{ datavault4dbt.prefix([hashkey], 'sat') }},
        {{ datavault4dbt.prefix([ldts], 'sat') }},
        {% if edts is not none %}
        COALESCE(LEAD({{ ldts }} - INTERVAL '1 MICROSECOND') OVER (PARTITION BY {{ datavault4dbt.prefix([hashkey], 'sat') }}, {{datavault4dbt.prefix([edts], 'sat')}} ORDER BY {{ ldts }}),{{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}) AS ledts,
        {{ datavault4dbt.prefix([edts], 'sat') }},
        edts_end.eedts
        {% else %}
            {%- if not ledts|string|lower in sat_columns|map('lower') %}
                COALESCE(LEAD({{ ldts }} - INTERVAL '1 MICROSECOND') OVER (PARTITION BY {{ datavault4dbt.prefix([hashkey], 'sat') }} ORDER BY {{ datavault4dbt.prefix([ldts], 'sat') }}),{{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}) AS ledts
            {% else %}
                {{ sat }}.{{ ledts }}
            {% endif %}
        {% endif %}
    FROM {{ ref(satellite) }} sat
    {% if edts is not none %}
    inner join edts_end
        on {{datavault4dbt.prefix([hashkey], 'sat')}} = {{datavault4dbt.prefix([hashkey], 'edts_end')}}
        and {{datavault4dbt.prefix([edts], 'sat')}} = {{datavault4dbt.prefix([edts], 'edts_end')}}
    {% endif %}
),
{% endfor %}
pit_records AS (

    SELECT
        
        {% if datavault4dbt.is_something(pit_type) -%}
            '{{ datavault4dbt.as_constant(pit_type) }}' as type,
        {%- endif %}
        {% if datavault4dbt.is_something(custom_rsrc) -%}
        '{{ custom_rsrc }}' as {{ rsrc }},
        {%- endif %}
        {{ datavault4dbt.hash(columns=hashed_cols,
                    alias=dimension_key,
                    is_hashdiff=false)   }} ,
        te.{{ hashkey }},
        snap.{{ sdts }},
        {% if edts is not none %}
        snap.edts,
        {%- for satellite in sat_names %}
            COALESCE({{ satellite }}.{{ hashkey }}, CAST({{ datavault4dbt.as_constant(column_str=unknown_key) }} AS {{ hash_dtype }})) AS hk_{{ satellite }},
            COALESCE({{ satellite }}.{{ ldts }}, {{ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) }}) AS {{ ldts }}_{{ satellite }},
            COALESCE({{ satellite }}.edts, {{ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) }}) edts_{{ satellite }}
            {{- "," if not loop.last }}
        {%- endfor %}
        {%- else %}
        {%- for satellite in sat_names %}
            COALESCE({{ satellite }}.{{ hashkey }}, CAST({{ datavault4dbt.as_constant(column_str=unknown_key) }} AS {{ hash_dtype }})) AS hk_{{ satellite }},
            COALESCE({{ satellite }}.{{ ldts }}, {{ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) }}) AS {{ ldts }}_{{ satellite }}
            {{- "," if not loop.last }}
        {%- endfor %}
        {%- endif %}

    FROM
            {{ ref(tracked_entity) }} te
        FULL OUTER JOIN
        {% if edts is not none %}
        --with business date
            effective_snapshot snap
        {%- else %}
        --without business date
            {{ ref(snapshot_relation) }} snap
            {% if datavault4dbt.is_something(snapshot_trigger_column) -%}
                ON snap.{{ snapshot_trigger_column }} = true
            {% else -%}
                ON 1=1
            {%- endif %}
        {%- endif %}
        {% for satellite in sat_names %}
        {%- set sat_columns = datavault4dbt.source_columns(ref(satellite)) %}
        LEFT JOIN {{ "cte_" ~ satellite }} {{ satellite }}
            ON
                {{ satellite }}.{{ hashkey}} = te.{{ hashkey }}
                AND snap.{{ sdts }} BETWEEN {{ satellite }}.{{ ldts }} AND {{ satellite }}.{{ ledts }}
                {% if edts is not none %}
                AND snap.{{edts}} BETWEEN {{ satellite }}.{{edts}} AND {{ satellite }}.eedts
                {% endif %}
        {% endfor %}
    {% if datavault4dbt.is_something(snapshot_trigger_column) %}
         WHERE snap.{{ snapshot_trigger_column }} 
    {%- endif %}

),

records_to_insert AS (

    SELECT DISTINCT *
    FROM pit_records
    {%- if is_incremental() %}
    WHERE {{ dimension_key }} NOT IN (SELECT * FROM existing_dimension_keys)
    {% endif -%}

)

SELECT * FROM records_to_insert

{%- endmacro -%}