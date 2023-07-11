{%- macro status_tracking_sat_v0(tracked_hashkey, stage_source_model=none, load_type='full', src_ldts=none, src_rsrc=none, src_edts=none,edts_hashkey=none) -%}

    {# Applying the default aliases as stored inside the global variables, if src_ldts and src_rsrc are not set. #}

    {%- set src_ldts = datavault4dbt.replace_standard(src_ldts, 'datavault4dbt.ldts_alias', 'ldts') -%}
    {%- set src_rsrc = datavault4dbt.replace_standard(src_rsrc, 'datavault4dbt.rsrc_alias', 'rsrc') -%}

    {{ adapter.dispatch('status_tracking_sat_v0', 'datavault4dbt')(tracked_hashkey=tracked_hashkey,
                                                                    stage_source_model=stage_source_model,
                                                                    load_type=load_type,
                                                                    src_ldts=src_ldts,
                                                                    src_rsrc=src_rsrc,
                                                                    src_edts=src_edts,
                                                                    edts_hashkey=edts_hashkey
                                                                    ) }}

{%- endmacro -%}

{%- macro snowflake__status_tracking_sat_v0(tracked_hashkey, stage_source_model, load_type, src_ldts, src_rsrc, src_edts, edts_hashkey) -%}


{{- datavault4dbt.check_required_parameters(tracked_hashkey=tracked_hashkey, stage_source_model=stage_source_model, load_type=load_type,
                                       src_ldts=src_ldts, src_rsrc=src_rsrc) -}}

{%- if  is_nothing(src_edts) -%}
    {%- set src_edts = none -%}
{%- endif -%}

{%- if  src_edts is not none -%}
{%- set source_cols = datavault4dbt.expand_column_list(columns=[tracked_hashkey, src_rsrc, src_ldts, src_edts, edts_hashkey]) -%}
{% else %}
{%- set source_cols = datavault4dbt.expand_column_list(columns=[tracked_hashkey, src_rsrc, src_ldts]) -%}
{%- endif -%}

{%- if  src_edts is not none -%}
    {%- set source_cols = datavault4dbt.expand_column_list(columns=[tracked_hashkey, src_rsrc, src_ldts, src_edts, edts_hashkey]) -%}
{% else %}
    {%- set source_cols = datavault4dbt.expand_column_list(columns=[tracked_hashkey, src_rsrc, src_ldts]) -%}
{%- endif -%}


{%- set beginning_of_all_times = datavault4dbt.beginning_of_all_times() -%}
{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}
{%- set unknown_key = get_dict_hash_value("unknown_key") -%}
{%- set source_relation = ref(stage_source_model) -%}
 
WITH

{#
    Get all records from staging layer where driving key and secondary foreign keys are not null.
    Deduplicate over HK+Driving Key uneuqls the previous (regarding src_ldts) combination.
#}
{%- if is_incremental() %}
cte_current_sts as
(
    select {{ datavault4dbt.prefix(source_cols, 'sts') }}, cdc
    from {{ this }} sts
    qualify row_number() over (PARTITION BY {{ datavault4dbt.prefix([tracked_hashkey], 'sts') }} order by {{ datavault4dbt.prefix([src_ldts], 'sts') }} desc) = 1
)
{%- else %}
cte_current_sts as
(
    {%- if  src_edts is not none %}
    select    '{{ unknown_key }}' as {{ tracked_hashkey }}
            , '' {{ src_rsrc }}
            , {{ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) }} {{ src_ldts }}
            , {{ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) }} {{ src_edts }}
            , 'I' as cdc
            , '{{ unknown_key }}' as {{ edts_hashkey }}
    {%- else %}
    select    '{{ unknown_key }}' as {{ tracked_hashkey }}
            , '' {{ src_rsrc }}
            , {{ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) }} {{ src_ldts }}
            , 'I' as cdc
    {%- endif %}
)
{%- endif %}
,
cte_current_sts_not_deleted as
(
  select  {{ datavault4dbt.prefix(source_cols, 'cte_current_sts') }}
  from cte_current_sts
  where cdc <> 'D'
)
, cte_max_rv_ldts AS
(
    SELECT COALESCE(max(ldts), {{ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) }} ) ldts 
    FROM cte_current_sts_not_deleted
)
, cte_stage AS
(
    select {{ datavault4dbt.prefix(source_cols, 'src') }}
    from {{ source_relation }} src
    CROSS JOIN cte_max_rv_ldts
    where not {{ datavault4dbt.prefix([src_ldts], 'src') }} in ({{ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) }}, {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }})
    AND {{ datavault4dbt.prefix([src_ldts], 'src') }} > cte_max_rv_ldts.ldts
)
, cte_rv_stage_union as
(
    select {{ datavault4dbt.prefix(source_cols, 'cte_current_sts_not_deleted') }}
    from cte_current_sts_not_deleted
    UNION
    (
        select {{ datavault4dbt.prefix(source_cols, 'cte_stage') }}
        from cte_stage
    )
)
, cte_dat_dom as
(
    select distinct {{ datavault4dbt.prefix([src_ldts], 'src') }}
    {%- if  src_edts is not none %}
            ,  {{ datavault4dbt.prefix([src_edts], 'src') }}
            ,  {{ datavault4dbt.prefix([edts_hashkey], 'src') }}
    {% endif %}
    from {{ source_relation }} src
    CROSS JOIN cte_max_rv_ldts
    where not {{ datavault4dbt.prefix([src_ldts], 'src') }} in ({{ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) }}, {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }})
    AND {{ datavault4dbt.prefix([src_ldts], 'src') }} > {{ datavault4dbt.prefix([src_ldts], 'cte_max_rv_ldts') }} 
)
, cte_key_dom as
(
    select {{ datavault4dbt.prefix([tracked_hashkey], 'cte_rv_stage_union') }}
    from cte_rv_stage_union
)
, cte_key_dat_dom as
(
    select distinct  {{ datavault4dbt.prefix([tracked_hashkey], 'cte_key_dom') }}
                    , {{ datavault4dbt.prefix([src_ldts], 'cte_dat_dom') }}
                    {% if  src_edts is not none %}
                    ,  {{ datavault4dbt.prefix([src_edts], 'cte_dat_dom') }}
                    ,  {{ datavault4dbt.prefix([edts_hashkey], 'cte_dat_dom') }}
                    {% endif -%}
    from cte_key_dom 
    cross join cte_dat_dom
), cte_data_join as
(
    select
          {{ datavault4dbt.prefix([tracked_hashkey], 'cte_key_dat_dom') }} dom_key
        , {{ datavault4dbt.prefix([src_ldts], 'cte_key_dat_dom') }} as dom_ldts
        , {{ datavault4dbt.prefix([src_ldts], 'cte_rv_stage_union') }} as stage_ldts
{%- if  src_edts is not none %}
        , {{ datavault4dbt.prefix([src_edts], 'cte_key_dat_dom') }} as stage_edts
        , {{ datavault4dbt.prefix([edts_hashkey], 'cte_key_dat_dom') }} as stage_edts_hashkey
{% endif -%}
        , lag({{ datavault4dbt.prefix([src_ldts], 'cte_key_dat_dom') }}) over (partition by  {{ datavault4dbt.prefix([tracked_hashkey], 'cte_key_dat_dom') }} order by  {{ datavault4dbt.prefix([src_ldts], 'cte_key_dat_dom') }}) as prev_dom_ldts
        , lag({{ datavault4dbt.prefix([src_ldts], 'cte_rv_stage_union') }}) over (partition by  {{ datavault4dbt.prefix([tracked_hashkey], 'cte_key_dat_dom') }} order by  {{ datavault4dbt.prefix([src_ldts], 'cte_key_dat_dom') }}) as prev_stage_ldts
        , {{ datavault4dbt.prefix([src_rsrc], 'cte_rv_stage_union') }}
    from cte_key_dat_dom 
    left join cte_rv_stage_union 
         on {{ datavault4dbt.prefix([src_ldts], 'cte_key_dat_dom') }} = {{ datavault4dbt.prefix([src_ldts], 'cte_rv_stage_union') }}
         and {{ datavault4dbt.prefix([tracked_hashkey], 'cte_key_dat_dom') }} = {{ datavault4dbt.prefix([tracked_hashkey], 'cte_rv_stage_union') }}
    where 1=1
)
, cte_data_interpretation as
(
    select
      dom_key
    , dom_ldts
    , stage_ldts
{%- if  src_edts is not none -%}
    , stage_edts
    , stage_edts_hashkey
{%- endif -%}
    , prev_dom_ldts
    , prev_stage_ldts
    , CASE WHEN stage_ldts IS NULL AND prev_dom_ldts IS NULL
        THEN 'discard'
    WHEN COALESCE (stage_ldts, {{ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) }})=dom_ldts AND prev_dom_ldts IS NULL
    THEN 'I'
    WHEN COALESCE (stage_ldts, {{ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) }})=dom_ldts AND prev_stage_ldts IS NULL 
        THEN 'I'
    {%- if load_type == 'full' %}
        WHEN stage_ldts IS NULL AND COALESCE(prev_stage_ldts, {{ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) }})= COALESCE(prev_dom_ldts, {{ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) }})
        THEN 'D'
    {% endif %}
        ELSE 'discard'
        END AS cdc
    , {{ datavault4dbt.prefix([src_rsrc], 'cte_data_join') }}
    from cte_data_join
)
SELECT
      dom_key AS {{ tracked_hashkey }}
    , dom_ldts AS {{ src_ldts }}
{%- if  src_edts is not none -%}
    , stage_edts as {{ src_edts }}
    , stage_edts_hashkey as {{ edts_hashkey }}
{%- endif -%}
    , {{ datavault4dbt.prefix([src_rsrc], 'cte_data_interpretation') }}
    , cdc
FROM cte_data_interpretation
WHERE cdc<>'discard'
{%- if not is_incremental() %}
UNION ALL
SELECT 
{{ tracked_hashkey }}
    ,  {{ src_ldts }}
{%- if  src_edts is not none -%}
    , {{ src_edts }}
    , {{ edts_hashkey }}
{%- endif -%}    
    , {{ src_rsrc }}
    , cdc
FROM cte_current_sts
{%- endif -%}
{% endmacro %}