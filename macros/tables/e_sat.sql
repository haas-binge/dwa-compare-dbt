{%- macro e_sat(sts_sats, link_hashkey, link_name, driving_key, secondary_fks, ledts_alias='ledts', src_edts=none, add_is_current_flag=false) -%}

    {# Applying the default aliases as stored inside the global variables, if src_ldts and src_rsrc are not set. #}

    {%- set src_ldts = datavault4dbt.replace_standard(src_ldts, 'datavault4dbt.ldts_alias', 'ldts') -%}
    {%- set src_rsrc = datavault4dbt.replace_standard(src_rsrc, 'datavault4dbt.rsrc_alias', 'rsrc') -%}

    {{ adapter.dispatch('e_sat')(sts_sats=sts_sats,
                                        link_hashkey=link_hashkey,
                                        link_name=link_name,                        
                                        driving_key=driving_key,
                                        secondary_fks=secondary_fks,                        
                                        ledts_alias=ledts_alias,
                                        src_edts=src_edts,										
                                        add_is_current_flag=add_is_current_flag) }}

{%- endmacro -%}

{%- macro snowflake__e_sat(sts_sats, link_hashkey, link_name, driving_key, secondary_fks, ledts_alias='ledts', src_edts=none, add_is_current_flag=false) -%}


{{- datavault4dbt.check_required_parameters(sts_sats=sts_sats,
                                                    link_hashkey=link_hashkey,
                                                    link_name=link_name,                        
                                                    driving_key=driving_key,
                                                    secondary_fks=secondary_fks,                        
                                                    ledts_alias=ledts_alias,
                                                    add_is_current_flag=add_is_current_flag) -}}


{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}
{%- set is_current_col_alias = var('datavault4dbt.is_current_col_alias', 'IS_CURRENT') -%}
{%- set link_relation = ref(link_name) -%}

{%- if  is_nothing(src_edts) -%}
    {%- set ledts_alias = none -%}
    {%- set src_edts = none -%}
{%- endif -%}

{%- if sts_sats is not mapping and not datavault4dbt.is_list(sts_sats) -%}
    {%- set sts_sats = {sts_sats: {}} -%}

{%- endif -%}

{{ config(materialized='view') }}


with union_sts as
(
{%- for sts_sat in sts_sats -%}	
	{%- set sts_sat=ref(sts_sat) %}	
	SELECT *
	FROM {{ sts_sat }}
	{%- if not loop.last %}
	UNION ALL
	{% endif -%}
{%- endfor %}
)
{%- if src_edts is not none -%}
, eedts_calculation as 
(
	with eedts_distinct as 
	(
		SELECT DISTINCT
			  union_sts.{{link_hashkey}}
			, {{ link_relation }}.{{driving_key}}
			, {{ link_relation }}.{{secondary_fks}}
			, union_sts.{{src_edts}}
		from union_sts
		inner join  {{ link_relation }} 
			on union_sts.{{link_hashkey}}={{link_name}}.{{link_hashkey}}
	)
	select 
	 	  {{link_hashkey}}
		, {{driving_key}}
		, {{secondary_fks}}
		, {{src_edts}}
		, coalesce(lead({{src_edts}} - interval '1 microsecond') 
					over (	partition by {{driving_key}}
				 	 		order by {{src_edts}}
						),{{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}
			) as eedts
	from eedts_distinct c
)
{%- endif -%}
, ledts_calculation as 
(
	select 
		  union_sts.{{link_hashkey}}
		, {{link_name}}.{{driving_key}}
		, {{link_name}}.{{secondary_fks}}
		, union_sts.ldts
	{%- if src_edts is not none %}		
		, union_sts.{{src_edts}}
		, coalesce(lead(union_sts.ldts- interval '1 microsecond') over (partition by union_sts.{{src_edts}}, {{link_name}}.{{driving_key}} order by union_sts.ldts),to_timestamp('8888-12-31t23:59:59', 'yyyy-mm-ddthh24:mi:ss')) as ledts
		, row_number() over (partition by union_sts.{{src_edts}}, {{link_name}}.{{driving_key}} order by union_sts.ldts desc) =1 as is_active
		, eedts
	{%- else -%}
		, coalesce(lead(union_sts.ldts- interval '1 microsecond') over (partition by {{link_name}}.{{driving_key}} order by union_sts.ldts),to_timestamp('8888-12-31t23:59:59', 'yyyy-mm-ddthh24:mi:ss')) as ledts
		, row_number() over (partition by {{link_name}}.{{driving_key}} order by union_sts.ldts desc) =1 is_active
	{%- endif -%}
    , union_sts.rsrc 
	, union_sts.cdc
	from union_sts
	inner join  {{link_relation }} 
		on union_sts.{{link_hashkey}}={{link_name}}.{{link_hashkey}}
	{%- if src_edts is not none %}		
	inner join  eedts_calculation e
		on union_sts.{{link_hashkey}} = e.{{link_hashkey}}
		and union_sts.{{src_edts}} = e.{{src_edts}}
	{%- endif -%}
)
select
*
{%- if add_is_current_flag %}
{%- if src_edts is not none -%}
, CASE WHEN {{ ledts_alias }} = {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}
	THEN TRUE
	ELSE FALSE
END AS {{ is_current_col_alias }}
{% else %}
, CASE WHEN ledts = {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}
	THEN TRUE
	ELSE FALSE
END AS {{ is_current_col_alias }}
{% endif %}
{% endif %}
from ledts_calculation
where cdc<>'D'

{% endmacro %}