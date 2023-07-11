{%- macro sns_table(pit, pit_hk, pit_satellites, base_entity, primary_sourcesystem='ws', src_ldts=none, src_rsrc=none) -%}

    {# Applying the default aliases as stored inside the global variables, if src_ldts and src_rsrc are not set. #}

    {%- set src_ldts = datavault4dbt.replace_standard(src_ldts, 'datavault4dbt.ldts_alias', 'ldts') -%}
    {%- set src_rsrc = datavault4dbt.replace_standard(src_rsrc, 'datavault4dbt.rsrc_alias', 'rsrc') -%}

    {{ adapter.dispatch('sns_table', 'datavault4dbt')(pit=pit,
                                                      pit_hk=pit_hk,
                                                      pit_satellites=pit_satellites,
                                                      base_entity=base_entity,
                                                      primary_sourcesystem=primary_sourcesystem,
                                                      src_ldts=src_ldts,
                                                      src_rsrc=src_rsrc) }}


{%- endmacro -%}

{%- macro snowflake__sns_table(pit, pit_hk, pit_satellites, base_entity, primary_sourcesystem, src_ldts, src_rsrc) -%}

{%- set hash = var('datavault4dbt.hash', 'MD5') -%}
{%- set hash_dtype = var('datavault4dbt.hash_datatype', 'STRING') -%}
{%- set hash_default_values = fromjson(datavault4dbt.hash_default_values(hash_function=hash,hash_datatype=hash_dtype)) -%}
{%- set unknown_key = datavault4dbt.as_constant(column_str=hash_default_values['unknown_key']) -%}


{{- datavault4dbt.check_required_parameters(pit=pit, pit_hk=pit_hk, pit_satellites=pit_satellites,
                                                                    base_entity=base_entity,
                                                                    primary_sourcesystem=primary_sourcesystem,
                                                                    src_ldts=src_ldts,
                                                                    src_rsrc=src_rsrc) -}}


{%- set pit_relation = ref(pit|string) -%}
{%- set ledts_alias = var('datavault4dbt.ledts_alias', 'ledts') -%}
{%- set sdts_alias = var('datavault4dbt.sdts_alias', 'sdts') -%}

{%- set pit_satellites_dict = {} -%}

{%- if datavault4dbt.is_list(pit_satellites) -%}
    {%- for pit_satellite in pit_satellites -%}
       {%- set sourcesystem = pit_satellite.split('_')[-2]  -%}
       {%- set suffix = '_' +sourcesystem if sourcesystem != primary_sourcesystem else '' -%}       
       {%- do pit_satellites_dict.update({pit_satellite: {'sourcesystem': sourcesystem, 'suffix': suffix}}) -%}
    {%- endfor -%}
{%- else -%}
    {%- set sourcesystem = pit_satellites.split('_')[-2]  -%}
    {%- set suffix = sourcesystem if sourcesystem != 'ws' else '' -%}
    {%- set pit_satellites_dict = {pit_satellites: {'sourcesystem': sourcesystem, 'suffix': suffix}} -%}

{%- endif -%}


{# define if the sns is based on a hub or a link #}
{%- set sns_type = "" -%}
{%- if pit_hk.endswith('_h') -%}
  {%- set sns_type = "hub_based" %}
{%- elif pit_hk.endswith('_l') -%} 
  {%- set sns_type = "link_based" %}
  {%- set ref_link_relation = ref(base_entity|string) -%}  
  {%- set link_columns = datavault4dbt.source_columns(ref_link_relation) -%}
  {%- set link_columns_hubs = [] -%}
  {%- for column in link_columns -%}
      {%- if column.lower().endswith('_h') -%}
        {%- set _ = link_columns_hubs.append(column.lower()) -%}
      {%- endif -%}
  {%- endfor -%}  
{%- endif -%}  


select
  {{pit}}.{{sdts_alias}}, 
  {{pit}}.{{pit_hk}}, 
  {{pit}}.{{pit_hk[:-2] + '_d' if pit_hk.endswith(('_l', '_h')) else pit_hk }}, 
  {# define a bk column only if a hub is referenced #}
  {%- if sns_type == "hub_based" -%}
  {{base_entity}}.{{pit_hk[3:-2] +'_bk' }}
  {%- endif -%}
  {# define the hub_hk referenced in the link if the base_entity is a link #}  
  {%- if sns_type == "link_based" -%}
  {%- for hub_hk in link_columns_hubs -%}
     {{base_entity}}.{{hub_hk}}{%- if not loop.last -%},{% endif %}{{ '\n' }}
  {%- endfor -%}
  {%- endif %}  
{%- for satellite in pit_satellites_dict.keys()  if not (satellite.endswith('_sts') or satellite.endswith('_es')) -%}
  {% if loop.first %},{% endif %}
  {%- set suffix = pit_satellites_dict[satellite]['suffix'] -%}
    {{ dbt_utils.star(from=ref(satellite|string), relation_alias=satellite|string, suffix=suffix)|replace('"', '')|lower() }},  
  lower({{satellite}}.rsrc)<>'system' has_{{pit_satellites_dict[satellite]['sourcesystem']}}_data{% if not loop.last %},{% endif %}{{ '\n' }} 
{%- endfor %} 
{%- for satellite in pit_satellites_dict.keys()  if  (satellite.endswith('_sts') or satellite.endswith('_es')) -%}
{% if loop.first %},{% endif %}
{{satellite}}.{{src_ldts}} as {{src_ldts}}_{{satellite}},{{ '\n' }}
{{satellite}}.{{src_rsrc}} as {{src_rsrc}}_{{satellite}}{% if not loop.last %},{% endif %}{{ '\n' }}
{%- endfor %} 
from {{ pit_relation}}  
inner join {{ ref(base_entity|string) }}  
on {{base_entity}}.{{pit_hk}} = {{pit}}.{{pit_hk}}
{%- for satellite in pit_satellites_dict.keys() %}
inner join {{ ref(satellite|string) }} 
on {{pit}}.hk_{{satellite}}={{satellite}}.{{pit_hk}}
and {{pit}}.ldts_{{satellite}}={{satellite}}.ldts 
{%- endfor %} 
where  
({{ '\n' }}
  {%- for satellite in pit_satellites_dict.keys() -%}
  {{pit}}.HK_{{satellite}} <>{{unknown_key}} {{ '\n' }}
  {%- if not loop.last %}OR {% endif -%} 
  {%- endfor %}
)
{% for satellite in pit_satellites_dict.keys() if satellite.endswith('_sts') or satellite.endswith('_es') -%}
  {%- if loop.first -%}
    AND
    (
  {%- endif -%}
      {%- if not loop.first %} OR {% endif -%} 
      ({{ satellite }}.cdc <>'D' and {{ satellite }}.{{pit_hk}} <>{{unknown_key}}){{ '\n' }}
  {%- if loop.last -%}
    )
  {% endif -%}
{%- endfor %}
{%- endmacro -%}