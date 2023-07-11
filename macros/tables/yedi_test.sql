{%- macro yedi_test(source_model_source, source_model_target, load_type, src_ldts = 'ldts', meta_load_name= 'meta_load' ) -%}
    
    
    {{- adapter.dispatch('yedi_test', 'datavault4dbt')(source_model_source=source_model_source,
                                        source_model_target=source_model_target,
                                        load_type=load_type,
                                        src_ldts = src_ldts,
                                        meta_load_name=meta_load_name
) -}}

{%- endmacro -%}


{%- macro snowflake__yedi_test(
                source_model_source,
                source_model_target,
                load_type,
                src_ldts,
                meta_load_name
                ) -%}


{%- set load_attrib_list = get_attrib_list('load', source_model_target,'attrib', false) -%} 
{#{ log("load_attrib_list: " + load_attrib_list| string, True) }#}

{%- set load_attrib_list_without_coalesce = get_attrib_list('load', source_model_target,'attrib', false) -%} 
{#{ log("load_attrib_list_without_coalesce: " + load_attrib_list_without_coalesce| string, True) }#}
{%- set object_list = get_object_list(source_model_target) -%} 
{%- set ldts = src_ldts -%} 
{%- set unknown_key = get_dict_hash_value("unknown_key") -%}    
WITH
cte_load_date as
(
  SELECT file_ldts as {{ ldts }}
  FROM {{ ref(meta_load_name) }}
  WHERE table_name = '{{source_model_source}}'
  qualify max({{ ldts }}) OVER (PARTITION BY TABLE_NAME) = {{ ldts }}
),
cte_load AS
(
    SELECT
        {{ format_list(load_attrib_list, 1) }}
        , {{ ldts }}
    FROM {{ ref(source_model_source) }}
    where is_check_ok
)
{%- set sat_list = [] -%} 
{%- set bo_attrib_list = [] -%} 
{%- set attrib_list = [] -%} 
{%- set total_attrib_dict = {} -%} 
{%- set hk_object = "" -%}

{%- for object in object_list -%}
    {%- set bo_total_attrib_dict = {} -%} 
    {%- set businessobject_name = '' -%}
    {%- set attribut_output_string = '' -%}
    {%- set hashkey_output_string = '' -%}
    {%- if object.endswith('_h') -%}
        {%- set type = 'hub' -%}
        {%- set businessobject_name = object.replace('_h','') -%}
        {%- set hk_object = 'hk_' + object -%}
        {%- set bo_attrib_list = get_attrib_list(object, source_model_target) -%}     
        {%- if  bo_attrib_list | length == 1 %}{#%#### if we have a combined business-key two or more BK->HK we take them from a satellite  #####%#}
            {%- set attribut_output_string = "IFF(" + businessobject_name + "_bk != '(unknown)', " + businessobject_name + "_bk, NULL) as " + format_list(bo_attrib_list, 1) -%}
        {%- endif -%}
        {%- set hashkey_output_string = object + "." + hk_object  -%} 
    {%- elif object.endswith('_l') -%}
        {%- set type = 'link' -%}
        {%- set businessobject_name = object.replace('_l','') -%}
        {%- set hk_object = 'hk_' + object -%}
        {%- set bo_attrib_list = get_attrib_list(object, source_model_target) -%}
        {%- if bo_attrib_list != [] %}     
            {%- set attribut_output_string = format_list(bo_attrib_list, 1)-%}
        {%- endif %}     
        {%- set hashkey_output_string = object + "." + hk_object  -%} 

    {%- elif object.endswith('_nhl') -%}
        {%- set type = 'hlink' -%}
        {%- set businessobject_name = object.replace('_nhl','') -%}
        {%- set bo_attrib_list = get_attrib_list(object, source_model_target) -%}
        {%- if bo_attrib_list != [] %}     
            {%- set attribut_output_string = format_list(bo_attrib_list, 1)-%}
        {%- endif -%}     
    {%- endif -%}

    {%- if  not (bo_attrib_list | length > 1 and type == 'hub') -%}
        {%- for bo_attrib in bo_attrib_list -%}
            {% set bo_total_attrib = {bo_attrib:('cte_' + object + "." + bo_attrib)} -%}  
            {% set total_attrib = {bo_attrib:('cte_' + businessobject_name + "." + bo_attrib)} -%}  
            {% set x = bo_total_attrib_dict.update(bo_total_attrib) -%}   
            {% set x = total_attrib_dict.update(total_attrib) -%}   
        {%- endfor %}
        {#{log("total_attrib_dict: " + total_attrib_dict|string, True)}#}
    {%- endif -%}
    {%- set sat_list = get_satellite_list(object, source_model_target) -%}
    {%- set has_satellites = (sat_list != []) %}
, cte_{{ businessobject_name }} as
( 
    {%- if has_satellites %}
    with cte_{{object}} as
    (
    {%- endif %}
        SELECT  
        {%- if hashkey_output_string != "" %}
            {{ hashkey_output_string }}
        {%- endif %}
        {%- if attribut_output_string != "" %}
            {%- if hashkey_output_string != "" %},{%- endif %} {{ attribut_output_string }}
            {%- endif %}
        {%- if  type == 'hlink' %}
        , cte_load_date.{{ldts}}
        FROM cte_load_date
        CROSS JOIN {{ ref(object) }} {{object}}
        WHERE  {{object}}.{{ldts}} <= cte_load_date.{{ldts}}
        {%- else %}
        FROM {{ ref(object) }} {{object}}
        {%- endif %}
    {%- if has_satellites %}
    )
        {%- for sat in sat_list -%}
            {%- set attrib_list = get_attrib_list(sat, source_model_target) -%}  
            
            {%- for attrib in attrib_list -%}
                {%- if not attrib in bo_total_attrib_dict -%}  
                    {% set bo_total_attrib = {attrib:("cte_" + sat + "." + attrib)} -%}  
                    {% set y = bo_total_attrib_dict.update(bo_total_attrib) -%}   
                {%- endif %}      
                {%- if not attrib in total_attrib_dict -%}  
                    {% set total_attrib = {attrib:('cte_' + businessobject_name + "." + attrib)} -%}  
                    {% set y = total_attrib_dict.update(total_attrib) -%}   
                {%- endif -%}      
            {%- endfor %}
    ,cte_{{ sat }} as
    (
        {%- if sat.endswith('_sts') or sat.endswith('_rts') -%}
        SELECT * FROM 
        (
        {%- endif  -%}
        {%- if sat.endswith('_ms') -%}
        WITH cte_{{ sat }}_date as
        (
            SELECT                       
                  {{ hk_object }}
                , {{ldts}}
                , COALESCE(LEAD({{ldts}} - INTERVAL '1 MICROSECOND') OVER (PARTITION BY {{ hk_object }}  ORDER BY {{ldts}}),TO_TIMESTAMP('8888-12-31T23:59:59', 'YYYY-MM-DDTHH24:MI:SS')) as ledts
            FROM 
            (
                SELECT distinct 
                      {{sat}}.{{ hk_object }}
                    , {{sat}}.{{ldts}}
                FROM {{ ref(sat) }} {{sat}}
            )t
        )
        {%- endif  %}
        SELECT    
              cte_{{object}}.{{ hk_object }}  
            {%- if attrib_list != [] %}
            , {{ format_list(attrib_list, 1) }}
            {%- endif -%}
            , {{sat}}.{{ldts}}
             {%- if sat.endswith('sts')   -%}
            , {{sat}}.cdc
            {%- endif -%}
            {%- if sat.endswith('_ms') -%}
            , cte_{{ sat }}_date.ledts
            {%- else -%}
            , COALESCE(LEAD({{sat}}.{{ldts}} - INTERVAL '1 MICROSECOND') OVER (PARTITION BY cte_{{object}}.{{ hk_object }}  ORDER BY {{sat}}.{{ldts}}),TO_TIMESTAMP('8888-12-31T23:59:59', 'YYYY-MM-DDTHH24:MI:SS')) as ledts
            {%- endif %}
        FROM cte_{{object}}
        INNER JOIN {{ ref(sat) }} {{sat}}
            ON cte_{{object}}.{{ hk_object }} = {{sat}}.{{ hk_object }}  
            {%- if sat.endswith('_ms') %}
        INNER JOIN cte_{{ sat }}_date            
            ON  {{sat}}.{{ hk_object }} = cte_{{ sat }}_date.{{ hk_object }}  
            AND {{sat}}.{{ ldts }} = cte_{{ sat }}_date.{{ ldts }}  
            {%- endif -%}
            {%- if sat.endswith('sts') or sat.endswith('rts') %}
        )
                {%- if sat.endswith('sts')  %}
        WHERE cdc <> 'D'
                {%- endif  %}
            {%- else %}
        WHERE {{sat}}.{{ hk_object }} <> '{{unknown_key}}'
            {%- endif %}
    )
        {%- endfor %} 
    SELECT  
        cte_{{object}}.{{ hk_object }}
        {%- if bo_total_attrib_dict != [] %}
            {%- for attribute in bo_total_attrib_dict %}
        , {{bo_total_attrib_dict[attribute]}}
            {%- endfor %}
        {%- endif %}
        , d.{{ldts}}
    FROM cte_load_date d
    CROSS JOIN cte_{{object}}
    {%- for sat in sat_list %}
    INNER JOIN  cte_{{ sat }}
        ON cte_{{ sat }}.{{ hk_object }} = cte_{{object}}.{{ hk_object }}   
        {%- if load_type | lower == "full" %}
        AND d.{{ldts}} between cte_{{ sat }}.{{ldts}} AND cte_{{ sat }}.ledts
        {%- else %}
        AND d.{{ldts}} = cte_{{ sat }}.{{ldts}}
        {%- endif %}
    {%- endfor %} 
    {%- endif %}
)
{%- endfor -%} {# hub #}
,
{%- set target_out = [] %}
cte_target as
(   
    SELECT 
    {%- for attrib in total_attrib_dict %}
        {%- if not total_attrib_dict[attrib].split(".")[1].startswith('hk_')  -%}
            {%- set x=target_out.append(total_attrib_dict[attrib]) -%} 
        {%- endif  %}
    {%- endfor  %}
    {{ format_list(target_out, 1) }}
    {%- set already_joined = [] -%} 
    {%- set link_list = get_link_list(source_model_target) -%} 
    {%- if link_list != []  -%} 
         , cte_load_date.{{ ldts }}
    FROM cte_load_date
        {%- for link in link_list -%}
            {%- set link_businessobject_name = link.replace('_l','').replace('_nhl','') -%}
            {%- set hub_key_list = get_attrib_list(link, source_model_target) %}    
            {%- set sat_list = get_satellite_list(link, source_model_target) %}
            {%- set link_has_satellites = (sat_list != []) %} 
    INNER JOIN cte_{{link_businessobject_name}} 
            {%- set next_join = 'ON' %}
            {%- if link_has_satellites or link.endswith('_nhl') %}
                {%- set next_join = 'AND' %}
        ON cte_{{link_businessobject_name}}.{{ldts}} = cte_load_date.{{ldts}}
            {%- endif -%}
            {%- for hub_key in hub_key_list -%}
                {%- if hub_key.startswith("hk_") -%}
                    {%- set hub_object_name = hub_key.replace("hk_", '').replace('_h','') -%}
                    {%- set sat_list = get_satellite_list(hub_object_name, source_model_target) %}
                    
                    {%- set hub_has_satellites = (sat_list != []) %} 
                    {%- if hub_object_name in already_joined %}
        {{ next_join }} cte_{{link_businessobject_name}}.{{hub_key}} = cte_{{hub_object_name}}.{{hub_key}}
                        {%- if hub_has_satellites %}
        AND cte_{{hub_object_name}}.{{ldts}} = cte_load_date.{{ldts}} 
                        {%- endif -%}
                    {%- endif -%}
                    {#%- set s = already_joined.append(hub_object_name) %#}
                {%- endif -%}
            {%- endfor -%}
            {%- for hub_key in hub_key_list -%}
                {%- if hub_key.startswith("hk_") -%}
                    {%- set hub_object_name = hub_key.replace("hk_", '').replace('_h','') -%}
                    {%- set sat_list = get_satellite_list(hub_object_name + '_h', source_model_target) %}
                    {%- set hub_has_satellites = (sat_list != []) %} 
                    {%- if not hub_object_name in already_joined %}
                        {%- set s = already_joined.append(hub_object_name) %}
    INNER JOIN  cte_{{hub_object_name}} 
        ON cte_{{link_businessobject_name}}.{{hub_key}} = cte_{{hub_object_name}}.{{hub_key}}
                        {%- if hub_has_satellites %}
        AND cte_{{hub_object_name}}.{{ldts}} =  cte_load_date.{{ldts}} 
                        {%- endif -%}
                    {%- endif -%}
                {%- endif -%}
            {%- endfor -%}
        {%- endfor %}
    {%- else  -%} 
        {%- set hub_list = get_hub_list(source_model_target) -%}
        {%- for hub in hub_list -%}
            {%- set hub_object_name = hub.replace('_h','') -%}
            {%- set sat_list = get_satellite_list(hub_object_name + '_h', source_model_target) %}
            {%- set hub_has_satellites = (sat_list != []) %} 
         , cte_load_date.{{ ldts }}
            {%- if hub_has_satellites %}
    FROM cte_load_date
    INNER JOIN  cte_{{hub_object_name}} 
        ON cte_load_date.{{ldts}} = cte_{{hub_object_name}}.{{ldts}}
            {% else %}
    FROM cte_{{hub_object_name}} 
                {%- if load_type == 'full' %}
    CROSS JOIN cte_load_date
                {%- endif -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif %}
)
(
    select
            {{ format_list(load_attrib_list_without_coalesce, 1) }}
            , {{ ldts }}
    from cte_load
    MINUS
    select
            {{ format_list(load_attrib_list_without_coalesce, 1) }}
            , {{ ldts }}
    from cte_target
)    
UNION
(
    select
            {{ format_list(load_attrib_list_without_coalesce, 1) }}
            , {{ ldts }}
    from cte_target
    minus
    select
            {{ format_list(load_attrib_list_without_coalesce, 1) }}
            , {{ ldts }}
    from cte_load
)
{% endmacro %}
