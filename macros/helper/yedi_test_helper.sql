{% macro get_attrib_list(model_name, source_model_target, object_type='attrib', set_key_to_unknown=true) -%}
    {%- set out_attrib_list = [] -%}
    {%- for model in source_model_target -%}
    {#{ log("model: " + model| string, True) }#}
        {%- for attribute in source_model_target[model] -%}
            {%- if attribute == 'business_object' and (model_name == 'load' or model_name == model) -%}
                {%- for item in source_model_target[model][attribute] -%}
                    {% set outer_loop = loop %}
                    {%- for subitem in item -%}
                        {% set subitem_i = source_model_target[model][attribute][outer_loop.index0][subitem] %}
                        {%- if (model_name == 'load' and not subitem_i.startswith('hk_' )and not subitem_i.endswith('_bk')) or model_name != 'load' -%}
                            {%- if object_type=='attrib' -%}
                                {%- if set_key_to_unknown and model_name == 'load' -%}
                                    {%- set out_attrib = "COALESCE(" + subitem_i + ",'" + var("datavault4dbt.unknown_value__STRING") +"') as " + subitem_i | string -%}
                                    {%- else -%}
                                    {%- set out_attrib = subitem_i | string -%}
                                {%- endif -%}                                
                            {%- else -%}
                                {%- set out_attrib = subitem | string -%}
                            {%- endif -%}                                
                            {%- if not out_attrib in out_attrib_list -%} 
                                {{ out_attrib_list.append(out_attrib) }}
                            {%- endif -%}
                        {%- endif -%}                        
                    {%- endfor -%}
                {%- endfor -%}
            {%- endif -%}
            {%- if attribute == 'satellites' and object_type == 'attrib' -%}
                {%- set attribute_items = source_model_target.get(model).get(attribute) -%} 
                {%- if is_something(attribute_items)  -%}                 
                    {%- for item in attribute_items -%}

                        {%- set attribute_subitems = attribute_items[item] -%} 
                        {%- if is_something(attribute_subitems) and (model_name == 'load' or model_name == item) -%}
                            {%- for subitem in attribute_subitems["columns"] -%}
                                {%- if not subitem in out_attrib_list -%} 
                                    {{ out_attrib_list.append(subitem) }}
                                {%- endif -%}
                            {%- endfor -%}
                        {%- endif -%}
                    {%- endfor -%}
                {%- endif -%}
            {%- endif -%}

            {%- if attribute == 'columns' and object_type == 'attrib' and model_name == model-%}
                {%- for subitem in source_model_target.get(model)["columns"] -%}
                    {%- if not subitem in out_attrib_list -%} 
                        {{ out_attrib_list.append(subitem) }}
                    {%- endif -%}
                {%- endfor -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endfor -%}    
    {{ return(out_attrib_list) }}    
{%- endmacro %}

{% macro get_object_list(source_model_target) -%}
    {%- set out_list = [] -%}
    {%- for model in source_model_target -%}
        {%- if model.endswith('_h') or model.endswith('_l') or model.endswith('_nhl') -%}
            {{ out_list.append(model|string) }}
        {%- endif -%}
    {%- endfor -%}        
    {{ return(out_list) }}    
{%- endmacro %}

{% macro get_hub_list(source_model_target) -%}
    {%- set out_list = [] -%}
    {%- for model in source_model_target -%}
        {%- if model.endswith('_h') -%}
            {{ out_list.append(model|string) }}
        {%- endif -%}
    {%- endfor -%}        
    {{ return(out_list) }}    
{%- endmacro %}

{% macro get_link_list(source_model_target) -%}
    {%- set out_list = [] -%}
    {%- for model in source_model_target -%}
        {%- if model.endswith('_l') or model.endswith('_nhl')-%}
            {{ out_list.append(model|string) }}
        {%- endif -%}
    {%- endfor -%}        
    {{ return(out_list) }}    
{%- endmacro %}

{% macro get_satellite_list(parent_object, source_model_target) -%}
    {%- set out_list = [] -%}
    {%- for model in source_model_target -%}
        {%- if model == parent_object -%}
            {%- set model_attribute = source_model_target.get(model) -%}
            {%- if is_something(model_attribute) -%}
                {%- for attribute in model_attribute -%}
                        {%- if attribute == 'satellites' and is_something(model_attribute[attribute]) -%}
                            {%- for item in model_attribute[attribute] -%}
                                {{ out_list.append(item) }}
                            {%- endfor -%}
                        {%- endif -%}
                {%- endfor -%}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}        
    {{ return(out_list) }}    

{%- endmacro %}


{% macro get_ma_attrib_list(model_name, source_model_target) -%}
    {%- set out_attrib_list = [] -%}
    {%- for model in source_model_target | default('') -%}
        {%- set ma_columns = source_model_target.get(model) -%}
        {%- if is_something(ma_columns) -%}
            {%- set satellites = source_model_target.get(model).get('satellites') -%}
            {%- if is_something(satellites) -%}
                {%- set satellite = source_model_target.get(model).get('satellites').get(model_name) -%}
                {%- if is_something(satellite) -%}
                    {%- set ma_columns = source_model_target.get(model).get('satellites').get(model_name).get('ma_columns') -%}
                    {%- if is_something(ma_columns) -%}
                        {%- for attribute in ma_columns -%}
                            {%- if not attribute in out_attrib_list -%} 
                                {{ out_attrib_list.append(attribute) }}
                            {%- endif -%}
                        {%- endfor -%}
                    {%- endif -%}
                {%- endif -%}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}
   {{ return(out_attrib_list) }}    
{%- endmacro %}
