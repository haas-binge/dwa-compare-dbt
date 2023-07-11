{%- macro load(
                      source_model
                    , default_columns
                    , additional_columns
                    , key_check
					, dub_check
                    , hwm
                    , sourcetype
                    , columns
                    ) -%}

    {{ adapter.dispatch('load')(
                      source_model=source_model
                    , default_columns=default_columns
                    , additional_columns=additional_columns
                    , key_check=key_check
					, dub_check=dub_check
                    , hwm=hwm
                    , sourcetype=sourcetype
                    , columns=columns
                    ) }}

{%- endmacro -%}

{%- macro snowflake__load(source_model
                    , default_columns
                    , additional_columns
                    , key_check
					, dub_check
                    , hwm
                    , sourcetype
                    , columns
                    ) -%}


{{- datavault4dbt.check_required_parameters(source_model=source_model
                    , default_columns=default_columns
                    , additional_columns=additional_columns
                    , key_check=key_check
                    , dub_check=dub_check
                    , hwm=hwm
                    , sourcetype=sourcetype
                    , columns=columns
                    ) -}}


{%- set default_raw_col_definition = get_attribute_definition_list(default_columns, "default", "raw") -%} 
{%- set additional_raw_col_definition = get_attribute_definition_list(additional_columns, "additional", "raw") -%} 
{%- set payload_raw_col_definition = get_attribute_definition_list(columns, "payload", "raw") -%} 

{%- set default_typed_col_definition = get_attribute_definition_list(default_columns, "default", "typed") -%} 
{%- set additional_typed_col_definition = get_attribute_definition_list(additional_columns, "additional", "typed") -%} 
{%- set payload_typed_col_definition = get_attribute_definition_list(columns, "payload", "typed") -%} 

{%- set default_typecheck_col_definition = get_attribute_definition_list(default_columns, "default", "type_check") -%} 
{%- set additional_typecheck_col_definition = get_attribute_definition_list(additional_columns, "additional", "type_check") -%} 
{%- set payload_typecheck_col_definition = get_attribute_definition_list(columns, "payload", "type_check") -%} 

{%- set all_checks_list = [] -%}
{%- set all_msg_list = [] -%}

{%- set all_checks_list = add_attribute_from_dict_to_checks_list(default_columns, all_checks_list, "all_chk") -%}
{%- set all_checks_list = add_attribute_from_dict_to_checks_list(additional_columns, all_checks_list, "all_chk") -%}
{%- set all_checks_list = add_attribute_from_dict_to_checks_list(columns, all_checks_list, "all_chk") -%}

{%- set all_msg_list = add_attribute_from_dict_to_checks_list(default_columns, all_msg_list, "all_msg") -%}
{%- set all_msg_list = add_attribute_from_dict_to_checks_list(additional_columns, all_msg_list, "all_msg") -%}
{%- set all_msg_list = add_attribute_from_dict_to_checks_list(columns, all_msg_list, "all_msg") -%}


{%- if dub_check != "" %} 
{%- set dub_check_name = "is_dub_check_ok" -%}
{%- set dub_check_definition = get_dubcheck(dub_check, dub_check_name) -%} 
{%- set dub_check_list = ["dub_check"] -%} 
{%- set all_checks_list = add_attribute_from_list_to_checks_list(dub_check_list, all_checks_list) -%}
{%- set out_dubcheck_list = format_list(dub_check,0,"," ) -%}
{%- set out_dubcheck_dict = '{"dub_check": "' ~ out_dubcheck_list ~ '"}' -%}
{%- set out_dubcheck = "IFF(NOT " ~ dub_check_name ~ ", '" ~ out_dubcheck_dict ~ "','')" -%}
{%- set all_msg_list = add_item_to_list(all_msg_list, out_dubcheck) -%}
{% endif -%} 

{%- if key_check != "" %} 
{%- set key_check_definition = get_keycheck(key_check, 'key_check') -%} 
{%- set all_checks_list = add_attribute_from_list_to_checks_list(key_check, all_checks_list,"_key_check") -%}
{% for single_key_check in key_check %}
{%- set out_key_dict = '{"key_check": "' ~ single_key_check ~ '"}' -%}
{%- set out_key_check = "IFF(NOT " ~ "is_" ~ single_key_check ~ "_key_check_ok" ~ ", '" ~ out_key_dict ~ "','')" -%}
{%- set all_msg_list = add_item_to_list(all_msg_list, out_key_check) -%}
{% endfor -%} 
{% endif -%} 



with
{%- if hwm %} 
{{ pre_hwm(this, omit_with=true, add_comma_at_end=true) }}
{% endif -%} 
raw_data AS 
(
	SELECT 
		  {{ format_list(default_raw_col_definition, 2) }}
		, {{ format_list(additional_raw_col_definition, 2) }}
		, {{ format_list(payload_raw_col_definition, 2) }}
    FROM {{ source(source_model['source_name'], source_model['source_table']) }}
)
SELECT 
		{%- if default_typed_col_definition != [] %} 
		  {{ format_list(default_typed_col_definition, 2) }}
		{%-  endif -%} 
		{%- if additional_typed_col_definition != [] %} 
		, {{ format_list(additional_typed_col_definition, 2) }}
		{%-  endif -%} 
		{%- if payload_typed_col_definition != [] %} 
		, {{ format_list(payload_typed_col_definition, 2) }}
		{%-  endif -%} 
		{%- if default_typecheck_col_definition != [] %} 
		, {{ format_list(default_typecheck_col_definition, 2) }}
		{%-  endif -%} 
		{%- if additional_typecheck_col_definition != [] %} 
		, {{ format_list(additional_typecheck_col_definition, 2) }}
		{%-  endif -%} 
		{%- if payload_typecheck_col_definition != [] %} 
		, {{ format_list(payload_typecheck_col_definition, 2) }}
		{%-  endif -%} 
		{%- if dub_check_definition != "" %} 
		, {{ dub_check_definition }}
		{%-  endif -%} 
		{%- if key_check_definition != [] %} 
		, {{ format_list(key_check_definition, spacenumber=2) }}
		{%  endif -%} 
		{%- if all_checks_list != [] %} 
		, {{ format_list(all_checks_list, spacenumber=0, separator= " AND ") }} is_check_ok
		{%  endif -%} 
		{%- if all_msg_list != [] %} 
		,  {{ "TO_VARIANT(ARRAY_EXCEPT([REPLACE(" ~ format_list(all_msg_list, spacenumber=0, separator= " || ") ~ ", '}{','},{')],['']))" }} chk_all_msg
		{%  endif -%} 

 FROM raw_data
{%- if hwm %} 
{{ post_hwm(this) }}
{% endif -%} 
{% endmacro %}  