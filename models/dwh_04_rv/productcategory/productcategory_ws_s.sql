{# template sat_v0 Version: 0.1.0 #}
{# automatically generated based on dataspot#}

{{ config(materialized='incremental',
           unique_key=['hk_productcategory_h', 'ldts']) }} 

{%- set yaml_metadata -%}
source_model: "stg_webshop_produktkategorie" 
parent_hashkey: 'hk_productcategory_h'
src_hashdiff: 'hd_productcategory_ws_s'
src_payload: 
  - name


{%- endset -%}

{%- set metadata_dict = fromyaml(yaml_metadata) -%}

{%- set parent_hashkey = metadata_dict['parent_hashkey'] -%}
{%- set src_hashdiff = metadata_dict['src_hashdiff'] -%}
{%- set source_model = metadata_dict['source_model'] -%}
{%- set src_payload = metadata_dict['src_payload'] -%}


{{ datavault4dbt.sat_v0(parent_hashkey=parent_hashkey,
                        src_hashdiff=src_hashdiff,
                        source_model=source_model,
                        src_payload=src_payload) }}