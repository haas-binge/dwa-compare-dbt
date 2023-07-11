{# template e_sat_v1 Version:0.2.0 #}
{# automatically generated based on dataspot#}

{{ config(materialized='view') }} 

-- depends_on: {{ ref('position_product_l') }}

{%- set yaml_metadata -%}
sts_sats: 'position_product_ws_sts'
link_hashkey:  'hk_position_product_l'
link_name: 'position_product_l'
driving_key: 'hk_position_h'
secondary_fks: 'hk_product_h'


add_is_current_flag: true
{%- endset -%}    

{%- set metadata_dict = fromyaml(yaml_metadata) -%}
{%- set sts_sats = metadata_dict['sts_sats'] -%}
{%- set link_hashkey = metadata_dict['link_hashkey'] -%}
{%- set link_name = metadata_dict['link_name'] -%}
{%- set driving_key = metadata_dict['driving_key'] -%}
{%- set secondary_fks = metadata_dict['secondary_fks'] -%}
{%- set ledts_alias = metadata_dict['ledts_alias'] -%}
{%- set src_edts = metadata_dict['src_edts'] -%}
{%- set add_is_current_flag = metadata_dict['add_is_current_flag'] -%}

{{ e_sat(sts_sats=sts_sats,
                        link_hashkey=link_hashkey,
                        link_name=link_name,                        
                        driving_key=driving_key,
                        secondary_fks=secondary_fks,                        
                        ledts_alias=ledts_alias,
                        src_edts=src_edts,
                        add_is_current_flag=add_is_current_flag) }}