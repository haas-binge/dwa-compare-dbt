{# template sns Version: 0.1.0 #}
{# automatically generated based on dataspot#}
{{ config(materialized='view') }}

{%- set yaml_metadata -%}
pit: 'productcategory_hierarchy_snp'
base_entity: 'productcategory_hierarchy_l'
pit_hk: 'hk_productcategory_hierarchy_l'
pit_satellites: 
  - 'productcategory_hierarchy_ws_sts'
primary_sourcesystem: 'ws'

{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ sns_table(pit=metadata_dict['pit'],
                    pit_hk=metadata_dict['pit_hk'],
                    pit_satellites=metadata_dict['pit_satellites'],
                    base_entity=metadata_dict['base_entity'],
                    primary_sourcesystem=metadata_dict['primary_sourcesystem']) }}


