{{ config(materialized="table", pre_hook=["{{ dbt_external_tables.stage_external_sources(select='DWS.EXT_WEBSHOP_POSITION') }}"], post_hook=["{{ insert_hwm(this) }}"]) }}

{%- set yaml_metadata -%}
source_model: 
  source_table: EXT_WEBSHOP_POSITION
  source_database: DWS
  source_name: LOAD_EXT
hwm: True
source_type: snowflake_external_table
dub_check:
- ldts
- bestellungid
- PosID

key_check:
- bestellungid
- PosID

columns:
    bestellungid:
      data_type: VARCHAR
      source_column_number: 1
    posid:
      data_type: VARCHAR
      source_column_number: 2
    produktid:
      data_type: VARCHAR
      source_column_number: 3
    spezlieferadrid:
      data_type: NUMBER
      source_column_number: 4
      type_check: True
    menge:
      data_type: NUMBER
      source_column_number: 5
      type_check: True
    preis:
      data_type: VARCHAR
      source_column_number: 6

default_columns:
    ldts:
      data_type: TIMESTAMP
      format: YYYYMMDD_HH24MISS
      type_check: True
      value: replace(right(filenamedate,19),'.csv','')
    rsrc:
      data_type: VARCHAR
      value: filenamedate

additional_columns:
    edts_in:
      data_type: DATE
      format: YYYYMMDD
      type_check: True
      value: trim(reverse(substring(reverse(replace(filenamedate,'.csv','')), 17,8))::varchar)
    raw_data:
      data_type: VARCHAR
      value: value
    row_number:
      data_type: NUMBER
      type_check: True
      value: metadata$file_row_number

{%- endset -%}

{%- set metadata_dict = fromyaml(yaml_metadata) -%}

{%- set source_model = metadata_dict['source_model'] -%}
{%- set default_columns = metadata_dict['default_columns'] -%}
{%- set additional_columns = metadata_dict['additional_columns'] -%}
{%- set key_check = metadata_dict['key_check'] -%}
{%- set dub_check = metadata_dict['dub_check'] -%}

{%- set hwm = metadata_dict['hwm'] -%}
{%- set sourcetype = metadata_dict['sourcetype'] -%}
{%- set columns = metadata_dict['columns'] -%}

{{ datavault_extension.load(source_model=source_model
                    , default_columns=default_columns
                    , additional_columns=additional_columns
                    , key_check=key_check
                    , dub_check=dub_check
                    , hwm=hwm
                    , sourcetype=sourcetype
                    , columns=columns
                    ) }}
