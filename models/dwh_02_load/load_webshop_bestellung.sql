{{ config(materialized="view", pre_hook=[], post_hook=["{{ insert_hwm(this) }}"]) }}

{%- set yaml_metadata -%}
source_model: 
  source_table: EXT_WEBSHOP_BESTELLUNG
  source_name: LOAD_EXT
hwm: True
source_type: snowflake_external_table
dub_check:
- ldts
- BestellungID
- KundeID

key_check:
- BestellungID

columns:
    bestellungid:
      data_type: VARCHAR
      source_column_number: 1
    kundeid:
      data_type: VARCHAR
      source_column_number: 2
    allglieferadrid:
      data_type: NUMBER
      source_column_number: 3
      type_check: True
    bestelldatum:
      data_type: DATE
      format: DD.MM.YYYY
      source_column_number: 4
      type_check: True
    wunschdatum:
      data_type: DATE
      format: DD.MM.YYYY
      source_column_number: 5
      type_check: True
    rabatt:
      data_type: NUMBER
      format: 28,10
      source_column_number: 6
      decimal_separator: .
      type_check: True

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

{{ load(source_model=source_model
                    , default_columns=default_columns
                    , additional_columns=additional_columns
                    , key_check=key_check
                    , dub_check=dub_check
                    , hwm=hwm
                    , sourcetype=sourcetype
                    , columns=columns
                    ) }}
