{{config(materialized='incremental',incremental_strategy='merge',unique_key='s_key_main',on_schema_change='append_new_columns')}}

select
*
from {{ref('complaints_stage')}}


