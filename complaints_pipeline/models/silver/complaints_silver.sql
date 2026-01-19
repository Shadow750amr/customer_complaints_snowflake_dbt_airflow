{{config(materialized='table')}}

select
*,
{{ dbt_utils.generate_surrogate_key(['GIRO', 'SECTOR']) }} as sector_key 
from {{ref('complaints_stage')}}


--- on_schema_change= 'append_new_columns')