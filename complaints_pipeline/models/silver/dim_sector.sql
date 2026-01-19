{{config(materialized='table')}}


WITH 
unique_values as (
    -- 2. Logical CTE: Deduplicate the data first
    select 
        GIRO, 
        SECTOR
    from {{ref('complaints_stage')}}
    group by 1, 2
)

select DISTINCT {{dbt_utils.generate_surrogate_key(['GIRO','SECTOR'])}} as s_key,
        GIRO,SECTOR
             from unique_values