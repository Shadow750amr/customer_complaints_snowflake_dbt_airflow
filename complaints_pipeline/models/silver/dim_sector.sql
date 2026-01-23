{{config(materialized='table')}}


WITH 
unique_values as (
    -- 2. Logical CTE: Deduplicate the data first
    select 
        giro, 
        sector
    from {{ref('complaints_stage')}}
    group by 1, 2
)

select 
        giro,sector
             from unique_values