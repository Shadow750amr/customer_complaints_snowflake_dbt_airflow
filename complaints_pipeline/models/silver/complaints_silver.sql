{{config(materialized='table')}}

select
stage.*,
    dim_zonas.dim_zonas_key
FROM {{ref('complaints_stage')}} stage
JOIN {{ref('dim_zonas')}} dim_zonas
ON dim_zonas.area_responsable = stage.area_responsable
AND is_current = 1


