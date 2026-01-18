
{{config(
    materialized='table',
    )}}


SELECT DISTINCT(estado) FROM {{ref('complaints_stage')}}