SELECT 
fecha_ingreso,
day(fecha_ingreso),
month(fecha_ingreso),
year(fecha_ingreso)
 FROM {{ref('complaints_stage')}}