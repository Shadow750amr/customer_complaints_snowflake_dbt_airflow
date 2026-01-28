
    with cleansed_data as (
    select
    cast("expediente" as string) as expediente,
    to_date("fecha_ingreso", 'YYYY/MM/DD') as fecha_ingreso,
    "anio_creacion" as anio_creacion,
    "estado_procesal" as estado_procesal,
    "razon_social" as razon_social,
    "nombre_comercial" as nombre_comercial,
    "giro" as giro,
    "sector" as sector,
    "area_responsable" as area_responsable,
    "estado" as estado,
    "motivo_reclamacion" as motivo_reclamacion,
    --- columna de auditoría
    current_timestamp() as updated_at
      from
    {{ source('complaints_bronze', 'bronze_complaints') }}    
    
    )

    select *,
    {{dbt_utils.generate_surrogate_key(['expediente','fecha_ingreso','estado_procesal']) }} as s_key_main ---this is actually not working because of the duplicates
    from cleansed_data
  