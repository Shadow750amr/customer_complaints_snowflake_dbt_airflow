
    
    select
    cast("expediente" as string) as expediente,
    to_date("fecha_ingreso", 'YYYY/MM/DD') as fecha_ingreso,
    "estado" as estado,
    "razon_social" as razon_social,
    "motivo_reclamacion" as motivo_reclamacion,
    "estado_procesal" as estado_procesal,
    "nombre_comercial" as nombre_comercial,
    "giro" as giro,
    "sector" as sector,
    --- columna de auditoría
    current_timestamp() as updated_at
    from
    {{ source('raw', 'bronze_complaints') }}    
    