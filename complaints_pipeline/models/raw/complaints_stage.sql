
    select
    cast("expediente" as string) as expediente,
    to_date("fecha_ingreso", 'YYYY/MM/DD') as fecha_ingreso,
    --- columna de auditoría
    current_timestamp() as updated_at
    from
    {{ source('raw', 'bronze_complaints') }}    
    