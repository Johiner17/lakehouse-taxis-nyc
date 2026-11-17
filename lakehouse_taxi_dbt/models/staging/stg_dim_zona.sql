select
    zona_id,
    distrito,
    zona,
    tipo_servicio,
    ingested_at
from {{ source('raw', 'dim_zona') }}