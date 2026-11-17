select
    fecha_recogida,
    zona_origen_id,
    viajes,
    viajes_con_alertas,
    viajes_limpios,
    ingresos_total,
    propinas_total,
    distancia_total_millas,
    duracion_total_min,
    viajes_pago_tarjeta,
    ingested_at
from {{ source('raw', 'fct_demanda_diaria_zona') }}