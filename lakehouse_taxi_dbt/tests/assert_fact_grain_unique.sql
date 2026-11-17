select
    fecha_recogida,
    zona_origen_id
from {{ ref('stg_fct_demanda_diaria_zona') }}
group by fecha_recogida, zona_origen_id
having count(*) > 1
