select *
from {{ ref('stg_fct_demanda_diaria_zona') }}
where viajes < 0
   or viajes_con_alertas < 0
   or viajes_limpios < 0
   or ingresos_total < 0
   or propinas_total < 0
   or distancia_total_millas < 0
   or duracion_total_min < 0
   or viajes_pago_tarjeta < 0
