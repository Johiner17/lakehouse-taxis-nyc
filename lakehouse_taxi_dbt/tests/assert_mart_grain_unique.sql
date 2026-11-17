select
    mes,
    distrito
from {{ ref('mrt_demanda_mensual_distrito') }}
group by mes, distrito
having count(*) > 1
