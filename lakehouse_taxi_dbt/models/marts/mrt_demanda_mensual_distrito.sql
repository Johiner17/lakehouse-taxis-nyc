with mensual_por_distrito as (

    select
        date_trunc('month', f.fecha_recogida)::date as mes,
        coalesce(d.distrito, 'Sin distrito') as distrito,
        sum(f.viajes) as viajes,
        sum(f.viajes_con_alertas) as viajes_con_alertas,
        sum(f.viajes_limpios) as viajes_limpios,
        sum(f.ingresos_total) as ingresos_total,
        sum(f.propinas_total) as propinas_total,
        sum(f.distancia_total_millas) as distancia_total_millas,
        sum(f.duracion_total_min) as duracion_total_min,
        sum(f.viajes_pago_tarjeta) as viajes_pago_tarjeta
    from {{ ref('stg_fct_demanda_diaria_zona') }} as f
    left join {{ ref('stg_dim_zona') }} as d
        on f.zona_origen_id = d.zona_id
    group by 1, 2

)

select
    *,
    round(ingresos_total / nullif(viajes, 0), 2) as ingreso_medio_por_viaje
from mensual_por_distrito