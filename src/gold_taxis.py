"""Gold: dim_zona y fct_demanda_diaria_zona (día x zona de origen).

Uso: python src/gold_taxis.py [--solo 2024-01]
Idempotente: cada mes se carga con MERGE; repetirlo da el mismo resultado.
"""
import argparse
from datetime import date

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from spark_session import get_spark

SILVER = "/app/data/lakehouse/silver/taxis_yellow"
ZONAS_CSV = "/app/data/landing/zonas/taxi_zone_lookup.csv"
GOLD_DIM_ZONA = "/app/data/lakehouse/gold/dim_zona"
GOLD_FCT = "/app/data/lakehouse/gold/fct_demanda_diaria_zona"


def cargar_dim_zona(spark):
    """Dimensión pequeña: se reemplaza completa en cada corrida."""
    df = (
        spark.read.option("header", True).csv(ZONAS_CSV)
        .select(
            F.col("LocationID").cast("int").alias("zona_id"),
            F.col("Borough").alias("distrito"),
            F.col("Zone").alias("zona"),
            F.col("service_zone").alias("tipo_servicio"),
        )
    )
    df.write.format("delta").mode("overwrite").save(GOLD_DIM_ZONA)
    print(f"dim_zona: {df.count()} zonas")


def limites_mes(mes: str):
    """'2024-01' -> ('2024-01-01', '2024-02-01'): inicio incluido, fin excluido."""
    a, m = map(int, mes.split("-"))
    inicio = date(a, m, 1)
    fin = date(a + 1, 1, 1) if m == 12 else date(a, m + 1, 1)
    return inicio.isoformat(), fin.isoformat()

def meses_en_gold(spark) -> set[str]:
    """Meses que ya tienen datos en la tabla de hechos (se deducen de fecha_recogida)."""
    if not DeltaTable.isDeltaTable(spark, GOLD_FCT):
        return set()
    filas = (
        spark.read.format("delta").load(GOLD_FCT)
        .select(F.date_format("fecha_recogida", "yyyy-MM").alias("mes")).distinct().collect()
    )
    return {f["mes"] for f in filas}


def procesar_mes(spark, mes: str):
    inicio, fin = limites_mes(mes)
    s = spark.read.format("delta").load(SILVER).where(F.col("_mes_origen") == mes)
    limpio = ~F.col("tiene_alertas")

    def si_limpio(expr):
        """Suma solo los viajes sin alertas."""
        return F.sum(F.when(limpio, expr).otherwise(0))

    agg = s.groupBy("fecha_recogida", F.col("pulocationid").alias("zona_origen_id")).agg(
        F.count("*").alias("viajes"),
        F.sum(F.when(F.col("tiene_alertas"), 1).otherwise(0)).alias("viajes_con_alertas"),
        si_limpio(F.lit(1)).alias("viajes_limpios"),
        si_limpio(F.col("total_amount")).alias("ingresos_total"),
        si_limpio(F.col("tip_amount")).alias("propinas_total"),
        si_limpio(F.col("trip_distance")).alias("distancia_total_millas"),
        si_limpio(F.col("duracion_min")).alias("duracion_total_min"),
        si_limpio(F.when(F.col("payment_type") == 1, 1).otherwise(0)).alias("viajes_pago_tarjeta"),
    )

    if not DeltaTable.isDeltaTable(spark, GOLD_FCT):
        agg.write.format("delta").save(GOLD_FCT)  # el primer mes crea la tabla
    else:
        (
            DeltaTable.forPath(spark, GOLD_FCT).alias("t")
            # <=> es igualdad que trata null = null como verdadero (por si una zona es nula).
            .merge(
                agg.alias("s"),
                "t.fecha_recogida <=> s.fecha_recogida AND t.zona_origen_id <=> s.zona_origen_id",
            )
            .whenMatchedUpdateAll()       # existe: se actualiza
            .whenNotMatchedInsertAll()    # no existe: se inserta
            # Filas del destino, dentro de ESTE mes, que ya no salen en el cálculo: se borran.
            .whenNotMatchedBySourceDelete(
                condition=f"t.fecha_recogida >= '{inicio}' AND t.fecha_recogida < '{fin}'"
            )
            .execute()
        )

    # Conciliación: la suma de viajes en gold debe ser igual a las filas de silver del mes.
    en_gold = (
        spark.read.format("delta").load(GOLD_FCT)
        .where((F.col("fecha_recogida") >= F.to_date(F.lit(inicio))) & (F.col("fecha_recogida") < F.to_date(F.lit(fin))))
        .agg(F.sum("viajes")).first()[0]
    )
    en_silver = s.count()
    assert en_gold == en_silver, f"{mes}: gold suma {en_gold}, silver tiene {en_silver}"
    print(f"{mes}: {en_silver:,} viajes agregados")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--solo", help="procesar un único mes (AAAA-MM), siempre se recalcula")
    p.add_argument("--forzar", action="store_true", help="recalcular también los meses ya cargados")
    args = p.parse_args()

    spark = get_spark("gold-taxis")
    cargar_dim_zona(spark)

    if args.solo:
        meses = [args.solo]
    else:
        filas = spark.read.format("delta").load(SILVER).select("_mes_origen").distinct().collect()
        meses = sorted(f["_mes_origen"] for f in filas)
    ya = set() if (args.solo or args.forzar) else meses_en_gold(spark)
    for mes in meses:
        if mes in ya:
            print(f"{mes}: omitido (ya cargado)")
            continue
        procesar_mes(spark, mes)

    # Primera consulta de negocio: las 10 zonas con más viajes (une el hecho con la dimensión).
    hechos = (
        spark.read.format("delta").load(GOLD_FCT)
        .groupBy("zona_origen_id")
        .agg(F.sum("viajes").alias("viajes"), F.round(F.sum("ingresos_total"), 0).alias("ingresos"))
    )
    dim = spark.read.format("delta").load(GOLD_DIM_ZONA)
    print("\nTop 10 zonas de origen:")
    (
        hechos.join(dim, hechos.zona_origen_id == dim.zona_id, "left")
        .select("zona", "distrito", "viajes", "ingresos")
        .orderBy(F.desc("viajes")).show(10, truncate=False)
    )
    spark.stop()