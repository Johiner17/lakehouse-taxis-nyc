# Databricks notebook source
from datetime import date
from pyspark.sql import functions as F

SILVER   = "workspace.silver.taxis_yellow"
DIM_ZONA = "workspace.gold.dim_zona"
FACT     = "workspace.gold.fct_demanda_diaria_zona"
RUTA_ZONAS = "/Volumes/workspace/landing/zonas/taxi_zone_lookup.csv"

SOLO   = None    # para probar un mes: SOLO = "2024-01"
FORZAR = False   # True = recalcular meses que ya están en gold

spark.conf.set("spark.sql.session.timeZone", "UTC")

# La tabla de hechos se crea vacía con su esquema explícito (el MERGE la necesita).
# Solo medidas ADITIVAS (sumas y conteos): los promedios se calculan al consultar.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {FACT} (
        fecha_recogida         DATE,
        zona_origen_id         INT,
        viajes                 BIGINT,
        viajes_con_alertas     BIGINT,
        viajes_limpios         BIGINT,
        ingresos_total         DOUBLE,
        propinas_total         DOUBLE,
        distancia_total_millas DOUBLE,
        duracion_total_min     DOUBLE,
        viajes_pago_tarjeta    BIGINT,
        _actualizado_en        TIMESTAMP
    ) USING DELTA
""")

# COMMAND ----------

# dim_zona se reescribe completa: es pequeña (unas 265 filas).
zonas = (
    spark.read.option("header", True).csv(RUTA_ZONAS)
    .select(
        F.col("LocationID").cast("int").alias("zona_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zona"),
        F.col("service_zone").alias("service_zone"),
    )
)
zonas.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(DIM_ZONA)
print(f"dim_zona: {spark.table(DIM_ZONA).count()} filas")

# COMMAND ----------

def meses_en_silver():
    return {r[0] for r in spark.table(SILVER).select("_mes_origen").distinct().collect()}


def meses_en_gold():
    filas = (spark.table(FACT)
             .select(F.date_format("fecha_recogida", "yyyy-MM").alias("m")).distinct().collect())
    return {r["m"] for r in filas}


def limites_mes(mes):
    """'2024-01' -> ('2024-01-01', '2024-02-01'): inicio inclusivo, fin exclusivo."""
    a, m = int(mes[:4]), int(mes[5:])
    ini = date(a, m, 1)
    fin = date(a + (m == 12), 1 if m == 12 else m + 1, 1)
    return ini.isoformat(), fin.isoformat()


def procesar_mes(mes):
    ini, fin = limites_mes(mes)
    silver_mes = spark.table(SILVER).where(F.col("_mes_origen") == mes)

    # Agregación: un registro por (día, zona de origen).
    # Las medidas de dinero, distancia y duración suman SOLO los viajes limpios
    # (sin alertas): así un reembolso o una distancia imposible no distorsionan los totales.
    # "viajes" sí cuenta todos, y "viajes_con_alertas" dice cuántos quedaron fuera.
    limpio = ~F.col("tiene_alertas")

    def si_limpio(expr):
        """Suma solo los viajes sin alertas."""
        return F.sum(F.when(limpio, expr).otherwise(0))

    agg = (
        silver_mes.groupBy("fecha_recogida", F.col("pulocationid").alias("zona_origen_id"))
        .agg(
            F.count("*").alias("viajes"),
            F.sum(F.when(F.col("tiene_alertas"), 1).otherwise(0)).alias("viajes_con_alertas"),
            si_limpio(F.lit(1)).alias("viajes_limpios"),
            si_limpio(F.col("total_amount")).alias("ingresos_total"),
            si_limpio(F.col("tip_amount")).alias("propinas_total"),
            si_limpio(F.col("trip_distance")).alias("distancia_total_millas"),
            si_limpio(F.col("duracion_min")).alias("duracion_total_min"),
            si_limpio(F.when(F.col("payment_type") == 1, 1).otherwise(0)).alias("viajes_pago_tarjeta"),
        )
        .withColumn("_actualizado_en", F.current_timestamp())
    )
    agg.createOrReplaceTempView("agg_mes")

    # MERGE idempotente: actualiza lo que existe, inserta lo nuevo y borra lo que
    # ya no aparece en la fuente, pero SOLO dentro de este mes.
    # "<=>" compara igual que "=" pero trata null = null como verdadero.
    spark.sql(f"""
        MERGE INTO {FACT} t
        USING agg_mes s
        ON t.fecha_recogida = s.fecha_recogida AND t.zona_origen_id <=> s.zona_origen_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        WHEN NOT MATCHED BY SOURCE
             AND t.fecha_recogida >= '{ini}' AND t.fecha_recogida < '{fin}' THEN DELETE
    """)

    # Conciliación: los viajes sumados en gold deben igualar las filas de silver.
    n_silver = silver_mes.count()
    n_gold = (spark.table(FACT)
              .where((F.col("fecha_recogida") >= ini) & (F.col("fecha_recogida") < fin))
              .agg(F.sum("viajes")).collect()[0][0])
    assert n_gold == n_silver, f"{mes}: gold {n_gold} != silver {n_silver}"
    print(f"{mes}: silver {n_silver:,} = gold {n_gold:,} viajes")

# COMMAND ----------

ya = meses_en_gold()
pendientes = sorted(meses_en_silver())
if SOLO:
    pendientes = [SOLO]

for mes in pendientes:
    if mes in ya and not FORZAR:
        print(f"{mes}: omitido (ya procesado)")
        continue
    procesar_mes(mes)

print("\nTop 10 zonas de origen por número de viajes:")
display(spark.sql(f"""
    SELECT z.zona, z.borough,
           SUM(f.viajes) AS viajes,
           ROUND(SUM(f.ingresos_total) / SUM(f.viajes), 2) AS ingreso_medio_por_viaje
    FROM {FACT} f
    LEFT JOIN {DIM_ZONA} z ON f.zona_origen_id = z.zona_id
    GROUP BY z.zona, z.borough
    ORDER BY viajes DESC
    LIMIT 10
"""))