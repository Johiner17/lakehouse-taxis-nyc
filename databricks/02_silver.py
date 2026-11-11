# Databricks notebook source
import operator
from functools import reduce
from pyspark.sql import functions as F

BRONZE     = "workspace.bronze.taxis_yellow"
SILVER     = "workspace.silver.taxis_yellow"
RECHAZADOS = "workspace.silver.taxis_yellow_rechazados"

SOLO   = None    # para probar un mes: SOLO = "2024-01"
FORZAR = False   # True = reprocesar meses que ya están en silver

# Mismas horas de NYC sin desplazar (igual que en bronze).
spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

def duracion_seg():
    return F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")


def motivo_rechazo():
    """Gana el primer motivo que se cumple; si ninguno, null = fila válida."""
    mes_recogida = F.date_format("tpep_pickup_datetime", "yyyy-MM")
    return (
        F.when(F.col("tpep_pickup_datetime").isNull() | F.col("tpep_dropoff_datetime").isNull(),
               "fecha_nula")
        .when(mes_recogida != F.col("_mes_origen"), "recogida_fuera_de_mes")
        .when(duracion_seg() < 0, "bajada_antes_de_subida")
        .when(duracion_seg() > 86400, "duracion_mayor_24h")
    )


def alerta(condicion):
    """Booleano sin nulos: null cuenta como 'no hay alerta'."""
    return F.coalesce(condicion, F.lit(False))


def alertas_viaje():
    return {
        "alerta_pasajeros_desconocido": alerta(F.col("passenger_count").isNull()),
        "alerta_pasajeros_atipico": alerta((F.col("passenger_count") == 0) | (F.col("passenger_count") > 6)),
        "alerta_duracion_cero": alerta(duracion_seg() == 0),
        "alerta_distancia_cero": alerta(F.col("trip_distance") <= 0),
        "alerta_distancia_atipica": alerta(F.col("trip_distance") > 100),
        "alerta_importe_no_positivo": alerta(F.col("total_amount") <= 0),
    }

# COMMAND ----------

def meses_en(tabla):
    if not spark.catalog.tableExists(tabla):
        return set()
    return {r["_mes_origen"] for r in spark.table(tabla).select("_mes_origen").distinct().collect()}


def escribir(df, tabla, mes):
    """Reemplaza solo la partición de ese mes (idempotente)."""
    if spark.catalog.tableExists(tabla):
        (df.write.format("delta").mode("overwrite")
           .option("replaceWhere", f"_mes_origen = '{mes}'")
           .option("mergeSchema", "true")
           .saveAsTable(tabla))
    else:
        (df.write.format("delta").mode("overwrite")
           .partitionBy("_mes_origen")
           .saveAsTable(tabla))


def procesar_mes(mes):
    raw = spark.table(BRONZE).where(F.col("_mes_origen") == mes)
    n_bronze = raw.count()

    # Columnas de datos = las que no empiezan con "_" (no los metadatos).
    cols_datos = [c for c in raw.columns if not c.startswith("_")]

    # 1) Duplicados exactos. Sin persist (serverless no lo admite): contamos aparte.
    base = raw.dropDuplicates(cols_datos).withColumn("_motivo_rechazo", motivo_rechazo())
    n_base = raw.dropDuplicates(cols_datos).count()
    duplicados = n_bronze - n_base

    # 2) Cuarentena: filas imposibles, con su motivo.
    rechazados = (base.where(F.col("_motivo_rechazo").isNotNull())
                      .withColumn("_procesado_en", F.current_timestamp()))
    escribir(rechazados, RECHAZADOS, mes)

    # 3) Válidos: columnas derivadas y alertas.
    alertas = alertas_viaje()
    validos = (
        base.where(F.col("_motivo_rechazo").isNull())
        .drop("_motivo_rechazo")
        # Hash de 64 bits de las columnas de datos: llave del viaje para el MERGE de gold.
        .withColumn("viaje_id", F.xxhash64(*[F.col(c) for c in cols_datos]))
        .withColumn("fecha_recogida", F.to_date("tpep_pickup_datetime"))
        .withColumn("hora_recogida", F.hour("tpep_pickup_datetime"))
        .withColumn("duracion_min", F.round(duracion_seg() / 60.0, 2))
        .withColumns(alertas)
        .withColumn("tiene_alertas", reduce(operator.or_, [F.col(c) for c in alertas]))
        .withColumn("_procesado_en", F.current_timestamp())
    )
    escribir(validos, SILVER, mes)

    # 4) Conciliación contra lo realmente escrito en las tablas.
    en_silver = spark.table(SILVER).where(F.col("_mes_origen") == mes).count()
    en_cuarentena = spark.table(RECHAZADOS).where(F.col("_mes_origen") == mes).count()
    assert n_bronze == duplicados + en_cuarentena + en_silver, (
        f"{mes}: no concilia (bronze {n_bronze} vs {duplicados}+{en_cuarentena}+{en_silver})"
    )
    print(f"{mes}: bronze {n_bronze:,} = duplicados {duplicados:,} "
          f"+ rechazados {en_cuarentena:,} + válidos {en_silver:,}")

# COMMAND ----------

ya = meses_en(SILVER)
pendientes = sorted(meses_en(BRONZE))
if SOLO:
    pendientes = [SOLO]

for mes in pendientes:
    if mes in ya and not FORZAR:
        print(f"{mes}: omitido (ya procesado)")
        continue
    procesar_mes(mes)

print("\nRechazos por motivo:")
display(spark.table(RECHAZADOS).groupBy("_motivo_rechazo").count().orderBy(F.desc("count")))