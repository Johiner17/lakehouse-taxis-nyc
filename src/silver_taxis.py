"""Silver: limpia el bronze mes a mes.

- Elimina duplicados exactos.
- Manda a cuarentena las filas imposibles (con el motivo).
- Conserva las sospechosas con columnas de alerta.
- Añade columnas derivadas.
Idempotente por mes (replaceWhere), igual que el bronze.

Uso: python src/silver_taxis.py [--solo 2024-01] [--forzar]
"""
import argparse
import operator
from functools import reduce

from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import functions as F

from spark_session import get_spark

BRONZE = "/app/data/lakehouse/bronze/taxis_yellow"
SILVER = "/app/data/lakehouse/silver/taxis_yellow"
RECHAZADOS = "/app/data/lakehouse/silver/taxis_yellow_rechazados"

# Las expresiones de Spark deben crearse DESPUÉS de crear la sesión, por eso van en
# funciones (se llaman desde procesar_mes, cuando la sesión ya existe).
def duracion_seg():
    return F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")


def motivo_rechazo():
    """Motivo de rechazo: gana el primero que se cumple; si ninguno, null (fila válida)."""
    mes_recogida = F.date_format("tpep_pickup_datetime", "yyyy-MM")
    return (
        F.when(
            F.col("tpep_pickup_datetime").isNull() | F.col("tpep_dropoff_datetime").isNull(),
            "fecha_nula",
        )
        .when(mes_recogida != F.col("_mes_origen"), "recogida_fuera_de_mes")
        .when(duracion_seg() < 0, "bajada_antes_de_subida")
        .when(duracion_seg() > 86400, "duracion_mayor_24h")
    )


def alerta(condicion):
    """Convierte una condición a booleano sin nulos (null cuenta como 'no hay alerta')."""
    return F.coalesce(condicion, F.lit(False))


def meses_en(spark, ruta) -> set[str]:
    if not DeltaTable.isDeltaTable(spark, ruta):
        return set()
    filas = spark.read.format("delta").load(ruta).select("_mes_origen").distinct().collect()
    return {f["_mes_origen"] for f in filas}


def escribir(df, ruta, mes):
    """Reemplaza solo la partición de ese mes, de forma atómica (idempotente)."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"_mes_origen = '{mes}'")
        .option("mergeSchema", "true")
        .partitionBy("_mes_origen")
        .save(ruta)
    )


def procesar_mes(spark, mes):
    raw = spark.read.format("delta").load(BRONZE).where(F.col("_mes_origen") == mes)
    n_bronze = raw.count()

    # Columnas de datos (las que no empiezan con "_" son las del origen, no los metadatos).
    cols_datos = [c for c in raw.columns if not c.startswith("_")]

    # 1) Duplicados exactos fuera, y marcamos el motivo de rechazo.
    # persist: guardamos el resultado en memoria/disco para no recalcularlo en cada conteo.
    base = (
        raw.dropDuplicates(cols_datos)
        .withColumn("_motivo_rechazo", motivo_rechazo())
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    n_base = base.count()
    duplicados = n_bronze - n_base
    n_rech = base.where(F.col("_motivo_rechazo").isNotNull()).count()
    n_validos = n_base - n_rech

    # 2) Cuarentena: filas imposibles, con su motivo.
    rechazados = (
        base.where(F.col("_motivo_rechazo").isNotNull())
        .withColumn("_procesado_en", F.current_timestamp())
    )
    escribir(rechazados, RECHAZADOS, mes)

    # 3) Válidos: columnas derivadas y alertas.
    alertas = {
        "alerta_pasajeros_desconocido": alerta(F.col("passenger_count").isNull()),
        "alerta_pasajeros_atipico": alerta((F.col("passenger_count") == 0) | (F.col("passenger_count") > 6)),
        "alerta_duracion_cero": alerta(duracion_seg() == 0),
        "alerta_distancia_cero": alerta(F.col("trip_distance") <= 0),
        "alerta_distancia_atipica": alerta(F.col("trip_distance") > 100),
        "alerta_importe_no_positivo": alerta(F.col("total_amount") <= 0),
    }
    validos = (
        base.where(F.col("_motivo_rechazo").isNull())
        .drop("_motivo_rechazo")
        # Identificador del viaje: hash de 64 bits de todas las columnas de datos.
        # Como ya quitamos duplicados exactos, cada viaje tiene un id distinto.
        # Sirve como llave para MERGE en gold.
        .withColumn("viaje_id", F.xxhash64(*[F.col(c) for c in cols_datos]))
        .withColumn("fecha_recogida", F.to_date("tpep_pickup_datetime"))
        .withColumn("hora_recogida", F.hour("tpep_pickup_datetime"))
        .withColumn("duracion_min", F.round(duracion_seg() / 60.0, 2))
        .withColumns(alertas)
        .withColumn("tiene_alertas", reduce(operator.or_, [F.col(c) for c in alertas]))
        .withColumn("_procesado_en", F.current_timestamp())
    )
    escribir(validos, SILVER, mes)
    base.unpersist()

    # 4) Conciliación contra lo que realmente quedó escrito en las tablas Delta.
    en_silver = spark.read.format("delta").load(SILVER).where(F.col("_mes_origen") == mes).count()
    en_cuarentena = spark.read.format("delta").load(RECHAZADOS).where(F.col("_mes_origen") == mes).count()
    assert en_silver == n_validos, f"{mes}: silver tiene {en_silver}, esperaba {n_validos}"
    assert en_cuarentena == n_rech, f"{mes}: cuarentena tiene {en_cuarentena}, esperaba {n_rech}"
    assert n_bronze == duplicados + n_rech + n_validos, f"{mes}: no concilia con el bronze"

    print(
        f"{mes}: bronze {n_bronze:,} = duplicados {duplicados:,} + rechazados {n_rech:,} "
        f"+ válidos {n_validos:,}"
    )





if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--solo", help="procesar un único mes (AAAA-MM)")
    p.add_argument("--forzar", action="store_true", help="reprocesar meses ya hechos")
    args = p.parse_args()

    spark = get_spark("silver-taxis")
    ya = meses_en(spark, SILVER)
    pendientes = sorted(meses_en(spark, BRONZE))
    if args.solo:
        pendientes = [args.solo]

    for mes in pendientes:
        if mes in ya and not args.forzar:
            print(f"{mes}: omitido (ya procesado)")
            continue
        procesar_mes(spark, mes)

    if DeltaTable.isDeltaTable(spark, RECHAZADOS):
        print("\nRechazos por motivo:")
        (
            spark.read.format("delta").load(RECHAZADOS)
            .groupBy("_motivo_rechazo").count().orderBy(F.desc("count")).show(truncate=False)
        )
    spark.stop()