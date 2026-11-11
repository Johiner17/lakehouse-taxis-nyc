# Databricks notebook source
from pyspark.sql import functions as F

# Todo lo que cambia entre entornos vive aquí arriba.
RUTA_YELLOW  = "/Volumes/workspace/landing/yellow"
TABLA_BRONZE = "workspace.bronze.taxis_yellow"
FORZAR = False   # True = recargar aunque el mes ya exista en bronze

# Mantiene las horas de NYC tal cual vienen en el archivo (sin desplazarlas).
spark.conf.set("spark.sql.session.timeZone", "UTC")

# Esquema canónico: nombres en minúscula y tipos forzados.
# Los meses de TLC no siempre traen el mismo tipo (p. ej. passenger_count
# a veces es double y a veces long); así todos los meses quedan iguales.
ESQUEMA = {
    "vendorid": "int",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "int",
    "trip_distance": "double",
    "ratecodeid": "int",
    "store_and_fwd_flag": "string",
    "pulocationid": "int",
    "dolocationid": "int",
    "payment_type": "int",
    "fare_amount": "double",
    "extra": "double",
    "mta_tax": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "improvement_surcharge": "double",
    "total_amount": "double",
    "congestion_surcharge": "double",
    "airport_fee": "double",
}

# COMMAND ----------

def meses_disponibles():
    """Devuelve {'2024-01': ruta_del_archivo, ...} leyendo el volumen."""
    meses = {}
    for f in dbutils.fs.ls(RUTA_YELLOW):
        if f.name.endswith(".parquet"):
            # yellow_tripdata_2024-01.parquet -> 2024-01
            mes = f.name.replace("yellow_tripdata_", "").replace(".parquet", "")
            meses[mes] = f.path
    return dict(sorted(meses.items()))


def mes_ya_cargado(mes):
    if not spark.catalog.tableExists(TABLA_BRONZE):
        return False
    return spark.table(TABLA_BRONZE).filter(F.col("_mes_origen") == mes).limit(1).count() > 0


def cargar_mes(mes, ruta):
    # 1) Leer y guardar el nombre del archivo (hay que hacerlo justo tras leer).
    df = spark.read.parquet(ruta).withColumn("_archivo", F.col("_metadata.file_name"))
    filas_origen = df.count()

    # 2) Nombres en minúscula (Airport_fee vs airport_fee según el año).
    df = df.toDF(*[c.lower() for c in df.columns])

    # 3) Forzar el esquema canónico; si falta una columna, entra como NULL.
    for nombre, tipo in ESQUEMA.items():
        if nombre in df.columns:
            df = df.withColumn(nombre, F.col(nombre).cast(tipo))
        else:
            df = df.withColumn(nombre, F.lit(None).cast(tipo))

    # 4) Columnas extra que traiga el archivo (p. ej. cbd_congestion_fee en 2025)
    #    se conservan al final; bronze no tira datos.
    extras = [c for c in df.columns if c not in ESQUEMA and not c.startswith("_")]
    df = df.select(*ESQUEMA.keys(), *extras, "_archivo")

    # 5) Metadata de trazabilidad.
    df = (df.withColumn("_mes_origen", F.lit(mes))
            .withColumn("_cargado_en", F.current_timestamp()))

    # 6) Escritura idempotente: reemplaza SOLO las filas de ese mes.
    if spark.catalog.tableExists(TABLA_BRONZE):
        (df.write.format("delta").mode("overwrite")
           .option("replaceWhere", f"_mes_origen = '{mes}'")
           .saveAsTable(TABLA_BRONZE))
    else:
        (df.write.format("delta").mode("overwrite")
           .partitionBy("_mes_origen")
           .saveAsTable(TABLA_BRONZE))

    # 7) Comprobación: lo escrito debe igualar lo leído.
    filas_bronze = spark.table(TABLA_BRONZE).filter(F.col("_mes_origen") == mes).count()
    assert filas_bronze == filas_origen, f"{mes}: origen {filas_origen} != bronze {filas_bronze}"
    return filas_bronze

# COMMAND ----------

total = 0
for mes, ruta in meses_disponibles().items():
    if mes_ya_cargado(mes) and not FORZAR:
        print(f"{mes}: ya estaba en bronze, se salta")
        continue
    n = cargar_mes(mes, ruta)
    total += n
    print(f"{mes}: {n:,} filas cargadas")
print(f"Filas cargadas en esta corrida: {total:,}")

# COMMAND ----------

display(spark.sql(f"""
    SELECT _mes_origen, COUNT(*) AS filas
    FROM {TABLA_BRONZE}
    GROUP BY _mes_origen
    ORDER BY _mes_origen
"""))