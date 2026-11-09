"""Carga los Parquet de taxis amarillos a la tabla Delta bronze.

Uso: python src/bronze_taxis.py [--forzar]
"""
import argparse
import re
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from spark_session import get_spark

LANDING = Path("/app/data/landing/yellow")
BRONZE = "/app/data/lakehouse/bronze/taxis_yellow"

# Esquema canónico: nombre en minúsculas -> tipo. Los Parquet de distintos años traen
# tipos distintos (p. ej. passenger_count a veces double, a veces entero) y nombres con
# otra capitalización (Airport_fee / airport_fee). Forzar tipos evita que Delta rechace
# un mes por esquema. Si un mes no trae una columna, se rellena con null.
ESQUEMA = {
    "vendorid": "long",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "long",
    "trip_distance": "double",
    "ratecodeid": "long",
    "store_and_fwd_flag": "string",
    "pulocationid": "long",
    "dolocationid": "long",
    "payment_type": "long",
    "fare_amount": "double",
    "extra": "double",
    "mta_tax": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "improvement_surcharge": "double",
    "total_amount": "double",
    "congestion_surcharge": "double",
    "airport_fee": "double",
    "cbd_congestion_fee": "double",  # existe solo desde 2025
}


def normalizar(df, mes: str, archivo: str):
    # 1) Nombres en minúsculas.
    df = df.toDF(*[c.lower() for c in df.columns])
    # 2) Columnas canónicas con su tipo (null si el mes no la trae).
    cols = [
        (F.col(n).cast(t) if n in df.columns else F.lit(None).cast(t)).alias(n)
        for n, t in ESQUEMA.items()
    ]
    # 3) Columnas desconocidas se conservan tal cual (bronze no descarta datos).
    extras = [F.col(c) for c in df.columns if c not in ESQUEMA]
    # 4) Metadatos de origen (el "sobre" de este proyecto).
    meta = [
        F.lit(archivo).alias("_archivo"),
        F.lit(mes).alias("_mes_origen"),
        F.current_timestamp().alias("_cargado_en"),
    ]
    return df.select(*cols, *extras, *meta)


def meses_cargados(spark) -> set[str]:
    if not DeltaTable.isDeltaTable(spark, BRONZE):
        return set()
    filas = spark.read.format("delta").load(BRONZE).select("_mes_origen").distinct().collect()
    return {f["_mes_origen"] for f in filas}


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--forzar", action="store_true", help="recargar meses ya cargados")
    args = p.parse_args()

    spark = get_spark("bronze-taxis")
    ya = meses_cargados(spark)

    for ruta in sorted(LANDING.glob("yellow_tripdata_*.parquet")):
        mes = re.search(r"(\d{4}-\d{2})", ruta.name).group(1)
        if mes in ya and not args.forzar:
            print(f"{mes}: omitido (ya cargado)")
            continue
        df = normalizar(spark.read.parquet(str(ruta)), mes, ruta.name)
        filas = df.count()
        # replaceWhere: reemplaza SOLO la partición de ese mes, de forma atómica.
        # Cargar el mismo mes dos veces deja el mismo resultado (idempotente).
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"_mes_origen = '{mes}'")
            .option("mergeSchema", "true")  # permite columnas nuevas en meses futuros
            .partitionBy("_mes_origen")
            .save(BRONZE)
        )
        print(f"{mes}: cargado, {filas:,} filas")

    print("\nResumen del bronze:")
    (
        spark.read.format("delta").load(BRONZE)
        .groupBy("_mes_origen").count().orderBy("_mes_origen").show(50)
    )
    spark.stop()