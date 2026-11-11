"""Chequeo: totales y top zonas de 2024 en el gold local, para compararlos con Databricks."""
from pyspark.sql import functions as F

from spark_session import get_spark

spark = get_spark("chequeo-2024")
f = (spark.read.format("delta")
     .load("/app/data/lakehouse/gold/fct_demanda_diaria_zona")
     .where(F.year("fecha_recogida") == 2024))

f.agg(F.sum("viajes").alias("viajes"),
      F.round(F.sum("ingresos_total"), 2).alias("ingresos")).show(truncate=False)

(f.groupBy("zona_origen_id")
  .agg(F.sum("viajes").alias("viajes"),
       F.round(F.sum("ingresos_total") / F.sum("viajes"), 2).alias("ingreso_medio"))
  .orderBy(F.desc("viajes")).limit(10).show(truncate=False))
spark.stop()