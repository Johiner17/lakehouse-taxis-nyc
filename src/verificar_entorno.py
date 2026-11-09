"""Prueba de humo: crea una tabla Delta, la modifica y lee una versión anterior."""
from importlib.metadata import version

from delta.tables import DeltaTable

from spark_session import get_spark

import shutil


RUTA = "/app/data/_prueba/numeros"  # carpeta de la tabla Delta (dentro de data/, ignorada por git)

# Borra la tabla de pruebas de corridas anteriores para que cada ejecución empiece limpia.
shutil.rmtree(RUTA, ignore_errors=True)

spark = get_spark("verificar-entorno")
print(f"\nSpark: {spark.version} | delta-spark: {version('delta-spark')}")

# Versión 0: crea la tabla con 5 filas (0..4). "overwrite" reemplaza si ya existía.
spark.range(5).write.format("delta").mode("overwrite").save(RUTA)

# Versión 1: añade 3 filas más (100..102). Delta no modifica archivos: añade uno nuevo
# y anota el cambio en su registro de transacciones (_delta_log).
spark.range(100, 103).write.format("delta").mode("append").save(RUTA)

print("\nFilas actuales:", spark.read.format("delta").load(RUTA).count())  # espera 8

# Historial de transacciones: una fila por versión.
print("\nHistorial:")
DeltaTable.forPath(spark, RUTA).history().select("version", "operation").show()

# "Time travel": leer la tabla como estaba en la versión 0.
antes = spark.read.format("delta").option("versionAsOf", 0).load(RUTA).count()
print("Filas en la versión 0:", antes)  # espera 5

spark.stop()