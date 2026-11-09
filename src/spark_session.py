"""Crea la sesión de Spark con Delta Lake. Todos los scripts del proyecto la reutilizan."""
import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark(app_name: str = "lakehouse-taxis") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        # local[2]: Spark corre dentro de este proceso, con 2 hilos (uno por núcleo que usamos).
        .master("local[2]")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        # Activa Delta Lake: la extensión añade los comandos SQL de Delta (MERGE, OPTIMIZE...)
        # y el catálogo hace que las tablas se lean/escriban como Delta.
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Por defecto Spark usa 200 particiones tras un shuffle: absurdo para una máquina
        # pequeña (200 tareas diminutas). Con 8 vamos mucho mejor.
        .config("spark.sql.shuffle.partitions", "8")
        # Carpeta de caché de los JAR de Delta (la montamos como volumen en docker-compose).
        .config("spark.jars.ivy", "/ivy")
        # Evita sorpresas con horarios de verano al comparar fechas y horas.
        .config("spark.sql.session.timeZone", "UTC")
        # Lee los timestamps de Parquet como TIMESTAMP normal (no NTZ): evita una función
        # experimental de Delta y, con la sesión en UTC, la hora se guarda sin desplazarse.
        .config("spark.sql.parquet.inferTimestampNTZ.enabled", "false")
    )
    # Esta función añade a la sesión el JAR de Delta que corresponde a la versión instalada.
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark