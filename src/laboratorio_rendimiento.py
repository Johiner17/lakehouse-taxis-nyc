"""Laboratorio de rendimiento: mide el efecto de decisiones de almacenamiento y de Spark.

Uso: python src/laboratorio_rendimiento.py --exp A|B|C|D|todos
Con --exp todos también escribe el informe docs/rendimiento.md.
"""

import argparse
import json
import os
import shutil
import statistics
import subprocess
import sys
import time
from importlib.metadata import version
from pathlib import Path
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from spark_session import get_spark

SILVER = "/app/data/lakehouse/silver/taxis_yellow"
LAB = Path("/app/data/lab")  # tablas temporales del laboratorio (dentro de data/, ignorada por git)
INFORME = Path("/app/docs/rendimiento.md")
RESULTADOS = []  # (experimento, variante, segundos, detalle)
RANGO = ""  # texto con el mínimo y máximo de la última medición (lo lee registrar)

def medir(consulta, repeticiones=3):
    """Ejecuta la consulta varias veces y devuelve la mediana en segundos.
    El formato 'noop' ejecuta todo el plan pero no escribe nada: medimos solo la consulta."""
    global RANGO
    tiempos = []
    for _ in range(repeticiones):
        t0 = time.perf_counter()
        consulta().write.format("noop").mode("overwrite").save()
        tiempos.append(time.perf_counter() - t0)
    RANGO = f"rango {min(tiempos):.1f}-{max(tiempos):.1f} s"
    return statistics.median(tiempos)


def registrar(exp, variante, segundos, detalle=""):
    # medir() ya se ejecutó cuando llega aquí (Python evalúa los argumentos primero), así que RANGO está al día.
    detalle = f"{detalle} ({RANGO})".strip()
    RESULTADOS.append((exp, variante, round(segundos, 1), detalle))
    print(f"[{exp}] {variante}: {segundos:.1f} s {detalle}")


def detalle_tabla(spark, ruta):
    d = DeltaTable.forPath(spark, ruta).detail().select("numFiles", "sizeInBytes").first()
    return f"{d['numFiles']} archivos, {d['sizeInBytes'] / 1e6:.0f} MB"


def preparar(spark, nombre, meses, archivos):
    """Copia unos meses de silver a una tabla de laboratorio con N archivos."""
    ruta = str(LAB / nombre)
    shutil.rmtree(ruta, ignore_errors=True)
    (
        spark.read.format("delta").load(SILVER)
        .where(F.col("_mes_origen").isin(meses))
        .repartition(archivos)  # repartition(N) -> N archivos al escribir
        .write.format("delta").mode("overwrite").save(ruta)
    )
    return ruta


def exp_a(spark):
    """A: problema de archivos pequeños."""
    ruta = preparar(spark, "a_fragmentado", ["2024-01"], 400)

    def consulta():
        return (
            spark.read.format("delta").load(ruta)
            .groupBy("pulocationid").agg(F.count("*"), F.sum("total_amount"))
        )

    registrar("A", "fragmentado", medir(consulta), detalle_tabla(spark, ruta))
    t0 = time.perf_counter()
    DeltaTable.forPath(spark, ruta).optimize().executeCompaction()  # junta archivos pequeños
    print(f"OPTIMIZE tardó {time.perf_counter() - t0:.1f} s")
    registrar("A", "tras OPTIMIZE", medir(consulta), detalle_tabla(spark, ruta))


def exp_b(spark):
    """B: poda de particiones (silver completo, particionado por _mes_origen)."""
    dia = F.to_date(F.lit("2024-01-15"))

    def base():
        return spark.read.format("delta").load(SILVER)

    def con_particion():
        return base().where((F.col("_mes_origen") == "2024-01") & (F.col("fecha_recogida") == dia)) \
            .agg(F.count("*"), F.sum("total_amount"))

    def sin_particion():
        return base().where(F.col("fecha_recogida") == dia).agg(F.count("*"), F.sum("total_amount"))

    def escaneo_completo():
        # hora_recogida no es columna de partición y su rango abarca todos los archivos:
        # ni la poda ni el salto por estadísticas pueden evitar leer los 36 meses.
        return base().where(F.col("hora_recogida") == 8).agg(F.count("*"), F.sum("total_amount"))

    registrar("B", "escaneo completo (filtro por hora)", medir(escaneo_completo))

    registrar("B", "filtro con columna de partición", medir(con_particion))
    registrar("B", "filtro solo por fecha", medir(sin_particion))


def exp_c(spark):
    """C: Z-order sobre la columna por la que más se filtra."""
    # Tamaño objetivo de archivo: con el valor por defecto (1 GB) todo quedaría en un solo
    # archivo y Z-order no tendría nada que separar. Si tras el ZORDER ves 1 archivo,
    # esta configuración no se aplicó y lo revisamos.
    spark.conf.set("spark.databricks.delta.optimize.maxFileSize", str(16 * 1024 * 1024))
    ruta = preparar(spark, "c_zorder", ["2024-01", "2024-02", "2024-03"], 8)

    def consulta():  # zona 132 = aeropuerto JFK
        return (
            spark.read.format("delta").load(ruta)
            .where(F.col("pulocationid") == 132).agg(F.count("*"), F.sum("total_amount"))
        )

    registrar("C", "sin ordenar", medir(consulta), detalle_tabla(spark, ruta))
    DeltaTable.forPath(spark, ruta).optimize().executeZOrderBy("pulocationid")
    registrar("C", "ZORDER BY pulocationid", medir(consulta), detalle_tabla(spark, ruta))


def exp_d(spark):
    """D: particiones de shuffle y AQE (ejecución adaptativa)."""
    ruta = preparar(spark, "d_shuffle", ["2024-01", "2024-02", "2024-03"], 8)

    def consulta():
        return (
            spark.read.format("delta").load(ruta)
            .groupBy("pulocationid", "dolocationid").agg(F.count("*"), F.sum("total_amount"))
        )

    variantes = [
        ("AQE apagado, 200 particiones", "false", "200"),
        ("AQE apagado, 8 particiones", "false", "8"),
        ("AQE encendido, 200 particiones", "true", "200"),
    ]
    for nombre, aqe, particiones in variantes:
        spark.conf.set("spark.sql.adaptive.enabled", aqe)
        spark.conf.set("spark.sql.shuffle.partitions", particiones)
        registrar("D", nombre, medir(consulta))
    # Restaura la configuración del proyecto.
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "8")


def escribir_informe():
    INFORME.parent.mkdir(parents=True, exist_ok=True)
    lineas = [
        "# Laboratorio de rendimiento",
        "",
        f"Entorno: Spark {version('pyspark')}, Delta {version('delta-spark')}, modo local con 2 hilos, "
        f"driver de {os.getenv('SPARK_DRIVER_MEMORY', '2g')}. Mediana de 3 ejecuciones.",
        "",
        "| Experimento | Variante | Segundos | Detalle |",
        "| --- | --- | --- | --- |",
    ]
    lineas += [f"| {e} | {v} | {s} | {d} |" for e, v, s, d in RESULTADOS]
    INFORME.write_text("\n".join(lineas) + "\n", encoding="utf-8")
    print(f"\nInforme escrito en {INFORME}")


EXPERIMENTOS = {"A": exp_a, "B": exp_b, "C": exp_c, "D": exp_d}

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--exp", default="todos", choices=["A", "B", "C", "D", "todos"])
    args = p.parse_args()

    if args.exp == "todos":
        # Cada experimento corre en su propio proceso: una JVM nueva cada vez, así la memoria
        # que usó uno no afecta al siguiente. Este proceso padre no abre Spark: solo coordina.
        LAB.mkdir(parents=True, exist_ok=True)
        for exp in EXPERIMENTOS:
            subprocess.run([sys.executable, __file__, "--exp", exp], check=True)
            guardado = json.loads((LAB / f"resultados_{exp}.json").read_text())
            RESULTADOS.extend(tuple(fila) for fila in guardado)
        escribir_informe()
    else:
        spark = get_spark("laboratorio-rendimiento")
        EXPERIMENTOS[args.exp](spark)
        LAB.mkdir(parents=True, exist_ok=True)
        # Los resultados se guardan en un JSON para que el proceso padre los recoja.
        (LAB / f"resultados_{args.exp}.json").write_text(json.dumps(RESULTADOS))
        spark.stop()
