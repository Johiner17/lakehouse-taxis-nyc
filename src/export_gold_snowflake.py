
"""
Exporta las tablas Gold de Delta Lake a archivos Parquet.

Origen:
    data/lakehouse/gold/

Destino:
    data/exports/snowflake/

Características:
    - Lee las tablas mediante Delta Lake.
    - No modifica las tablas Gold.
    - Sobrescribe las exportaciones anteriores.
    - Registra cantidad de filas y esquema.
    - Verifica que las filas exportadas coincidan con el origen.
"""

import json
from pathlib import Path

from pyspark.sql import functions as F

from spark_session import get_spark


# ==========================================================
# CONFIGURACIÓN
# ==========================================================

GOLD_PATH = Path("/app/data/lakehouse/gold")
EXPORT_PATH = Path("/app/data/exports/snowflake")

TABLAS = {
    "dim_zona": GOLD_PATH / "dim_zona",
    "fct_demanda_diaria_zona": GOLD_PATH / "fct_demanda_diaria_zona",
}


# ==========================================================
# EXPORTACIÓN
# ==========================================================

def exportar_tabla(spark, nombre: str, origen: Path):
    """Exporta una tabla Delta a Parquet y valida su resultado."""

    destino = EXPORT_PATH / nombre

    print(f"\n{'=' * 60}")
    print(f"Exportando: {nombre}")
    print(f"{'=' * 60}")

    # 1. Leer la versión vigente de Delta Lake.
    df = spark.read.format("delta").load(str(origen))

    # 2. Obtener cantidad de registros del origen.
    filas_origen = df.count()

    print(f"Filas en Gold: {filas_origen:,}")

    # 3. Exportar a Parquet.
    (
        df.write
        .mode("overwrite")
        .parquet(str(destino))
    )

    # 4. Leer nuevamente los archivos exportados.
    df_exportado = spark.read.parquet(str(destino))

    filas_exportadas = df_exportado.count()

    # 5. Validar cantidad de registros.
    if filas_origen != filas_exportadas:
        raise RuntimeError(
            f"Error en {nombre}: "
            f"origen={filas_origen}, "
            f"exportado={filas_exportadas}"
        )

    # 6. Validar esquema.
    esquema_origen = df.schema
    esquema_exportado = df_exportado.schema

    if esquema_origen != esquema_exportado:
        raise RuntimeError(
            f"El esquema exportado de {nombre} no coincide con Gold."
        )

    print(f"Filas exportadas: {filas_exportadas:,}")
    print("Validación de conteo: OK")
    print("Validación de esquema: OK")
    print(f"Destino: {destino}")

    return {
        "tabla": nombre,
        "filas": filas_exportadas,
        "columnas": df.columns,
        "destino": str(destino),
    }


# ==========================================================
# EJECUCIÓN PRINCIPAL
# ==========================================================

def main():

    EXPORT_PATH.mkdir(parents=True, exist_ok=True)

    spark = get_spark("export-gold-snowflake")

    resultados = []

    try:
        for nombre, origen in TABLAS.items():

            if not origen.exists():
                raise FileNotFoundError(
                    f"No existe la tabla Gold: {origen}"
                )

            resultado = exportar_tabla(
                spark,
                nombre,
                origen,
            )

            resultados.append(resultado)

        # Guardar manifiesto de exportación.
        manifiesto = {
            "origen": "Delta Lake Gold",
            "formato": "Parquet",
            "tablas": resultados,
        }

        manifiesto_path = EXPORT_PATH / "manifest.json"

        with open(manifiesto_path, "w", encoding="utf-8") as archivo:
            json.dump(
                manifiesto,
                archivo,
                indent=4,
                ensure_ascii=False,
            )

        print("\n" + "=" * 60)
        print("EXPORTACIÓN COMPLETADA")
        print("=" * 60)

        for resultado in resultados:
            print(
                f"{resultado['tabla']}: "
                f"{resultado['filas']:,} filas"
            )

        print(f"\nManifiesto: {manifiesto_path}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
