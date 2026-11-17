
"""
Carga las tablas Gold de Delta Lake en Snowflake.

Origen:
    data/exports/snowflake/

Destino:
    NYC_TAXI_DW.RAW

Proceso:
    1. Conexión con Snowflake.
    2. Upload de Parquet al internal stage.
    3. COPY INTO tablas temporales.
    4. Validación de conteos.
    5. Reemplazo transaccional de las tablas RAW.

La contraseña se solicita por terminal y no se almacena.
"""

import getpass
import json
from pathlib import Path

import snowflake.connector


# ==========================================================
# CONFIGURACIÓN
# ==========================================================

ROOT_DIR = Path(__file__).resolve().parent.parent

EXPORT_DIR = ROOT_DIR / "data" / "exports" / "snowflake"

MANIFEST_PATH = EXPORT_DIR / "manifest.json"

ACCOUNT = "YU12182.us-east-2.aws"
USER = "JOHINER17"

WAREHOUSE = "WH_TAXI_ETL"
DATABASE = "NYC_TAXI_DW"
SCHEMA = "RAW"
ROLE = "TAXI_ETL_ROLE"

STAGE = f"{DATABASE}.{SCHEMA}.GOLD_STAGE"
FILE_FORMAT = f"{DATABASE}.{SCHEMA}.FF_GOLD_PARQUET"


# ==========================================================
# DEFINICIÓN DE LAS TABLAS
# ==========================================================

TABLAS = {
    "dim_zona": {
        "origen": "dim_zona",
        "temporal": "TMP_LOAD_DIM_ZONA",
        "destino": "RAW_DIM_ZONA",
        "columnas": [
            "ZONA_ID",
            "DISTRITO",
            "ZONA",
            "TIPO_SERVICIO",
        ],
        "select": """
            SELECT
                $1:"zona_id"::NUMBER(10,0),
                $1:"distrito"::VARCHAR(100),
                $1:"zona"::VARCHAR(200),
                $1:"tipo_servicio"::VARCHAR(100)
            FROM @{stage}/dim_zona
        """,
    },

    "fct_demanda_diaria_zona": {
        "origen": "fct_demanda_diaria_zona",
        "temporal": "TMP_LOAD_FCT_DEMANDA_DIARIA_ZONA",
        "destino": "RAW_FCT_DEMANDA_DIARIA_ZONA",
        "columnas": [
            "FECHA_RECOGIDA",
            "ZONA_ORIGEN_ID",
            "VIAJES",
            "VIAJES_CON_ALERTAS",
            "VIAJES_LIMPIOS",
            "INGRESOS_TOTAL",
            "PROPINAS_TOTAL",
            "DISTANCIA_TOTAL_MILLAS",
            "DURACION_TOTAL_MIN",
            "VIAJES_PAGO_TARJETA",
        ],
        "select": """
            SELECT
                $1:"fecha_recogida"::DATE,
                $1:"zona_origen_id"::NUMBER(10,0),
                $1:"viajes"::NUMBER(38,0),
                $1:"viajes_con_alertas"::NUMBER(38,0),
                $1:"viajes_limpios"::NUMBER(38,0),
                $1:"ingresos_total"::NUMBER(38,6),
                $1:"propinas_total"::NUMBER(38,6),
                $1:"distancia_total_millas"::NUMBER(38,6),
                $1:"duracion_total_min"::NUMBER(38,6),
                $1:"viajes_pago_tarjeta"::NUMBER(38,0)
            FROM @{stage}/fct_demanda_diaria_zona
        """,
    },
}


# ==========================================================
# UTILIDADES
# ==========================================================

def cargar_manifiesto():
    """Lee los conteos esperados de la exportación Gold."""

    if not MANIFEST_PATH.exists():
        raise FileNotFoundError(
            f"No se encontró el manifiesto: {MANIFEST_PATH}"
        )

    with open(MANIFEST_PATH, "r", encoding="utf-8") as archivo:
        return json.load(archivo)


def ejecutar(cursor, sql):
    """Ejecuta SQL y devuelve todas las filas."""

    cursor.execute(sql)
    return cursor.fetchall()


def contar_filas(cursor, tabla):
    """Obtiene el conteo de una tabla."""

    cursor.execute(f"SELECT COUNT(*) FROM {tabla}")
    return cursor.fetchone()[0]


# ==========================================================
# CONEXIÓN
# ==========================================================

def conectar():
    """Establece la conexión con Snowflake."""

    password = getpass.getpass(
        "Introduce tu contraseña de Snowflake: "
    )

    print("\nConectando con Snowflake...")

    connection = snowflake.connector.connect(
        account=ACCOUNT,
        user=USER,
        password=password,
        authenticator="snowflake",
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA,
        role=ROLE,
        enable_server_session_keep_alive_auto_detection=True,
    )

    print("Conexión establecida correctamente.")

    return connection


# ==========================================================
# SUBIDA DE ARCHIVOS
# ==========================================================

def subir_archivos(cursor):
    """Sube exclusivamente los archivos Parquet al internal stage."""

    print("\n" + "=" * 65)
    print("SUBIDA DE ARCHIVOS PARQUET")
    print("=" * 65)

    for nombre, config in TABLAS.items():

        carpeta = EXPORT_DIR / config["origen"]

        archivos = sorted(carpeta.glob("*.parquet"))

        if not archivos:
            raise FileNotFoundError(
                f"No hay archivos Parquet en {carpeta}"
            )

        print(f"\nTabla: {nombre}")
        print(f"Archivos encontrados: {len(archivos)}")

        for archivo in archivos:

            # Snowflake espera una URI file://; en Windows usamos barras
            # diagonales y comillas para admitir rutas con espacios.
            ruta_local = archivo.resolve().as_posix().replace("'", "''")
            destino_stage = f"@{STAGE}/{config['origen']}"

            sql = (
                f"PUT 'file://{ruta_local}' "
                f"{destino_stage} "
                "AUTO_COMPRESS=FALSE "
                "OVERWRITE=TRUE"
            )

            cursor.execute(sql)

            resultados = cursor.fetchall()

            for resultado in resultados:
                print(
                    f"Archivo local: {resultado[0]} | "
                    f"Archivo en stage: {resultado[1]} | "
                    f"Estado: {resultado[6]} | "
                    f"Mensaje: {resultado[7]}"
                )

    print("\nSubida de archivos completada.")


# ==========================================================
# CARGA EN TABLAS TEMPORALES
# ==========================================================

def cargar_tablas_temporales(cursor):
    """Carga los Parquet en tablas temporales de Snowflake."""

    print("\n" + "=" * 65)
    print("CARGA EN TABLAS TEMPORALES")
    print("=" * 65)

    for nombre, config in TABLAS.items():

        temporal = config["temporal"]

        columnas = ", ".join(config["columnas"])

        print(f"\nPreparando: {temporal}")

        # Crear una tabla temporal con la misma estructura RAW.
        cursor.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE {temporal}
            LIKE {DATABASE}.{SCHEMA}.{config['destino']}
            """
        )

        # Construir la transformación del Parquet.
        consulta = config["select"].format(stage=STAGE)

        sql = f"""
            COPY INTO {temporal} ({columnas})
            FROM (
                {consulta}
            )
            FILE_FORMAT = (
                FORMAT_NAME = '{FILE_FORMAT}'
            )
            ON_ERROR = 'ABORT_STATEMENT'
            FORCE = TRUE
        """

        cursor.execute(sql)

        resultados = cursor.fetchall()

        for resultado in resultados:
            print(
                f"Archivo: {resultado[0]} | "
                f"Estado: {resultado[1]} | "
                f"Filas cargadas: {resultado[3]} | "
                f"Errores: {resultado[5]}"
            )

        filas = contar_filas(cursor, temporal)

        print(f"Total en tabla temporal: {filas:,}")


# ==========================================================
# VALIDACIÓN
# ==========================================================

def validar_tablas(cursor, manifiesto):
    """Compara las filas temporales con el manifiesto de Gold."""

    print("\n" + "=" * 65)
    print("VALIDACIÓN DE CONTEOS")
    print("=" * 65)

    esperados = {
        tabla["tabla"]: tabla["filas"]
        for tabla in manifiesto["tablas"]
    }

    for nombre, config in TABLAS.items():

        esperado = esperados[nombre]
        obtenido = contar_filas(cursor, config["temporal"])

        print(f"\nTabla: {nombre}")
        print(f"Esperado: {esperado:,}")
        print(f"Obtenido: {obtenido:,}")

        if esperado != obtenido:
            raise RuntimeError(
                f"Conteo incorrecto en {nombre}: "
                f"esperado={esperado}, obtenido={obtenido}"
            )

        print("Validación: OK")


# ==========================================================
# PUBLICACIÓN TRANSACCIONAL
# ==========================================================

def publicar_tablas(cursor, manifiesto):
    """
    Reemplaza ambas tablas RAW dentro de una transacción.

    INGESTED_AT se omite intencionalmente para que Snowflake
    aplique CURRENT_TIMESTAMP() mediante su DEFAULT.
    """

    print("\n" + "=" * 65)
    print("PUBLICACIÓN EN TABLAS RAW")
    print("=" * 65)

    cursor.execute("BEGIN")

    try:

        for nombre, config in TABLAS.items():

            destino = f"{DATABASE}.{SCHEMA}.{config['destino']}"
            temporal = config["temporal"]

            columnas = ", ".join(config["columnas"])

            print(f"\nActualizando: {destino}")

            # Eliminar los datos anteriores.
            cursor.execute(f"DELETE FROM {destino}")

            # Insertar los datos nuevos.
            cursor.execute(
                f"""
                INSERT INTO {destino} ({columnas})
                SELECT {columnas}
                FROM {temporal}
                """
            )

            print("Inserción completada.")

        # Validar dentro de la transacción: si un conteo no coincide,
        # la excepción provoca el ROLLBACK y no se publica el resultado.
        validar_resultado_final(cursor, manifiesto)

        # Confirmar ambas tablas juntas.
        cursor.execute("COMMIT")

        print("\nTransacción confirmada.")

    except Exception:
        cursor.execute("ROLLBACK")
        print("\nError durante la publicación. Rollback ejecutado.")
        raise


# ==========================================================
# VALIDACIÓN FINAL
# ==========================================================

def validar_resultado_final(cursor, manifiesto):
    """Comprueba que las tablas RAW contienen las filas esperadas."""

    print("\n" + "=" * 65)
    print("VALIDACIÓN FINAL EN SNOWFLAKE")
    print("=" * 65)

    esperados = {
        tabla["tabla"]: tabla["filas"]
        for tabla in manifiesto["tablas"]
    }

    for nombre, config in TABLAS.items():

        destino = f"{DATABASE}.{SCHEMA}.{config['destino']}"

        esperado = esperados[nombre]
        obtenido = contar_filas(cursor, destino)

        print(f"\n{destino}")
        print(f"Esperado: {esperado:,}")
        print(f"Encontrado: {obtenido:,}")

        if esperado != obtenido:
            raise RuntimeError(
                f"El conteo final de {destino} no coincide."
            )

        print("Resultado: OK")


# ==========================================================
# EJECUCIÓN PRINCIPAL
# ==========================================================

def main():

    manifiesto = cargar_manifiesto()

    connection = None
    cursor = None

    try:

        connection = conectar()
        cursor = connection.cursor()

        # ==================================================
        # FASE A: CARGA Y VALIDACIÓN
        # ==================================================

        subir_archivos(cursor)

        cargar_tablas_temporales(cursor)

        validar_tablas(cursor, manifiesto)

        # ==================================================
        # FASE B: PUBLICACIÓN
        # ==================================================

        publicar_tablas(cursor, manifiesto)

        print("\n" + "=" * 65)
        print("CARGA GOLD -> SNOWFLAKE COMPLETADA")
        print("=" * 65)

    finally:

        try:
            if cursor is not None:
                cursor.close()
        finally:
            if connection is not None:
                connection.close()


if __name__ == "__main__":
    main()
