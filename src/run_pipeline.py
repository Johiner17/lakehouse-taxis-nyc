"""Ejecuta el pipeline completo, en orden: descarga -> bronze -> silver -> gold.

Uso: python src/run_pipeline.py [--desde 2023-01] [--hasta 2025-12] [--saltar-descarga] [--forzar]

- Cada paso corre en su propio proceso (una JVM nueva por paso), para que la memoria que
  usa uno no afecte al siguiente.
- Si un paso falla, el pipeline se detiene y termina con código de salida distinto de cero
  (así lo sabrá cualquier orquestador o script que lo llame).
- Es idempotente: repetirlo solo procesa lo que falte. Con --forzar reprocesa todo.
"""
import argparse
import logging
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

SRC = Path(__file__).parent
LOGS = Path("/app/logs")

# Líneas de arranque y de log interno de Spark que no queremos ver en pantalla
# (siguen guardadas completas en el archivo de log).
RUIDO = re.compile(
    r"^(WARNING: Using incubator|:: |Ivy Default|The jars for|io\.delta#|downloading|"
    r"Using Spark's|Setting default|To adjust|\[Stage \d+|"
    r"\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} (WARN|INFO)|"
    r"\s+(confs|found|\d+ artifacts copied|\||-{5,}|org\.|com\.|io\.|:: |\[SUCCESSFUL))"
)


def configurar_logs() -> Path:
    LOGS.mkdir(exist_ok=True)
    archivo = LOGS / f"pipeline_{datetime.now():%Y%m%d_%H%M%S}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(archivo, encoding="utf-8"), logging.StreamHandler(sys.stdout)],
    )
    return archivo


def ejecutar_paso(nombre: str, comando: list[str], log: Path) -> float:
    """Ejecuta un paso. Devuelve los segundos que tardó o termina el programa si falla."""
    logging.info("=== %s: inicio", nombre)
    t0 = time.perf_counter()
    # PYTHONUNBUFFERED: sin esto, Python acumula la salida cuando va por una tubería
    # y verías todo de golpe al final en vez de ir viendo el avance.
    proc = subprocess.Popen(
        comando,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )
    with open(log, "a", encoding="utf-8") as f:
        for linea in proc.stdout:
            f.write(linea)  # el log guarda todo
            if linea.strip() and not RUIDO.match(linea):
                print(linea.rstrip())  # la pantalla solo lo relevante
    codigo = proc.wait()
    segundos = time.perf_counter() - t0
    if codigo != 0:
        logging.error("=== %s: FALLÓ (código %s) tras %.0f s. Log completo: %s", nombre, codigo, segundos, log)
        sys.exit(1)
    logging.info("=== %s: terminado en %.0f s", nombre, segundos)
    return segundos


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--desde", default="2023-01", help="primer mes a descargar (AAAA-MM)")
    p.add_argument("--hasta", default="2025-12", help="último mes a descargar (AAAA-MM)")
    p.add_argument("--saltar-descarga", action="store_true")
    p.add_argument("--forzar", action="store_true", help="reprocesar bronze, silver y gold")
    p.add_argument("--probar-fallo", action="store_true", help="añade un paso que falla (para probar el manejo de errores)")
    args = p.parse_args()

    log = configurar_logs()
    py = sys.executable
    forzar = ["--forzar"] if args.forzar else []

    pasos = []
    if not args.saltar_descarga:
        pasos.append(("descarga", [py, str(SRC / "descargar.py"), "--desde", args.desde, "--hasta", args.hasta]))
    pasos += [
        ("bronze", [py, str(SRC / "bronze_taxis.py"), *forzar]),
        ("silver", [py, str(SRC / "silver_taxis.py"), *forzar]),
        ("gold", [py, str(SRC / "gold_taxis.py"), *forzar]),
    ]
    if args.probar_fallo:
        pasos.append(("prueba_fallo", [py, "-c", "raise SystemExit(3)"]))

    inicio = time.perf_counter()
    tiempos = {nombre: ejecutar_paso(nombre, comando, log) for nombre, comando in pasos}

    logging.info("Pipeline completo en %.0f s", time.perf_counter() - inicio)
    for nombre, seg in tiempos.items():
        logging.info("  %-10s %6.0f s", nombre, seg)
    logging.info("Log completo: %s", log)