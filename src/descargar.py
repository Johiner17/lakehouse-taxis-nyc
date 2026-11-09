"""Descarga los Parquet mensuales de taxis amarillos de NYC a la zona de aterrizaje.

Uso: python src/descargar.py --desde 2024-01 --hasta 2024-03
Es idempotente: si el archivo ya existe, no lo vuelve a bajar.
"""
import argparse
import os
from pathlib import Path

import requests

BASE = "https://d37ci6vzurychx.cloudfront.net"
URL_MES = BASE + "/trip-data/yellow_tripdata_{mes}.parquet"
URL_ZONAS = BASE + "/misc/taxi_zone_lookup.csv"
DESTINO = Path("/app/data/landing")


def meses(desde: str, hasta: str):
    """Genera 'AAAA-MM' desde..hasta, ambos incluidos."""
    a, m = map(int, desde.split("-"))
    a2, m2 = map(int, hasta.split("-"))
    while (a, m) <= (a2, m2):
        yield f"{a:04d}-{m:02d}"
        a, m = (a + 1, 1) if m == 12 else (a, m + 1)


def bajar(url: str, destino: Path) -> str:
    if destino.exists():
        return "ya existía"
    destino.parent.mkdir(parents=True, exist_ok=True)
    r = requests.get(url, stream=True, timeout=60)
    if r.status_code in (403, 404):  # TLC aún no publica ese mes
        return f"no disponible ({r.status_code})"
    r.raise_for_status()
    # Bajamos a un .part y renombramos al final: si se corta la descarga, nunca queda
    # un archivo a medias con el nombre definitivo (el rename es atómico).
    parcial = destino.with_suffix(destino.suffix + ".part")
    with open(parcial, "wb") as f:
        for trozo in r.iter_content(chunk_size=1024 * 1024):
            f.write(trozo)
    os.replace(parcial, destino)
    return f"descargado ({destino.stat().st_size / 1e6:.0f} MB)"


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--desde", required=True, help="AAAA-MM")
    p.add_argument("--hasta", required=True, help="AAAA-MM")
    args = p.parse_args()

    print("zonas:", bajar(URL_ZONAS, DESTINO / "zonas" / "taxi_zone_lookup.csv"))
    for mes in meses(args.desde, args.hasta):
        print(mes, bajar(URL_MES.format(mes=mes), DESTINO / "yellow" / f"yellow_tripdata_{mes}.parquet"))