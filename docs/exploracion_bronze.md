# Exploración del bronze (128.2 millones de filas, 2023-01 a 2025-12)

Antes de escribir las reglas de limpieza de silver se midió cuántas filas tenían anomalías.
Criterio resultante: se rechaza solo lo imposible y lo sospechoso se marca (ver `src/silver_taxis.py`).

| Hallazgo | Filas | % | Decisión en silver |
| --- | --- | --- | --- |
| Recogida fuera del mes del archivo (años 2001, 2026...) | 1,364 | 0.001% | Rechazar |
| Bajada antes de la subida | 6,285 | 0.005% | Rechazar |
| Duración mayor a 24 horas | 794 | 0.001% | Rechazar |
| Pasajeros nulos | 17,012,482 | 13.270% | Marcar |
| Pasajeros 0 o más de 6 | 1,244,421 + 839 | 0.97% | Marcar |
| Duración cero | 569,098 | 0.444% | Marcar |
| Distancia cero o negativa | 2,952,720 | 2.303% | Marcar |
| Distancia mayor a 100 millas | 5,787 | 0.005% | Marcar |
| Tarifa negativa | 3,961,294 | 3.090% | Marcar (junto con total no positivo) |
| Total cero o negativo | 1,977,810 | 1.543% | Marcar |
| Zonas de origen o destino inválidas | 0 | 0% | Sin regla |
| Duplicados exactos (muestra: 2024-01) | 0 | 0% | Se eliminan igualmente |

Evolución de esquema: `cbd_congestion_fee` es nula en 2023 y 2024, y trae valores desde 2025-01.