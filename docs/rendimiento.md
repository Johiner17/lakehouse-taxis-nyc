# Laboratorio de rendimiento

Entorno: Spark 4.1.1, Delta 4.1.0, modo local con 2 hilos, driver de 2g. Mediana de 3 ejecuciones.

| Experimento | Variante | Segundos | Detalle |
| --- | --- | --- | --- |
| A | fragmentado | 6.8 | 400 archivos, 136 MB (rango 6.4-10.6 s) |
| A | tras OPTIMIZE | 0.8 | 1 archivos, 115 MB (rango 0.8-2.5 s) |
| B | escaneo completo (filtro por hora) | 12.2 | (rango 12.2-26.4 s) |
| B | filtro con columna de partición | 4.9 | (rango 4.8-5.1 s) |
| B | filtro solo por fecha | 4.8 | (rango 4.7-4.9 s) |
| C | sin ordenar | 2.1 | 8 archivos, 380 MB (rango 1.6-6.7 s) |
| C | ZORDER BY pulocationid | 1.3 | 22 archivos, 375 MB (rango 1.1-4.0 s) |
| D | AQE apagado, 200 particiones | 3.0 | (rango 2.3-7.7 s) |
| D | AQE apagado, 8 particiones | 1.8 | (rango 1.7-1.9 s) |
| D | AQE encendido, 200 particiones | 1.9 | (rango 1.5-3.2 s) |

## Conclusiones

**Qué se midió y cómo.** Cuatro experimentos sobre la capa silver (128 millones de filas, 36 meses) o sobre copias de 1 a 3 meses. Cada consulta se ejecutó 3 veces con el formato `noop` (ejecuta el plan sin escribir) y se reporta la mediana con el rango mínimo-máximo. Entorno: Spark en modo local con 2 hilos y 2 GB de memoria, en una laptop de 8 GB. Las ejecuciones varían bastante entre corridas, por eso solo se interpretan diferencias que no se solapan entre rangos.

1. **Archivos pequeños (A): la mayor palanca.** La misma consulta pasó de 6.8 s con 400 archivos a 0.8 s con 1 archivo tras `OPTIMIZE` (8.5×). El costo fijo por archivo (planificación de tareas, apertura y lectura del pie de página) supera al trabajo útil cuando cada archivo tiene ~7 mil filas. El precio es el propio `OPTIMIZE` (44 s para un mes), que reescribe los datos y deja los archivos viejos hasta ejecutar `VACUUM`. Este problema aparece sobre todo con cargas pequeñas y frecuentes, y no con la carga mensual de este proyecto.
2. **Poda de datos (B): leer solo lo necesario cuesta menos de la mitad.** Un filtro que permite descartar archivos tardó 4.8-4.9 s frente a 12.2 s del escaneo completo (2.5×). Filtrar por la columna de partición o solo por la fecha dio el mismo tiempo. Mi hipótesis, que **no verifiqué** con el plan de ejecución, es que Delta ya salta por estadísticas los archivos de otros meses, porque `fecha_recogida` está ligada al mes. Además, el ahorro está lejos de 36× (1 mes de 36): parte del tiempo es costo fijo (leer el registro de transacciones y planificar).
3. **Z-order (C): sin evidencia suficiente.** La consulta por zona bajó de 2.1 s a 1.3 s y en una corrida anterior de 1.7 s a 1.3 s, pero los rangos se solapan. Con ~9.5 millones de filas el margen es pequeño y el ruido de la máquina es del mismo orden. No se concluye que Z-order ayude a esta escala; haría falta una tabla más grande.
4. **Particiones de shuffle (D): hoy casi no hace falta tocarlas.** Con la ejecución adaptativa (AQE) apagada, las 200 particiones por defecto fueron ~1.7× más lentas que 8 (3.0 s vs 1.8 s). Con AQE encendido, que es el valor por defecto de Spark 4, 200 particiones rindió como 8 (1.9 s), porque AQE fusiona solo las particiones pequeñas.

**Límites del laboratorio.** Máquina de 8 GB y 2 hilos, tablas de hasta 128 millones de filas, caché del sistema de archivos activa tras la primera ejecución, y una variabilidad entre corridas de hasta 2×. Las conclusiones 1, 2 y 4 se sostienen en estas condiciones; la 3 no.
