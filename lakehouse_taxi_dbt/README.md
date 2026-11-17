# Modelado dbt de NYC Taxi

Proyecto dbt Core que transforma las tablas Gold cargadas en Snowflake.

## Flujo

1. `RAW.RAW_DIM_ZONA` y `RAW.RAW_FCT_DEMANDA_DIARIA_ZONA` son las fuentes.
2. Los modelos `stg_*` exponen y documentan las columnas de RAW en `STAGING`.
3. `mrt_demanda_mensual_distrito` agrega viajes e ingresos por mes y distrito en `MARTS`.
4. Las pruebas de `models/*/schema.yml` y `tests/` validan claves, relaciones, métricas y granularidad.

## Comandos

Ejecutar desde esta carpeta con el entorno virtual del repositorio activo:

```powershell
..\.venv-dbt\Scripts\dbt.exe debug
..\.venv-dbt\Scripts\dbt.exe run --select "+mrt_demanda_mensual_distrito"
..\.venv-dbt\Scripts\dbt.exe test --select "stg_dim_zona stg_fct_demanda_diaria_zona mrt_demanda_mensual_distrito"
..\.venv-dbt\Scripts\dbt.exe docs generate
..\.venv-dbt\Scripts\dbt.exe docs serve --port 8081
```

El perfil de conexión se configura localmente en `%USERPROFILE%\.dbt\profiles.yml`.
La contraseña se proporciona mediante `DBT_ENV_SECRET_SNOWFLAKE_PASSWORD` durante
la sesión de PowerShell; no se guarda en este proyecto.

En esta instalación local, para evitar que la función opcional dbt State solicite
autenticación de dbt Platform, establece temporalmente
`$env:DBT_ENGINE_MANAGE_STATE = 'false'` antes de invocar dbt y elimínala al terminar.
