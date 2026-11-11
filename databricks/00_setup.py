# Databricks notebook source
# En Unity Catalog los nombres tienen tres niveles: catálogo.esquema.tabla.
# Usamos el catálogo "workspace" y un esquema por capa del medallion.
for esquema in ["landing", "bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS workspace.{esquema}")

# Volúmenes: carpetas gestionadas para guardar archivos (no tablas).
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.landing.yellow")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.landing.zonas")

display(spark.sql("SHOW SCHEMAS IN workspace"))