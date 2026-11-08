"""Ingesta Bronze: copia fiel de los CSV + metadatos de ingesta.

Decisiones:
- Todas las columnas se leen como STRING: Bronze no interpreta ni corrige el dato
  (las fechas vienen en 3 formatos; tiparlas aquí perdería el valor original).
- Metadatos: archivo origen, fecha/hora de ingesta, run_id, fecha de corte y hash de la fila.
- Escritura idempotente: overwrite con replaceWhere sobre _fecha_corte.
  Reejecutar la misma fecha reemplaza ese lote; no duplica.
- mergeSchema=true: si llega una columna nueva, se agrega sin romper el pipeline.
- Reconciliación: si filas leídas != filas escritas, la etapa falla.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import PipelineConfig, get_logger
from src.common.control import registrar_carga, totales_monetarios

log = get_logger("bronze")

ENTIDADES = ["clientes", "polizas", "recaudos", "siniestros", "movimientos"]

COLUMNAS_METADATA = ["_source_file", "_ingestion_ts", "_run_id", "_fecha_corte", "_row_hash"]


def leer_csv(spark: SparkSession, ruta: str) -> DataFrame:
    """Lee un CSV de la fuente sin inferir tipos (todo string)."""
    return (
        spark.read.format("csv")
        .option("header", True)
        .option("sep", ";")
        .option("encoding", "UTF-8")
        .option("quote", '"')
        .option("escape", '"')          # el payload JSON trae comillas dobles escapadas como ""
        .option("inferSchema", False)
        .option("mode", "PERMISSIVE")
        .load(ruta)
    )


def limpiar_nombres_columnas(df: DataFrame) -> DataFrame:
    """Quita el BOM y espacios de los encabezados. No toca los valores."""
    for c in df.columns:
        limpio = c.replace("\ufeff", "").strip().lower()
        if limpio != c:
            df = df.withColumnRenamed(c, limpio)
    return df


def agregar_metadata(df: DataFrame, cfg: PipelineConfig) -> DataFrame:
    """Agrega columnas técnicas de trazabilidad."""
    columnas_negocio = [c for c in df.columns if not c.startswith("_")]
    return (
        df.withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_run_id", F.lit(cfg.run_id))
        .withColumn("_fecha_corte", F.lit(cfg.fecha_corte).cast("date"))
        .withColumn(
            "_row_hash",
            F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("<NULL>")) for c in columnas_negocio]), 256),
        )
    )


def escribir_bronze(df: DataFrame, cfg: PipelineConfig, entidad: str) -> None:
    """Escritura idempotente: reemplaza solo el lote de la fecha de corte."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"_fecha_corte = '{cfg.fecha_corte}'")
        .option("mergeSchema", True)
        .saveAsTable(cfg.table(f"bronze_{entidad}"))
    )


def ingerir_entidad(spark: SparkSession, cfg: PipelineConfig, entidad: str) -> dict:
    """Ingesta una entidad y registra en ctl_cargas las métricas para reconciliación."""
    ruta = f"{cfg.raw_path}/{entidad}.csv"
    tabla = f"bronze_{entidad}"
    log.info(f"[{cfg.run_id}] Ingestando {entidad} desde {ruta}")
    try:
        df = agregar_metadata(limpiar_nombres_columnas(leer_csv(spark, ruta)), cfg)
        filas_leidas = df.count()
        escribir_bronze(df, cfg, entidad)
    except Exception as e:
        log.error(f"[{cfg.run_id}] Falló la ingesta de {entidad} ({ruta}): {e}")
        raise
    escrito = spark.table(cfg.table(tabla)).filter(F.col("_fecha_corte") == cfg.fecha_corte)
    filas_escritas = escrito.count()
    totales = totales_monetarios(escrito, entidad)
    registrar_carga(spark, cfg, "bronze", tabla, origen=ruta, filas_leidas=filas_leidas,
                    filas_escritas=filas_escritas, filas_rechazadas=0, totales=totales)
    if filas_leidas != filas_escritas:
        raise ValueError(f"Reconciliación fallida en {tabla}: leidas={filas_leidas} escritas={filas_escritas}")
    return {"entidad": entidad, "tabla": tabla, "filas": filas_escritas, "ruta": ruta}


def ingerir_todo(spark: SparkSession, cfg: PipelineConfig, entidades=None) -> list:
    return [ingerir_entidad(spark, cfg, e) for e in (entidades or ENTIDADES)]