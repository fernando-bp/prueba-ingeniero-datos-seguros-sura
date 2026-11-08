"""Tablas de control: trazabilidad de ejecuciones y reconciliación de cargas.

ctl_ejecuciones -> una fila por (run_id, etapa): estado, duración, parámetros, error.
ctl_cargas      -> una fila por (run_id, tabla): filas leídas/escritas/rechazadas y totales monetarios.

Ambas se crean con DDL explícito (el esquema es un contrato) y se escriben de forma
idempotente: reintentar una tarea con el mismo run_id reemplaza su fila, no la duplica.
"""
import time
from contextlib import contextmanager
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DecimalType, LongType, MapType, StringType, StructField,
                               StructType, TimestampType, DateType, DoubleType)

from src.common.config import PipelineConfig, get_logger

log = get_logger("control")

ESQUEMA_EJECUCIONES = StructType([
    StructField("run_id", StringType(), False),
    StructField("etapa", StringType(), False),          # bronze | silver | gold | calidad
    StructField("fecha_corte", DateType(), False),
    StructField("modo", StringType(), False),
    StructField("env", StringType(), False),
    StructField("estado", StringType(), False),         # EN_CURSO | EXITOSO | FALLIDO
    StructField("inicio_ts", TimestampType(), False),
    StructField("fin_ts", TimestampType(), True),
    StructField("duracion_seg", DoubleType(), True),
    StructField("error_mensaje", StringType(), True),
])

ESQUEMA_CARGAS = StructType([
    StructField("run_id", StringType(), False),
    StructField("fecha_corte", DateType(), False),
    StructField("capa", StringType(), False),           # bronze | silver | gold
    StructField("tabla", StringType(), False),
    StructField("origen", StringType(), True),          # archivo o tabla de entrada
    StructField("filas_leidas", LongType(), True),
    StructField("filas_escritas", LongType(), True),
    StructField("filas_rechazadas", LongType(), True),
    StructField("totales_monetarios", MapType(StringType(), DecimalType(22, 2)), True),
    StructField("registrado_ts", TimestampType(), False),
])

# Columnas monetarias por entidad, usadas para reconciliar totales entre capas.
COLUMNAS_MONETARIAS = {
    "polizas": ["prima"],
    "recaudos": ["valor"],
    "siniestros": ["valor_pagado", "valor_reserva"],
}


def _ahora():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def crear_tablas_control(spark: SparkSession, cfg: PipelineConfig) -> None:
    """Crea las tablas de control si no existen (idempotente)."""
    for nombre, esquema in (("ctl_ejecuciones", ESQUEMA_EJECUCIONES), ("ctl_cargas", ESQUEMA_CARGAS)):
        (spark.createDataFrame([], esquema).write.format("delta")
         .mode("ignore").saveAsTable(cfg.table(nombre)))


def _escribir_fila(spark, cfg, tabla, esquema, fila: dict, condicion: str) -> None:
    """Reemplaza atómicamente las filas que cumplen `condicion` por `fila`."""
    df = spark.createDataFrame([fila], esquema)
    (df.write.format("delta").mode("overwrite")
     .option("replaceWhere", condicion)
     .saveAsTable(cfg.table(tabla)))


def registrar_ejecucion(spark, cfg, etapa, estado, inicio, fin=None, error=None) -> None:
    fila = {
        "run_id": cfg.run_id, "etapa": etapa,
        "fecha_corte": datetime.strptime(cfg.fecha_corte, "%Y-%m-%d").date(),
        "modo": cfg.modo, "env": cfg.env, "estado": estado,
        "inicio_ts": inicio, "fin_ts": fin,
        "duracion_seg": (fin - inicio).total_seconds() if fin else None,
        "error_mensaje": (error or "")[:2000] or None,
    }
    _escribir_fila(spark, cfg, "ctl_ejecuciones", ESQUEMA_EJECUCIONES, fila,
                   f"run_id = '{cfg.run_id}' AND etapa = '{etapa}'")


@contextmanager
def ejecucion(spark: SparkSession, cfg: PipelineConfig, etapa: str):
    """Envuelve una etapa: registra EN_CURSO al inicio y EXITOSO/FALLIDO al final.

    Uso:
        with ejecucion(spark, cfg, "bronze"):
            ingerir_todo(spark, cfg)
    """
    crear_tablas_control(spark, cfg)
    inicio = _ahora()
    registrar_ejecucion(spark, cfg, etapa, "EN_CURSO", inicio)
    log.info(f"[{cfg.run_id}] Etapa {etapa} iniciada (fecha_corte={cfg.fecha_corte}, modo={cfg.modo})")
    t0 = time.time()
    try:
        yield
    except Exception as e:
        registrar_ejecucion(spark, cfg, etapa, "FALLIDO", inicio, _ahora(), f"{type(e).__name__}: {e}")
        log.error(f"[{cfg.run_id}] Etapa {etapa} FALLIDA tras {time.time() - t0:.1f}s: {e}")
        raise
    registrar_ejecucion(spark, cfg, etapa, "EXITOSO", inicio, _ahora())
    log.info(f"[{cfg.run_id}] Etapa {etapa} EXITOSA en {time.time() - t0:.1f}s")


def totales_monetarios(df, entidad: str) -> dict:
    """Suma de cada columna monetaria de la entidad (cast seguro a decimal)."""
    cols = [c for c in COLUMNAS_MONETARIAS.get(entidad, []) if c in df.columns]
    if not cols:
        return {}
    fila = df.agg(*[F.sum(F.col(c).cast(DecimalType(22, 2))).alias(c) for c in cols]).first()
    return {c: fila[c] for c in cols}


def registrar_carga(spark, cfg, capa, tabla, origen=None, filas_leidas=None,
                    filas_escritas=None, filas_rechazadas=None, totales=None) -> None:
    fila = {
        "run_id": cfg.run_id,
        "fecha_corte": datetime.strptime(cfg.fecha_corte, "%Y-%m-%d").date(),
        "capa": capa, "tabla": tabla, "origen": origen,
        "filas_leidas": filas_leidas, "filas_escritas": filas_escritas,
        "filas_rechazadas": filas_rechazadas,
        "totales_monetarios": totales or {}, "registrado_ts": _ahora(),
    }
    _escribir_fila(spark, cfg, "ctl_cargas", ESQUEMA_CARGAS, fila,
                   f"run_id = '{cfg.run_id}' AND tabla = '{tabla}'")
    log.info(f"[{cfg.run_id}] ctl_cargas <- {tabla}: leidas={filas_leidas} escritas={filas_escritas} "
             f"rechazadas={filas_rechazadas} totales={totales}")