"""Postcondiciones de Gold: pruebas de integración que se ejecutan sobre los datos reales
ANTES de publicar. Si alguna falla, la etapa gold falla y no se sobrescriben las tablas.

Cada chequeo devuelve (nombre, ok, detalle). Todas las consultas son agregaciones:
nunca se traen datasets completos al driver.
"""
from typing import List, Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

Resultado = Tuple[str, bool, str]


def _unicidad(df: DataFrame, claves: list, nombre: str) -> Resultado:
    fila = df.agg(F.count(F.lit(1)).alias("n"), F.countDistinct(*claves).alias("u")).first()
    return (nombre, fila.n == fila.u, f"filas={fila.n} claves_unicas={fila.u}")


def chequear_poliza_360(p360: DataFrame, silver_polizas: DataFrame) -> List[Resultado]:
    n_silver = silver_polizas.count()
    agg = p360.agg(
        F.count(F.lit(1)).alias("n"),
        F.sum("prima_emitida").alias("prima_gold"),
        F.sum(F.when(F.col("prima_pendiente") < 0, 1).otherwise(0)).alias("pend_neg"),
        F.sum(F.when(F.col("estado_al_corte").isNull(), 1).otherwise(0)).alias("estado_nulo"),
    ).first()
    prima_silver = silver_polizas.agg(F.sum("prima")).first()[0]
    return [
        _unicidad(p360, ["poliza_id"], "P360_UNA_FILA_POR_POLIZA"),
        ("P360_COBERTURA_TOTAL", agg.n == n_silver, f"gold={agg.n} silver={n_silver}"),
        ("P360_PRIMA_CUADRA_CON_SILVER", agg.prima_gold == prima_silver, f"gold={agg.prima_gold} silver={prima_silver}"),
        ("P360_PENDIENTE_NO_NEGATIVO", agg.pend_neg == 0, f"filas_negativas={agg.pend_neg}"),
        ("P360_ESTADO_SIEMPRE_DEFINIDO", agg.estado_nulo == 0, f"estado_nulo={agg.estado_nulo}"),
    ]


def chequear_siniestralidad(mensual: DataFrame, siniestros_esperados: int) -> List[Resultado]:
    agg = mensual.agg(
        F.sum("num_siniestros").alias("sin"),
        F.sum(F.when(F.col("costo_incurrido") != F.col("valor_pagado") + F.col("valor_reserva"), 1)
              .otherwise(0)).alias("costo_mal"),
        F.sum(F.when(F.col("prima_devengada") < 0, 1).otherwise(0)).alias("dev_neg"),
    ).first()
    return [
        _unicidad(mensual, ["periodo", "producto", "sucursal"], "SIN_UNA_FILA_POR_MES_PRODUCTO_SUCURSAL"),
        ("SIN_CONTEO_CUADRA_CON_SILVER", agg.sin == siniestros_esperados,
         f"gold={agg.sin} esperados={siniestros_esperados}"),
        ("SIN_COSTO_IGUAL_PAGADO_MAS_RESERVA", agg.costo_mal == 0, f"filas_inconsistentes={agg.costo_mal}"),
        ("SIN_DEVENGADA_NO_NEGATIVA", agg.dev_neg == 0, f"filas_negativas={agg.dev_neg}"),
    ]


def chequear_calidad(calidad: DataFrame, fecha_corte: str) -> List[Resultado]:
    """Solo mira el control MÁS RECIENTE de cada (dataset, regla) de la fecha de corte:
    una falla antigua ya corregida no debe bloquear la publicación actual."""
    w = Window.partitionBy("dataset", "regla").orderBy(F.col("control_ts").desc())
    vigentes = (calidad.filter((F.col("tipo_control") == "RECONCILIACION")
                               & (F.col("fecha_corte") == F.lit(fecha_corte).cast("date")))
                .withColumn("_rn", F.row_number().over(w)).filter("_rn = 1"))
    fallas = vigentes.filter(F.col("resultado") == "FALLA").count()
    return [("CAL_RECONCILIACIONES_OK", fallas == 0, f"reconciliaciones_fallidas={fallas}")]