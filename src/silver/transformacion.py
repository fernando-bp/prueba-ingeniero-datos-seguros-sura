"""Transformación Silver pura: Bronze -> (aceptados, rechazados, resumen de calidad).

No escribe nada; la escritura vive en src/silver/proceso.py. Así se puede probar completa.
"""
from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config import PipelineConfig
from src.quality.reglas import aplicar_reglas, preparar_cuarentena, resumen_calidad, separar
from src.silver.entidades import EspecEntidad
from src.silver.estandarizacion import estandarizar, marcar_duplicados


def transformar(df_bronze: DataFrame, espec: EspecEntidad, padres: Dict[str, DataFrame], cfg: PipelineConfig):
    df = estandarizar(df_bronze, fechas=espec.fechas, montos=espec.montos, textos=espec.textos)
    for c in espec.enteros:
        df = df.withColumn(c, F.expr(f"try_cast(`{c}` AS INT)"))
    orden = [F.col(c).desc_nulls_last() for c in espec.orden_version] + [F.col("_ingestion_ts").desc()]
    df = marcar_duplicados(df, [espec.clave], orden)
    if espec.enriquecer:
        df = espec.enriquecer(df, padres)

    evaluado = aplicar_reglas(df, espec.reglas)
    resumen = resumen_calidad(evaluado, espec.reglas, f"silver_{espec.nombre}", cfg)
    aceptados, rechazados = separar(evaluado)

    columnas_raw = [c for c in aceptados.columns if c.endswith("_raw")]
    aceptados = (aceptados
                 .drop(*columnas_raw, "_dup_tipo", *espec.columnas_tecnicas_silver)
                 .withColumn("_silver_run_id", F.lit(cfg.run_id))
                 .withColumn("_silver_ts", F.current_timestamp()))
    rechazados = preparar_cuarentena(rechazados, cfg, pipeline=f"silver_{espec.nombre}")
    return aceptados, rechazados, resumen