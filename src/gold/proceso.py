"""Orquestación de la capa Gold: lee Silver y control, construye productos, valida, publica.

Estrategia de escritura:
- gold_poliza_360, gold_siniestralidad_mensual, gold_discrepancias: overwrite con replaceWhere
  sobre fecha_corte -> una "foto" por fecha de corte; reejecutar reemplaza la misma foto.
- gold_calidad_pipeline: se reconstruye completo desde las tablas de control (historial).
Las postcondiciones se evalúan ANTES de escribir: si fallan, no se publica nada.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.config import PipelineConfig, get_logger
from src.common.control import registrar_carga
from src.gold import postcondiciones as pc
from src.gold.productos import (construir_calidad_pipeline, construir_discrepancias,
                                construir_poliza_360, construir_siniestralidad_mensual)

log = get_logger("gold")

ENTIDADES_SILVER = ["clientes", "polizas", "recaudos", "siniestros", "movimientos"]


def _publicar_foto(df, tabla: str, cfg: PipelineConfig) -> None:
    (df.withColumn("_gold_run_id", F.lit(cfg.run_id)).withColumn("_gold_ts", F.current_timestamp())
     .write.format("delta").mode("overwrite")
     .option("replaceWhere", f"fecha_corte = '{cfg.fecha_corte}'")
     .option("mergeSchema", True).saveAsTable(tabla))


def construir_y_publicar(spark: SparkSession, cfg: PipelineConfig) -> list:
    s = {e: spark.table(cfg.table(f"silver_{e}")) for e in ENTIDADES_SILVER}
    corte = F.lit(cfg.fecha_corte).cast("date")

    p360 = construir_poliza_360(s, cfg.fecha_corte)
    mensual = construir_siniestralidad_mensual(s, cfg.fecha_corte)
    discrep = construir_discrepancias(s, cfg.fecha_corte)
    calidad = construir_calidad_pipeline(spark.table(cfg.table("ctl_calidad_resultados")),
                                         spark.table(cfg.table("ctl_cargas")))

    # Siniestros que deben aparecer en el mensual: con póliza válida y ocurridos hasta el corte
    sin_esperados = (s["siniestros"].filter(F.col("fecha_ocurrencia") <= corte)
                     .join(s["polizas"].select("poliza_id"), "poliza_id").count())
    chequeos = (pc.chequear_poliza_360(p360, s["polizas"])
                + pc.chequear_siniestralidad(mensual, sin_esperados)
                + pc.chequear_calidad(calidad, cfg.fecha_corte))
    for nombre, ok, detalle in chequeos:
        (log.info if ok else log.error)(f"[{cfg.run_id}] Postcondición {nombre}: {'OK' if ok else 'FALLA'} ({detalle})")
    fallidas = [n for n, ok, _ in chequeos if not ok]
    if fallidas:
        raise ValueError(f"Postcondiciones Gold incumplidas, no se publica: {fallidas}")

    resultado = []
    for nombre, df in (("gold_poliza_360", p360), ("gold_siniestralidad_mensual", mensual),
                       ("gold_discrepancias", discrep)):
        _publicar_foto(df, cfg.table(nombre), cfg)
        n = spark.table(cfg.table(nombre)).filter(F.col("fecha_corte") == corte).count()
        registrar_carga(spark, cfg, "gold", nombre, origen="silver_*", filas_leidas=None, filas_escritas=n)
        resultado.append({"tabla": nombre, "filas": n})

    (calidad.write.format("delta").mode("overwrite").option("overwriteSchema", True)
     .saveAsTable(cfg.table("gold_calidad_pipeline")))
    n = spark.table(cfg.table("gold_calidad_pipeline")).count()
    registrar_carga(spark, cfg, "gold", "gold_calidad_pipeline", origen="ctl_*", filas_escritas=n)
    resultado.append({"tabla": "gold_calidad_pipeline", "filas": n})
    return resultado