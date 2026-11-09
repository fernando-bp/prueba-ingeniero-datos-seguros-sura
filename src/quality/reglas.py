"""Motor de reglas de calidad, cuarentena y reporte por ejecución.

Cada regla define DOS cosas independientes:
- accion (nivel FILA):     CUARENTENA -> la fila sale de Silver y va a quarantine_<entidad>
                           MARCAR     -> la fila se queda en Silver con la falla anotada en _fallas
- severidad (nivel PIPELINE): BLOQUEO     -> si el % de fallas supera el umbral, la etapa FALLA
                              ADVERTENCIA -> si supera el umbral, se registra ALERTA y continúa
                              INFORMATIVA -> solo se reporta
"""
from dataclasses import dataclass
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config import PipelineConfig, get_logger

log = get_logger("calidad")

DIMENSIONES = ("completitud", "unicidad", "validez", "consistencia", "integridad", "oportunidad")
SEVERIDADES = ("BLOQUEO", "ADVERTENCIA", "INFORMATIVA")
ACCIONES = ("CUARENTENA", "MARCAR")


@dataclass(frozen=True)
class Regla:
    codigo: str            # identificador estable, p.ej. POL_PRIMA_NEGATIVA
    dimension: str
    severidad: str
    accion: str
    descripcion: str
    falla_si: str          # expresión SQL que es TRUE cuando la fila INCUMPLE la regla
    umbral_pct: float = 5.0  # % máximo de filas que pueden fallar antes de ALERTA/FALLA

    def __post_init__(self):
        assert self.dimension in DIMENSIONES, f"{self.codigo}: dimensión inválida {self.dimension}"
        assert self.severidad in SEVERIDADES, f"{self.codigo}: severidad inválida {self.severidad}"
        assert self.accion in ACCIONES, f"{self.codigo}: acción inválida {self.accion}"


def aplicar_reglas(df: DataFrame, reglas: List[Regla]) -> DataFrame:
    """Agrega _fallas (array de códigos incumplidos) y _a_cuarentena (bool).

    Una sola pasada sobre los datos, sin collect: todo son expresiones de columna.
    `coalesce(..., false)` hace que una condición que da NULL no cuente como falla.
    """
    fallas = F.array(*[
        F.when(F.coalesce(F.expr(r.falla_si), F.lit(False)), F.lit(r.codigo)) for r in reglas
    ])
    codigos_cuarentena = [r.codigo for r in reglas if r.accion == "CUARENTENA"]
    return (
        df.withColumn("_fallas", F.filter(fallas, lambda x: x.isNotNull()))
        .withColumn(
            "_a_cuarentena",
            F.arrays_overlap(F.col("_fallas"), F.array(*[F.lit(c) for c in codigos_cuarentena]))
            if codigos_cuarentena else F.lit(False),
        )
    )


def separar(df_evaluado: DataFrame):
    """Devuelve (aceptados, rechazados)."""
    aceptados = df_evaluado.filter(~F.col("_a_cuarentena")).drop("_a_cuarentena")
    rechazados = df_evaluado.filter(F.col("_a_cuarentena")).drop("_a_cuarentena")
    return aceptados, rechazados


def resumen_calidad(df_evaluado: DataFrame, reglas: List[Regla], dataset: str,
                    cfg: PipelineConfig, etapa: str = "silver") -> DataFrame:
    """Una fila por regla con evaluados, fallidos, % y resultado (OK / ALERTA / FALLA)."""
    aggs = [F.count(F.lit(1)).alias("_total")] + [
        F.sum(F.array_contains("_fallas", r.codigo).cast("long")).alias(r.codigo) for r in reglas
    ]
    conteos = df_evaluado.agg(*aggs)          # 1 sola fila
    filas = [
        F.struct(
            F.lit(r.codigo).alias("regla"), F.lit(r.dimension).alias("dimension"),
            F.lit(r.severidad).alias("severidad"), F.lit(r.accion).alias("accion"),
            F.lit(r.descripcion).alias("descripcion"), F.lit(r.falla_si).alias("expresion"),
            F.lit(float(r.umbral_pct)).alias("umbral_pct"),
            F.coalesce(F.col(r.codigo), F.lit(0)).alias("fallidos"),
        ) for r in reglas
    ]
    pct = F.round(F.col("r.fallidos") * 100.0 / F.greatest(F.col("_total"), F.lit(1)), 4)
    return (
        conteos.select("_total", F.explode(F.array(*filas)).alias("r"))
        .select(
            F.lit(cfg.run_id).alias("run_id"),
            F.lit(cfg.fecha_corte).cast("date").alias("fecha_corte"),
            F.lit(etapa).alias("etapa"), F.lit(dataset).alias("dataset"),
            "r.regla", "r.dimension", "r.severidad", "r.accion", "r.descripcion", "r.expresion",
            F.col("_total").alias("evaluados"), "r.fallidos",
            pct.alias("pct_fallidos"), "r.umbral_pct",
            F.when(F.col("r.fallidos") == 0, "OK")
            .when(pct <= F.col("r.umbral_pct"), "OK_CON_FALLAS")
            .when(F.col("r.severidad") == "BLOQUEO", "FALLA")
            .when(F.col("r.severidad") == "ADVERTENCIA", "ALERTA")
            .otherwise("INFORMATIVA").alias("resultado"),
            F.current_timestamp().alias("evaluado_ts"),
        )
    )


def preparar_cuarentena(rechazados: DataFrame, cfg: PipelineConfig, pipeline: str) -> DataFrame:
    """Agrega a las filas rechazadas el contexto exigido: regla, fecha, pipeline, run_id."""
    return (
        rechazados.withColumn("_motivos", F.array_join("_fallas", ","))
        .withColumn("_pipeline", F.lit(pipeline))
        .withColumn("_cuarentena_run_id", F.lit(cfg.run_id))
        .withColumn("_cuarentena_ts", F.current_timestamp())
    )


def verificar_umbrales(resumen: DataFrame, dataset: str) -> None:
    """Falla la etapa si alguna regla BLOQUEO superó su umbral. Las ALERTA solo se loguean.

    collect() es seguro: el resumen tiene una fila por regla (decenas como máximo).
    """
    problemas = resumen.filter(F.col("resultado").isin("FALLA", "ALERTA")).collect()
    for p in problemas:
        msg = f"{dataset}.{p.regla}: {p.fallidos}/{p.evaluados} ({p.pct_fallidos}%) > umbral {p.umbral_pct}%"
        if p.resultado == "FALLA":
            log.error("BLOQUEO " + msg)
        else:
            log.warning("ALERTA " + msg)
    bloqueos = [p.regla for p in problemas if p.resultado == "FALLA"]
    if bloqueos:
        raise ValueError(f"Calidad bloqueante incumplida en {dataset}: {bloqueos}")