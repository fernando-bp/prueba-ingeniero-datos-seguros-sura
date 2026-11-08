"""Funciones reutilizables de estandarización para la capa Silver.

Son funciones puras (DataFrame -> DataFrame): no leen ni escriben tablas,
por eso se pueden probar con pytest sin depender de Databricks.
"""
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

# Formatos encontrados en el perfilamiento. Los de barra se asumen día/mes/año
# (convención colombiana; confirmado por valores como 27/12/2023).
FORMATOS_FECHA = ["yyyy-MM-dd", "dd/MM/yyyy", "yyyy/MM/dd"]


def parsear_fecha(col: str) -> Column:
    """Convierte texto a DATE probando cada formato; null si ninguno aplica.

    Usa try_to_timestamp para no fallar con ANSI activo (Serverless).
    """
    intentos = [F.try_to_timestamp(F.trim(F.col(col)), F.lit(fmt)) for fmt in FORMATOS_FECHA]
    return F.coalesce(*intentos).cast("date")


def parsear_decimal(col: str, precision: int = 18, escala: int = 2) -> Column:
    """Convierte texto a DECIMAL; null si no es numérico (try_cast no lanza error)."""
    return F.expr(f"try_cast(trim(`{col}`) AS DECIMAL({precision},{escala}))")


def normalizar_texto(col: str) -> Column:
    """Trim + espacios múltiples a uno. Cadena vacía se vuelve null."""
    limpio = F.regexp_replace(F.trim(F.col(col)), r"\s+", " ")
    return F.when(limpio == "", None).otherwise(limpio)


def estandarizar(df: DataFrame, fechas=(), montos=(), textos=()) -> DataFrame:
    """Aplica tipado y limpieza conservando la columna original en <col>_raw
    cuando el valor no pudo convertirse (para trazabilidad en cuarentena)."""
    for c in textos:
        df = df.withColumn(c, normalizar_texto(c))
    for c in fechas:
        df = df.withColumn(f"{c}_raw", F.col(c)).withColumn(c, parsear_fecha(c))
    for c in montos:
        df = df.withColumn(f"{c}_raw", F.col(c)).withColumn(c, parsear_decimal(c))
    return df


def marcar_duplicados(df: DataFrame, claves: list, orden: list) -> DataFrame:
    """Agrega _dup_tipo: None (fila ganadora), 'DUP_EXACTO' o 'DUP_CLAVE'.

    - DUP_EXACTO: mismo _row_hash que otra fila (copia idéntica).
    - DUP_CLAVE: misma clave de negocio, contenido distinto; gana la versión
      más reciente según `orden`. Desempate final por _row_hash para que el
      resultado sea determinístico aunque la marca temporal sea igual.
    """
    w_hash = Window.partitionBy("_row_hash").orderBy("_ingestion_ts")
    w_clave = Window.partitionBy(*claves).orderBy(*orden, F.col("_row_hash").asc())
    return (
        df.withColumn("_rn_hash", F.row_number().over(w_hash))
        .withColumn("_rn_clave", F.row_number().over(w_clave))
        .withColumn(
            "_dup_tipo",
            F.when(F.col("_rn_hash") > 1, F.lit("DUP_EXACTO"))
            .when(F.col("_rn_clave") > 1, F.lit("DUP_CLAVE")),
        )
        .drop("_rn_hash", "_rn_clave")
    )