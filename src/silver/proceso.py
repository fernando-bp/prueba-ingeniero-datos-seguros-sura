"""Orquestación de la capa Silver: lee Bronze, transforma, valida, escribe.

Estrategia de escritura (idempotente):
- silver_<entidad>: MERGE por clave de negocio. Solo actualiza si la versión entrante es
  igual o más reciente y el contenido cambió (_row_hash distinto). Reejecutar el mismo
  lote no cambia nada. modo=full reconstruye la tabla completa.
- quarantine_<entidad>: overwrite con replaceWhere sobre _fecha_corte.
- ctl_calidad_resultados: una fila por (run_id, dataset, regla); replaceWhere por run_id+dataset.
Orden de seguridad: primero se guarda la evidencia (calidad y cuarentena), luego se
verifican umbrales y, solo si no hay bloqueo, se publica en Silver.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import PipelineConfig, get_logger
from src.common.control import registrar_carga, totales_monetarios
from src.quality.reglas import verificar_umbrales
from src.silver.entidades import ESPECIFICACIONES, ORDEN_PROCESO
from src.silver.transformacion import transformar

log = get_logger("silver")


def _existe(spark: SparkSession, tabla: str) -> bool:
    return spark.catalog.tableExists(tabla)


def escribir_silver(spark: SparkSession, df: DataFrame, cfg: PipelineConfig, entidad: str) -> None:
    espec = ESPECIFICACIONES[entidad]
    destino = cfg.table(f"silver_{entidad}")
    if cfg.modo == "full" or not _existe(spark, destino):
        log.info(f"[{cfg.run_id}] {destino}: escritura completa (modo={cfg.modo})")
        (df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(destino))
        return
    vista = f"_lote_silver_{entidad}"
    df.createOrReplaceTempView(vista)
    version = espec.orden_version[0]
    spark.sql(f"""
        MERGE INTO {destino} t
        USING {vista} s
        ON t.{espec.clave} = s.{espec.clave}
        WHEN MATCHED AND s._row_hash <> t._row_hash
             AND coalesce(s.{version}, DATE'1900-01-01') >= coalesce(t.{version}, DATE'1900-01-01')
          THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    log.info(f"[{cfg.run_id}] {destino}: MERGE aplicado")


def escribir_reemplazando(df: DataFrame, tabla: str, condicion: str) -> None:
    (df.write.format("delta").mode("overwrite").option("replaceWhere", condicion)
     .option("mergeSchema", True).saveAsTable(tabla))


def procesar_entidad(spark: SparkSession, cfg: PipelineConfig, entidad: str) -> dict:
    espec = ESPECIFICACIONES[entidad]
    origen = cfg.table(f"bronze_{entidad}")
    log.info(f"[{cfg.run_id}] Silver {entidad}: leyendo {origen} fecha_corte={cfg.fecha_corte}")

    bronze = spark.table(origen).filter(F.col("_fecha_corte") == F.lit(cfg.fecha_corte).cast("date"))
    padres = {p: spark.table(cfg.table(f"silver_{p}")) for p in espec.padres}
    aceptados, rechazados, resumen = transformar(bronze, espec, padres, cfg)

    # 1) Evidencia primero: resultados de calidad y cuarentena
    escribir_reemplazando(resumen, cfg.table("ctl_calidad_resultados"),
                          f"run_id = '{cfg.run_id}' AND dataset = 'silver_{entidad}'")
    escribir_reemplazando(rechazados, cfg.table(f"quarantine_{entidad}"),
                          f"_fecha_corte = '{cfg.fecha_corte}'")

    # 2) Reconciliación de conteos: bronze = silver + cuarentena
    n_bronze, n_ok, n_rech = bronze.count(), aceptados.count(), rechazados.count()
    totales = totales_monetarios(aceptados, entidad)
    totales.update({f"{k}_cuarentena": v for k, v in totales_monetarios(rechazados, entidad).items()})
    registrar_carga(spark, cfg, "silver", f"silver_{entidad}", origen=origen, filas_leidas=n_bronze,
                    filas_escritas=n_ok, filas_rechazadas=n_rech, totales=totales)
    if n_bronze != n_ok + n_rech:
        raise ValueError(f"Reconciliación fallida silver_{entidad}: bronze={n_bronze} != {n_ok}+{n_rech}")

    # 3) Umbrales: si una regla BLOQUEO se excede, no se publica
    verificar_umbrales(resumen, f"silver_{entidad}")

    # 4) Publicación
    escribir_silver(spark, aceptados, cfg, entidad)
    log.info(f"[{cfg.run_id}] Silver {entidad}: bronze={n_bronze} silver={n_ok} cuarentena={n_rech}")
    return {"entidad": entidad, "bronze": n_bronze, "silver": n_ok, "cuarentena": n_rech}


def procesar_todo(spark: SparkSession, cfg: PipelineConfig, entidades=None) -> list:
    return [procesar_entidad(spark, cfg, e) for e in (entidades or ORDEN_PROCESO)]