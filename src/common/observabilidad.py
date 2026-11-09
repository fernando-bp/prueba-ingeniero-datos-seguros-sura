"""Vistas de observabilidad sobre las tablas de control (alimentan el tablero).

Cubren lo que pide el numeral 3.7: estado, duración, volumen de entrada y salida, aceptados
y rechazados, frescura, duplicados, evolución del esquema, costo estimado y cumplimiento del SLA.

SLA acordado: Gold publicado antes de las 08:00 (hora Colombia) del día de ejecución.
Costo: estimación por duración (Free Edition no expone la tabla de facturación). En un
workspace corporativo se reemplaza por system.billing.usage.
"""
from typing import List

from src.common.config import PipelineConfig, get_logger

log = get_logger("observabilidad")

HORA_LIMITE_SLA = 8            # 08:00 hora Colombia
TARIFA_USD_POR_HORA = 0.70     # supuesto para estimar costo serverless; ajustar con la tarifa real


def sql_vistas(cfg: PipelineConfig) -> List[str]:
    t = cfg.table
    return [
        # 1. Una fila por ejecución (run_id) con el estado de cada etapa, duración total y SLA
        f"""CREATE OR REPLACE VIEW {t('vw_obs_ejecuciones')}
COMMENT 'Una fila por ejecución: estado por etapa, duración, costo estimado y cumplimiento del SLA'
AS SELECT
  run_id,
  max(fecha_corte) AS fecha_corte,
  min(inicio_ts) AS inicio_ts,
  max(fin_ts) AS fin_ts,
  round((unix_timestamp(max(fin_ts)) - unix_timestamp(min(inicio_ts))) / 60.0, 2) AS duracion_min,
  max_by(estado, inicio_ts) AS estado_ultima_etapa,
  CASE WHEN count_if(estado = 'FALLIDO') > 0 THEN 'FALLIDO'
       WHEN count_if(estado = 'EN_CURSO') > 0 THEN 'EN_CURSO'
       WHEN count_if(etapa = 'gold' AND estado = 'EXITOSO') > 0 THEN 'EXITOSO'
       ELSE 'PARCIAL' END AS estado_ejecucion,
  max(CASE WHEN etapa = 'bronze' THEN estado END) AS estado_bronze,
  max(CASE WHEN etapa = 'silver' THEN estado END) AS estado_silver,
  max(CASE WHEN etapa = 'gold' THEN estado END) AS estado_gold,
  max_by(error_mensaje, inicio_ts) FILTER (WHERE estado = 'FALLIDO') AS error,
  round(sum(duracion_seg) / 3600.0 * {TARIFA_USD_POR_HORA}, 4) AS costo_estimado_usd,
  from_utc_timestamp(max(fin_ts) FILTER (WHERE etapa = 'gold' AND estado = 'EXITOSO'), 'America/Bogota') AS gold_publicado_col,
  CASE WHEN max(fin_ts) FILTER (WHERE etapa = 'gold' AND estado = 'EXITOSO') IS NULL THEN 'NO_PUBLICADO'
       WHEN hour(from_utc_timestamp(max(fin_ts) FILTER (WHERE etapa = 'gold' AND estado = 'EXITOSO'), 'America/Bogota')) < {HORA_LIMITE_SLA}
       THEN 'CUMPLE' ELSE 'FUERA_DE_HORA' END AS sla
FROM {t('ctl_ejecuciones')}
GROUP BY run_id""",
        # 2. Volumen por tabla y ejecución: entrada, salida, rechazados, % rechazo
        f"""CREATE OR REPLACE VIEW {t('vw_obs_volumenes')}
COMMENT 'Volumen de entrada, salida y rechazados por tabla y ejecución'
AS SELECT run_id, fecha_corte, capa, tabla, filas_leidas AS entrada, filas_escritas AS aceptados,
  coalesce(filas_rechazadas, 0) AS rechazados,
  round(100.0 * coalesce(filas_rechazadas, 0) / nullif(filas_leidas, 0), 3) AS pct_rechazo,
  registrado_ts
FROM {t('ctl_cargas')}""",
        # 3. Frescura: horas desde la última publicación exitosa de Gold
        f"""CREATE OR REPLACE VIEW {t('vw_obs_frescura')}
COMMENT 'Frescura: última fecha de corte publicada en Gold y horas transcurridas'
AS SELECT
  max(fecha_corte) AS ultima_fecha_corte,
  from_utc_timestamp(max(fin_ts), 'America/Bogota') AS ultima_publicacion_col,
  round((unix_timestamp(current_timestamp()) - unix_timestamp(max(fin_ts))) / 3600.0, 1) AS horas_desde_publicacion,
  CASE WHEN max(fin_ts) >= current_timestamp() - INTERVAL 35 DAYS THEN 'AL_DIA' ELSE 'DESACTUALIZADO' END AS estado_frescura
FROM {t('ctl_ejecuciones')}
WHERE etapa = 'gold' AND estado = 'EXITOSO'""",
        # 4. Duplicados detectados por ejecución y dataset
        f"""CREATE OR REPLACE VIEW {t('vw_obs_duplicados')}
COMMENT 'Duplicados exactos y por clave detectados en Silver por ejecución'
AS SELECT run_id, fecha_corte, dataset,
  sum(CASE WHEN regla LIKE '%DUP_EXACTO' THEN fallidos ELSE 0 END) AS duplicados_exactos,
  sum(CASE WHEN regla LIKE '%DUP_CLAVE' THEN fallidos ELSE 0 END) AS duplicados_clave,
  max(evaluado_ts) AS evaluado_ts
FROM {t('ctl_calidad_resultados')}
GROUP BY run_id, fecha_corte, dataset""",
        # 5. Evolución del esquema: cambia la huella respecto a la ejecución anterior
        f"""CREATE OR REPLACE VIEW {t('vw_obs_esquemas')}
COMMENT 'Evolución del esquema por entidad: estado del contrato y si la huella cambió'
AS SELECT entidad, run_id, fecha_corte, estado, huella_esquema, columnas_nuevas, faltantes_obligatorias, validado_ts,
  CASE WHEN lag(huella_esquema) OVER (PARTITION BY entidad ORDER BY validado_ts) IS NULL THEN 'PRIMERA_CARGA'
       WHEN lag(huella_esquema) OVER (PARTITION BY entidad ORDER BY validado_ts) <> huella_esquema THEN 'CAMBIO'
       ELSE 'SIN_CAMBIO' END AS evolucion
FROM {t('ctl_esquemas')}""",
        # 6. Calidad: resultado de reglas por ejecución
        f"""CREATE OR REPLACE VIEW {t('vw_obs_calidad')}
COMMENT 'Resumen de calidad por ejecución: reglas OK, con fallas, alertas y bloqueos'
AS SELECT run_id, fecha_corte, dataset,
  count(*) AS reglas,
  count_if(resultado = 'OK') AS ok,
  count_if(resultado = 'OK_CON_FALLAS') AS ok_con_fallas,
  count_if(resultado = 'ALERTA') AS alertas,
  count_if(resultado = 'FALLA') AS bloqueos,
  sum(fallidos) AS registros_con_fallas
FROM {t('ctl_calidad_resultados')}
GROUP BY run_id, fecha_corte, dataset""",
    ]


def aplicar(spark, cfg: PipelineConfig) -> int:
    vistas = sql_vistas(cfg)
    for s in vistas:
        spark.sql(s)
        log.info(f"Vista creada: {s.split()[4]}")
    return len(vistas)