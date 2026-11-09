"""Gobierno en Unity Catalog generado desde los contratos de datos.

Qué aplica (todo idempotente: se puede ejecutar cuantas veces se quiera):
1. Documentación: COMMENT en tablas y columnas (bronze_* y silver_* desde el contrato; gold y ctl con texto fijo).
2. Clasificación: TAGS en tablas (capa, dominio, propietario) y en columnas sensibles (clasificacion=pii_alta...).
3. Protección: funciones de enmascaramiento y vistas seguras que muestran el dato real solo a quien
   pertenece al grupo autorizado. Los analistas reciben permiso sobre las vistas, nunca sobre la tabla base.

Las funciones que generan SQL son puras (devuelven texto) para poder probarlas sin Databricks.
"""
from typing import List

from src.common.config import PipelineConfig, get_logger
from src.common.contratos import CONFIDENCIAL, CONTRATOS, PII_ALTA, PII_MEDIA

log = get_logger("gobierno")

GRUPO_SENSIBLES = "grp_datos_sensibles"     # ve PII sin máscara
GRUPO_FINANZAS = "grp_finanzas"             # ve datos financieros confidenciales

DESCRIPCION_TABLAS = {
    "gold_poliza_360": "Una fila por póliza y fecha de corte: estado al corte, primas, recaudo, siniestros y última actividad.",
    "gold_siniestralidad_mensual": "Una fila por mes, producto y sucursal: exposición, prima devengada, costo incurrido y razón de siniestralidad.",
    "gold_discrepancias": "Una fila por registro y discrepancia: pagos sin póliza, siniestros sin póliza válida, hechos fuera de vigencia.",
    "gold_calidad_pipeline": "Historial de reglas de calidad y reconciliaciones por ejecución, dataset y control.",
    "ctl_ejecuciones": "Bitácora: una fila por ejecución y etapa con estado, duración y error.",
    "ctl_cargas": "Una fila por ejecución y tabla con filas leídas, escritas, rechazadas y totales monetarios.",
    "ctl_esquemas": "Validación del contrato de datos por ejecución y entidad, con huella del esquema.",
    "ctl_calidad_resultados": "Resultado de cada regla de calidad por ejecución y dataset.",
}


def _q(texto: str) -> str:
    """Escapa un texto para usarlo como literal SQL entre comillas simples."""
    return texto.replace("\\", "\\\\").replace("'", "\\'")


def sql_documentacion(cfg: PipelineConfig) -> List[str]:
    """COMMENT y TAGS para tablas y columnas."""
    sentencias = []
    for entidad, c in CONTRATOS.items():
        for capa in ("bronze", "silver"):
            t = cfg.table(f"{capa}_{entidad}")
            sentencias.append(f"COMMENT ON TABLE {t} IS '{_q(f'[{capa.upper()}] {c.descripcion}. Dueño: {c.propietario}. Origen: {c.sistema_origen}. SLA: {c.sla}.')}'")
            sentencias.append(f"ALTER TABLE {t} SET TAGS ('capa' = '{capa}', 'dominio' = 'seguros', "
                              f"'propietario' = '{_q(c.propietario)}', 'sistema_origen' = '{c.sistema_origen}')")
            for col in c.columnas:
                sentencias.append(f"ALTER TABLE {t} ALTER COLUMN {col.nombre} COMMENT '{_q(col.descripcion)}'")
                if col.clasificacion != "interno":
                    sentencias.append(f"ALTER TABLE {t} ALTER COLUMN {col.nombre} SET TAGS ('clasificacion' = '{col.clasificacion}')")
    for tabla, desc in DESCRIPCION_TABLAS.items():
        capa = tabla.split("_")[0]
        sentencias.append(f"COMMENT ON TABLE {cfg.table(tabla)} IS '{_q(desc)}'")
        sentencias.append(f"ALTER TABLE {cfg.table(tabla)} SET TAGS ('capa' = '{capa}', 'dominio' = 'seguros')")
    return sentencias


def sql_funciones_mascara(cfg: PipelineConfig) -> List[str]:
    """Funciones reutilizables registradas en Unity Catalog."""
    return [
        f"""CREATE OR REPLACE FUNCTION {cfg.table('fn_mask_ultimos')}(valor STRING, visibles INT)
RETURNS STRING
COMMENT 'Oculta todo excepto los últimos N caracteres. Ej: 10000777 -> ****0777'
RETURN CASE WHEN valor IS NULL THEN NULL
            ELSE concat(repeat('*', greatest(length(valor) - visibles, 0)), right(valor, visibles)) END""",
        f"""CREATE OR REPLACE FUNCTION {cfg.table('fn_mask_correo')}(valor STRING)
RETURNS STRING
COMMENT 'Deja la primera letra y el dominio. Ej: cliente21@ejemplo.com -> c***@ejemplo.com'
RETURN CASE WHEN valor IS NULL THEN NULL
            ELSE concat(left(valor, 1), '***@', split_part(valor, '@', 2)) END""",
    ]


def _expr_protegida(col: str, clasificacion: str, cfg: PipelineConfig) -> str:
    """Expresión SQL que muestra el dato real solo al grupo autorizado."""
    if clasificacion == PII_ALTA:
        grupo = GRUPO_SENSIBLES
        mascara = (f"{cfg.table('fn_mask_correo')}({col})" if col == "correo"
                   else f"{cfg.table('fn_mask_ultimos')}({col}, 4)")
    elif clasificacion == PII_MEDIA:
        grupo, mascara = GRUPO_SENSIBLES, f"CAST(year({col}) AS STRING)"   # fecha -> solo el año
    elif clasificacion == CONFIDENCIAL:
        grupo, mascara = GRUPO_FINANZAS, f"{cfg.table('fn_mask_ultimos')}({col}, 3)"
    else:
        return col
    return f"CASE WHEN is_account_group_member('{grupo}') THEN CAST({col} AS STRING) ELSE {mascara} END AS {col}"


def sql_vista_protegida(cfg: PipelineConfig, entidad: str) -> str:
    """Vista sobre silver_<entidad> que enmascara las columnas sensibles del contrato."""
    sensibles = CONTRATOS[entidad].sensibles()
    columnas = [(_expr_protegida(c.nombre, sensibles[c.nombre], cfg) if c.nombre in sensibles else c.nombre)
                for c in CONTRATOS[entidad].columnas]
    columnas += ["_fecha_corte", "_run_id", "_source_file"]
    cuerpo = ",\n  ".join(columnas)
    return (f"CREATE OR REPLACE VIEW {cfg.table(f'vw_{entidad}_protegido')}\n"
            f"COMMENT 'Vista segura de silver_{entidad}: columnas sensibles enmascaradas salvo para grupos autorizados'\n"
            f"AS SELECT\n  {cuerpo}\nFROM {cfg.table(f'silver_{entidad}')}")


def entidades_con_sensibles() -> List[str]:
    return [e for e, c in CONTRATOS.items() if c.sensibles()]


def aplicar(spark, cfg: PipelineConfig) -> dict:
    """Ejecuta todo. Una sentencia que falla se registra y no detiene las demás."""
    sentencias = (sql_funciones_mascara(cfg)
                  + [sql_vista_protegida(cfg, e) for e in entidades_con_sensibles()]
                  + sql_documentacion(cfg))
    ok, fallidas = 0, []
    for s in sentencias:
        try:
            spark.sql(s)
            ok += 1
        except Exception as e:
            fallidas.append((s.splitlines()[0][:120], str(e).splitlines()[0][:200]))
            log.warning(f"No aplicada: {s.splitlines()[0][:120]} -> {str(e).splitlines()[0][:200]}")
    log.info(f"Gobierno aplicado: {ok} sentencias OK, {len(fallidas)} con error")
    return {"ok": ok, "fallidas": fallidas}