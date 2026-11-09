"""Especificación Silver por entidad: claves, tipos, deduplicación, enriquecimiento y reglas.

Toda la lógica de negocio de Silver queda declarada aquí, en un solo lugar legible.
Las expresiones de las reglas usan `_fecha_corte` (columna de Bronze), por lo que no
dependen de fechas escritas a mano.
"""
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType

from src.quality.reglas import Regla

# Días a partir de los cuales un movimiento se considera "tardío".
DIAS_MOVIMIENTO_TARDIO = 30


def _reglas_base(prefijo: str, clave: str) -> List[Regla]:
    """Reglas comunes a todas las entidades: clave obligatoria y duplicados."""
    return [
        Regla(f"{prefijo}_ID_NULO", "completitud", "BLOQUEO", "CUARENTENA",
              f"{clave} es obligatorio", f"{clave} IS NULL", 1.0),
        Regla(f"{prefijo}_DUP_EXACTO", "unicidad", "INFORMATIVA", "CUARENTENA",
              "Fila idéntica a otra del mismo lote (se conserva una)", "_dup_tipo = 'DUP_EXACTO'"),
        Regla(f"{prefijo}_DUP_CLAVE", "unicidad", "ADVERTENCIA", "CUARENTENA",
              "Versión superada de la misma clave (se conserva la más reciente)", "_dup_tipo = 'DUP_CLAVE'"),
    ]


def _fecha_ilegible(col: str) -> str:
    """Había un valor pero no se pudo interpretar como fecha."""
    return f"({col} IS NULL AND {col}_raw IS NOT NULL)"


# ----------------------------------------------------------------------------- enriquecimientos
def _enriquecer_clientes(df: DataFrame, padres: Dict[str, DataFrame]) -> DataFrame:
    w = Window.partitionBy("tipo_documento", "numero_documento")
    return df.withColumn(
        "_doc_repetido",
        F.col("numero_documento").isNotNull()
        & (F.size(F.collect_set("cliente_id").over(w)) > 1),
    )


def _enriquecer_polizas(df: DataFrame, padres: Dict[str, DataFrame]) -> DataFrame:
    clientes = padres["clientes"].select("cliente_id").distinct().withColumn("_existe_cliente", F.lit(True))
    return (df.join(clientes, "cliente_id", "left")
            .withColumn("_existe_cliente", F.coalesce("_existe_cliente", F.lit(False))))


def _con_vigencia(df: DataFrame, padres: Dict[str, DataFrame]) -> DataFrame:
    """Agrega existencia de la póliza y su vigencia (para validar fechas del hecho)."""
    pol = padres["polizas"].select(
        "poliza_id",
        F.col("fecha_inicio_vigencia").alias("_pol_inicio"),
        F.col("fecha_fin_vigencia").alias("_pol_fin"),
        F.lit(True).alias("_existe_poliza"),
    )
    return (df.join(pol, "poliza_id", "left")
            .withColumn("_existe_poliza", F.coalesce("_existe_poliza", F.lit(False))))


def _enriquecer_movimientos(df: DataFrame, padres: Dict[str, DataFrame]) -> DataFrame:
    payload = F.from_json("payload", MapType(StringType(), StringType()))
    df = (df.withColumn("_payload_map", payload)
          .withColumn("estado_origen", F.col("_payload_map")["estado_origen"])
          .withColumn("estado_destino", F.col("_payload_map")["estado_destino"])
          .withColumn("usuario_proceso", F.col("_payload_map")["usuario_proceso"])
          .withColumn("_payload_valido", F.col("_payload_map").isNotNull())
          .drop("_payload_map"))
    ids = (
        padres["clientes"].select(F.lit("CLIENTE").alias("entidad"), F.col("cliente_id").alias("id_entidad"))
        .unionByName(padres["polizas"].select(F.lit("POLIZA").alias("entidad"), F.col("poliza_id").alias("id_entidad")))
        .unionByName(padres["recaudos"].select(F.lit("RECAUDO").alias("entidad"), F.col("recaudo_id").alias("id_entidad")))
        .unionByName(padres["siniestros"].select(F.lit("SINIESTRO").alias("entidad"), F.col("siniestro_id").alias("id_entidad")))
        .distinct().withColumn("_existe_entidad", F.lit(True))
    )
    return (df.join(ids, ["entidad", "id_entidad"], "left")
            .withColumn("_existe_entidad", F.coalesce("_existe_entidad", F.lit(False))))


# ----------------------------------------------------------------------------- especificación
@dataclass(frozen=True)
class EspecEntidad:
    nombre: str
    clave: str
    fechas: List[str]
    montos: List[str]
    textos: List[str]
    orden_version: List[str]            # columnas que definen la versión más reciente (desc)
    reglas: List[Regla]
    padres: List[str] = field(default_factory=list)
    enriquecer: Optional[Callable[[DataFrame, Dict[str, DataFrame]], DataFrame]] = None
    enteros: List[str] = field(default_factory=list)
    columnas_tecnicas_silver: List[str] = field(default_factory=list)  # derivadas que se descartan al escribir


ESPECIFICACIONES: Dict[str, EspecEntidad] = {
    "clientes": EspecEntidad(
        nombre="clientes", clave="cliente_id",
        fechas=["fecha_nacimiento", "fecha_actualizacion"], montos=[],
        textos=["cliente_id", "tipo_documento", "numero_documento", "ciudad", "segmento", "correo", "telefono"],
        orden_version=["fecha_actualizacion"],
        enriquecer=_enriquecer_clientes,
        reglas=_reglas_base("CLI", "cliente_id") + [
            Regla("CLI_DOC_NULO", "completitud", "ADVERTENCIA", "MARCAR",
                  "Cliente sin número de documento", "numero_documento IS NULL", 5.0),
            Regla("CLI_DOC_REPETIDO", "unicidad", "ADVERTENCIA", "MARCAR",
                  "Mismo documento asociado a varios cliente_id (posible doble registro)", "_doc_repetido", 5.0),
            Regla("CLI_FECHA_NAC_INVALIDA", "validez", "ADVERTENCIA", "MARCAR",
                  "Fecha de nacimiento ilegible o futura",
                  f"{_fecha_ilegible('fecha_nacimiento')} OR fecha_nacimiento > _fecha_corte", 5.0),
        ],
        columnas_tecnicas_silver=["_doc_repetido"],
    ),
    "polizas": EspecEntidad(
        nombre="polizas", clave="poliza_id",
        fechas=["fecha_inicio_vigencia", "fecha_fin_vigencia", "fecha_actualizacion"], montos=["prima"],
        textos=["poliza_id", "cliente_id", "producto", "sucursal", "estado", "moneda", "canal"],
        orden_version=["fecha_actualizacion"], padres=["clientes"],
        enriquecer=_enriquecer_polizas,
        reglas=_reglas_base("POL", "poliza_id") + [
            Regla("POL_VIGENCIA_NULA", "completitud", "BLOQUEO", "CUARENTENA",
                  "Inicio y fin de vigencia son obligatorios",
                  "fecha_inicio_vigencia IS NULL OR fecha_fin_vigencia IS NULL", 1.0),
            Regla("POL_VIGENCIA_INVERTIDA", "consistencia", "BLOQUEO", "CUARENTENA",
                  "Fin de vigencia anterior al inicio", "fecha_fin_vigencia < fecha_inicio_vigencia", 1.0),
            Regla("POL_PRIMA_INVALIDA", "validez", "BLOQUEO", "CUARENTENA",
                  "Prima nula, no numérica o negativa", "prima IS NULL OR prima < 0", 1.0),
            Regla("POL_PRODUCTO_NULO", "completitud", "ADVERTENCIA", "MARCAR",
                  "Póliza sin producto (se reporta como 'No informado')", "producto IS NULL", 1.0),
            Regla("POL_SIN_CLIENTE", "integridad", "ADVERTENCIA", "MARCAR",
                  "cliente_id no existe en silver_clientes", "NOT _existe_cliente", 2.0),
            Regla("POL_ESTADO_INCONSISTENTE", "consistencia", "INFORMATIVA", "MARCAR",
                  "Estado Vigente con vigencia terminada, o Vencida con vigencia activa al corte",
                  "(estado = 'Vigente' AND fecha_fin_vigencia < _fecha_corte) OR "
                  "(estado = 'Vencida' AND fecha_fin_vigencia >= _fecha_corte)"),
        ],
        columnas_tecnicas_silver=["_existe_cliente"],
    ),
    "recaudos": EspecEntidad(
        nombre="recaudos", clave="recaudo_id",
        fechas=["fecha_pago", "fecha_ingesta"], montos=["valor"],
        textos=["recaudo_id", "poliza_id", "medio_pago", "estado_transaccion", "referencia_bancaria"],
        orden_version=["fecha_ingesta"], padres=["polizas"],
        enriquecer=_con_vigencia,
        reglas=_reglas_base("REC", "recaudo_id") + [
            Regla("REC_FECHA_PAGO_NULA", "completitud", "BLOQUEO", "CUARENTENA",
                  "Fecha de pago ausente o ilegible", "fecha_pago IS NULL", 1.0),
            Regla("REC_VALOR_INVALIDO", "validez", "BLOQUEO", "CUARENTENA",
                  "Valor nulo, o negativo en una transacción Aplicada (los reversos sí pueden ser negativos)",
                  "valor IS NULL OR (valor < 0 AND estado_transaccion = 'Aplicado')", 1.0),
            Regla("REC_SIN_POLIZA", "integridad", "ADVERTENCIA", "MARCAR",
                  "Pago sin póliza correspondiente", "NOT _existe_poliza", 2.0),
            Regla("REC_FUERA_VIGENCIA", "consistencia", "INFORMATIVA", "MARCAR",
                  "Pago fuera de la vigencia de la póliza",
                  "_existe_poliza AND (fecha_pago < _pol_inicio OR fecha_pago > _pol_fin)"),
            Regla("REC_POSTERIOR_CORTE", "oportunidad", "INFORMATIVA", "MARCAR",
                  "Pago con fecha posterior a la fecha de corte", "fecha_pago > _fecha_corte"),
        ],
        columnas_tecnicas_silver=["_pol_inicio", "_pol_fin", "_existe_poliza"],
    ),
    "siniestros": EspecEntidad(
        nombre="siniestros", clave="siniestro_id",
        fechas=["fecha_ocurrencia", "fecha_aviso", "fecha_actualizacion"],
        montos=["valor_pagado", "valor_reserva"],
        textos=["siniestro_id", "poliza_id", "tipo_siniestro", "estado", "ciudad_ocurrencia"],
        orden_version=["fecha_actualizacion"], padres=["polizas"],
        enriquecer=_con_vigencia,
        reglas=_reglas_base("SIN", "siniestro_id") + [
            Regla("SIN_FECHA_OCURRENCIA_NULA", "completitud", "BLOQUEO", "CUARENTENA",
                  "Fecha de ocurrencia ausente o ilegible", "fecha_ocurrencia IS NULL", 1.0),
            Regla("SIN_MONTOS_INVALIDOS", "validez", "BLOQUEO", "CUARENTENA",
                  "Pagado o reserva no numéricos, o reserva negativa",
                  "valor_pagado IS NULL OR valor_reserva IS NULL OR valor_reserva < 0", 1.0),
            Regla("SIN_ESTADO_NULO", "completitud", "ADVERTENCIA", "MARCAR",
                  "Siniestro sin estado", "estado IS NULL", 1.0),
            Regla("SIN_PAGO_NEGATIVO", "validez", "ADVERTENCIA", "MARCAR",
                  "Pago negativo: posible recobro/salvamento, requiere validación", "valor_pagado < 0", 1.0),
            Regla("SIN_AVISO_ANTES_OCURRENCIA", "consistencia", "ADVERTENCIA", "MARCAR",
                  "Fecha de aviso anterior a la ocurrencia", "fecha_aviso < fecha_ocurrencia", 5.0),
            Regla("SIN_SIN_POLIZA", "integridad", "ADVERTENCIA", "MARCAR",
                  "Siniestro sin póliza válida", "NOT _existe_poliza", 2.0),
            Regla("SIN_FUERA_VIGENCIA", "consistencia", "ADVERTENCIA", "MARCAR",
                  "Ocurrencia fuera de la vigencia de la póliza (posible falta de cobertura)",
                  "_existe_poliza AND (fecha_ocurrencia < _pol_inicio OR fecha_ocurrencia > _pol_fin)", 5.0),
            Regla("SIN_POSTERIOR_CORTE", "oportunidad", "INFORMATIVA", "MARCAR",
                  "Ocurrencia posterior a la fecha de corte", "fecha_ocurrencia > _fecha_corte"),
        ],
        columnas_tecnicas_silver=["_pol_inicio", "_pol_fin", "_existe_poliza"],
    ),
    "movimientos": EspecEntidad(
        nombre="movimientos", clave="evento_id",
        fechas=["fecha_evento", "fecha_ingesta"], montos=[], enteros=["secuencia"],
        textos=["evento_id", "entidad", "id_entidad", "tipo_evento"],
        orden_version=["fecha_ingesta"], padres=["clientes", "polizas", "recaudos", "siniestros"],
        enriquecer=_enriquecer_movimientos,
        reglas=_reglas_base("MOV", "evento_id") + [
            Regla("MOV_FECHA_EVENTO_NULA", "completitud", "BLOQUEO", "CUARENTENA",
                  "Fecha del evento ausente o ilegible", "fecha_evento IS NULL", 1.0),
            Regla("MOV_PAYLOAD_INVALIDO", "validez", "BLOQUEO", "CUARENTENA",
                  "Payload no es un JSON válido", "NOT _payload_valido", 1.0),
            Regla("MOV_ENTIDAD_INEXISTENTE", "integridad", "ADVERTENCIA", "MARCAR",
                  "El evento referencia una entidad que no existe en Silver", "NOT _existe_entidad", 2.0),
            Regla("MOV_INGESTA_ANTES_EVENTO", "consistencia", "ADVERTENCIA", "MARCAR",
                  "Ingestado antes de ocurrir (reloj o fecha de evento errónea)",
                  "fecha_ingesta < fecha_evento", 5.0),
            Regla("MOV_TARDIO", "oportunidad", "INFORMATIVA", "MARCAR",
                  f"Evento recibido más de {DIAS_MOVIMIENTO_TARDIO} días después de ocurrido",
                  f"datediff(fecha_ingesta, fecha_evento) > {DIAS_MOVIMIENTO_TARDIO}"),
        ],
        columnas_tecnicas_silver=["_payload_valido", "_existe_entidad"],
    ),
}

ORDEN_PROCESO = ["clientes", "polizas", "recaudos", "siniestros", "movimientos"]