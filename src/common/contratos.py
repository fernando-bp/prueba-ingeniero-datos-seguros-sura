"""Contratos de datos: lo que productor y consumidor acuerdan sobre cada fuente.

Un contrato define, por entidad: dueño, frecuencia, SLA, clave de negocio y, por columna,
tipo esperado, si es obligatoria, su clasificación de sensibilidad y su descripción.

Se usa en tres lugares:
1. Bronze valida el esquema recibido contra el contrato ANTES de escribir.
2. Gobierno (Unity Catalog) toma descripciones y clasificación para COMMENT, TAGS y máscaras.
3. Documentación: es la fuente única de verdad del diccionario de datos.
"""
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List

from pyspark.sql.types import ArrayType, DateType, StringType, StructField, StructType, TimestampType

from src.common.config import PipelineConfig, get_logger

log = get_logger("contratos")

# Clasificación de sensibilidad (de mayor a menor)
PII_ALTA = "pii_alta"          # identifica directamente a una persona: documento, correo, teléfono
PII_MEDIA = "pii_media"        # identifica combinada con otros datos: fecha de nacimiento
CONFIDENCIAL = "confidencial"  # dato financiero sensible: referencia bancaria
INTERNO = "interno"            # uso interno, sin restricción especial


@dataclass(frozen=True)
class Columna:
    nombre: str
    tipo: str                  # tipo esperado en Silver: string | date | decimal | int | json
    obligatoria: bool
    descripcion: str
    clasificacion: str = INTERNO


@dataclass(frozen=True)
class Contrato:
    entidad: str
    descripcion: str
    propietario: str           # área dueña del dato en la fuente
    sistema_origen: str
    frecuencia: str
    sla: str
    clave: List[str]
    columnas: List[Columna] = field(default_factory=list)

    @property
    def obligatorias(self) -> List[str]:
        return [c.nombre for c in self.columnas if c.obligatoria]

    @property
    def nombres(self) -> List[str]:
        return [c.nombre for c in self.columnas]

    def sensibles(self) -> Dict[str, str]:
        return {c.nombre: c.clasificacion for c in self.columnas if c.clasificacion != INTERNO}


_ORIGEN = Columna("origen", "string", False, "Sistema que generó el registro")

CONTRATOS: Dict[str, Contrato] = {
    "clientes": Contrato(
        "clientes", "Maestro de clientes; una fila por cliente o versión del cliente",
        "Gerencia de Clientes", "CRM", "mensual", "Disponible el día 5 antes de las 06:00", ["cliente_id"],
        [
            Columna("cliente_id", "string", True, "Identificador único del cliente"),
            Columna("tipo_documento", "string", True, "CC, NIT, CE o PP"),
            Columna("numero_documento", "string", False, "Número de documento de identidad", PII_ALTA),
            Columna("fecha_nacimiento", "date", False, "Fecha de nacimiento", PII_MEDIA),
            Columna("ciudad", "string", False, "Ciudad de residencia"),
            Columna("segmento", "string", False, "Segmento comercial"),
            Columna("correo", "string", False, "Correo electrónico", PII_ALTA),
            Columna("telefono", "string", False, "Teléfono de contacto", PII_ALTA),
            Columna("fecha_actualizacion", "date", True, "Última modificación en el CRM"),
            _ORIGEN,
        ]),
    "polizas": Contrato(
        "polizas", "Pólizas emitidas; una fila por póliza o movimiento de estado",
        "Gerencia Técnica", "CORE_POLIZAS", "mensual", "Disponible el día 5 antes de las 06:00", ["poliza_id"],
        [
            Columna("poliza_id", "string", True, "Identificador único de la póliza"),
            Columna("cliente_id", "string", True, "Cliente tomador (FK a clientes)"),
            Columna("producto", "string", False, "Ramo o producto"),
            Columna("sucursal", "string", True, "Sucursal emisora"),
            Columna("fecha_inicio_vigencia", "date", True, "Inicio de cobertura"),
            Columna("fecha_fin_vigencia", "date", True, "Fin de cobertura"),
            Columna("estado", "string", True, "Estado reportado por la fuente"),
            Columna("prima", "decimal", True, "Prima emitida en la moneda indicada"),
            Columna("moneda", "string", False, "Moneda de la prima"),
            Columna("canal", "string", False, "Canal de venta"),
            Columna("fecha_actualizacion", "date", True, "Última modificación en el core"),
            _ORIGEN,
        ]),
    "recaudos": Contrato(
        "recaudos", "Transacciones de pago; una fila por transacción",
        "Gerencia Financiera", "PAGOS", "mensual", "Disponible el día 5 antes de las 06:00", ["recaudo_id"],
        [
            Columna("recaudo_id", "string", True, "Identificador único de la transacción"),
            Columna("poliza_id", "string", True, "Póliza pagada (FK a polizas)"),
            Columna("fecha_pago", "date", True, "Fecha del pago"),
            Columna("valor", "decimal", True, "Valor de la transacción"),
            Columna("medio_pago", "string", False, "Medio de pago"),
            Columna("estado_transaccion", "string", True, "Aplicado, Pendiente, Rechazado o Reversado"),
            Columna("referencia_bancaria", "string", False, "Referencia de la entidad financiera", CONFIDENCIAL),
            Columna("fecha_ingesta", "date", True, "Fecha de registro en el sistema de pagos"),
            _ORIGEN,
        ]),
    "siniestros": Contrato(
        "siniestros", "Reclamaciones; una fila por siniestro o actualización",
        "Gerencia de Indemnizaciones", "CORE_SINIESTROS", "mensual", "Disponible el día 5 antes de las 06:00",
        ["siniestro_id"],
        [
            Columna("siniestro_id", "string", True, "Identificador único del siniestro"),
            Columna("poliza_id", "string", True, "Póliza afectada (FK a polizas)"),
            Columna("fecha_ocurrencia", "date", True, "Fecha en que ocurrió el evento"),
            Columna("fecha_aviso", "date", False, "Fecha en que se reportó"),
            Columna("tipo_siniestro", "string", False, "Tipo de siniestro"),
            Columna("estado", "string", False, "Estado del siniestro"),
            Columna("valor_pagado", "decimal", True, "Valor pagado acumulado"),
            Columna("valor_reserva", "decimal", True, "Reserva pendiente estimada"),
            Columna("ciudad_ocurrencia", "string", False, "Ciudad del evento"),
            Columna("fecha_actualizacion", "date", True, "Última modificación"),
            _ORIGEN,
        ]),
    "movimientos": Contrato(
        "movimientos", "Eventos de negocio; una fila por evento",
        "Arquitectura de Integración", "BUS_EVENTOS", "mensual", "Disponible el día 5 antes de las 06:00",
        ["evento_id"],
        [
            Columna("evento_id", "string", True, "Identificador único del evento"),
            Columna("entidad", "string", True, "POLIZA, SINIESTRO, RECAUDO o CLIENTE"),
            Columna("id_entidad", "string", True, "Identificador de la entidad afectada"),
            Columna("tipo_evento", "string", True, "Tipo de evento"),
            Columna("fecha_evento", "date", True, "Fecha en que ocurrió el evento"),
            Columna("fecha_ingesta", "date", True, "Fecha de recepción en el bus"),
            Columna("payload", "json", True, "Atributos del evento en JSON"),
            Columna("secuencia", "int", False, "Orden del evento dentro de la entidad"),
            _ORIGEN,
        ]),
}


@dataclass
class ResultadoEsquema:
    entidad: str
    recibidas: List[str]
    faltantes_obligatorias: List[str]
    faltantes_opcionales: List[str]
    nuevas: List[str]

    @property
    def estado(self) -> str:
        if self.faltantes_obligatorias:
            return "INCUMPLE"            # BLOQUEO: no se puede procesar
        if self.nuevas or self.faltantes_opcionales:
            return "CAMBIO_ESQUEMA"      # ADVERTENCIA: se procesa y se alerta
        return "CUMPLE"

    @property
    def huella(self) -> str:
        """Hash del esquema recibido: si cambia entre ejecuciones, el esquema evolucionó."""
        return hashlib.sha256("|".join(sorted(self.recibidas)).encode()).hexdigest()[:16]


def validar_esquema(columnas_recibidas: List[str], entidad: str) -> ResultadoEsquema:
    """Compara las columnas recibidas (sin las técnicas _*) contra el contrato."""
    contrato = CONTRATOS[entidad]
    recibidas = [c for c in columnas_recibidas if not c.startswith("_")]
    esperadas = set(contrato.nombres)
    return ResultadoEsquema(
        entidad=entidad,
        recibidas=recibidas,
        faltantes_obligatorias=[c for c in contrato.obligatorias if c not in recibidas],
        faltantes_opcionales=[c for c in contrato.nombres if c not in recibidas and c not in contrato.obligatorias],
        nuevas=[c for c in recibidas if c not in esperadas],
    )


ESQUEMA_CTL_ESQUEMAS = StructType([
    StructField("run_id", StringType(), False),
    StructField("fecha_corte", DateType(), False),
    StructField("entidad", StringType(), False),
    StructField("estado", StringType(), False),
    StructField("huella_esquema", StringType(), False),
    StructField("columnas_recibidas", ArrayType(StringType()), False),
    StructField("faltantes_obligatorias", ArrayType(StringType()), False),
    StructField("faltantes_opcionales", ArrayType(StringType()), False),
    StructField("columnas_nuevas", ArrayType(StringType()), False),
    StructField("validado_ts", TimestampType(), False),
])


def registrar_y_exigir(spark, cfg: PipelineConfig, resultado: ResultadoEsquema) -> None:
    """Guarda el resultado en ctl_esquemas y falla si el contrato se incumple."""
    fila = {
        "run_id": cfg.run_id,
        "fecha_corte": datetime.strptime(cfg.fecha_corte, "%Y-%m-%d").date(),
        "entidad": resultado.entidad, "estado": resultado.estado, "huella_esquema": resultado.huella,
        "columnas_recibidas": resultado.recibidas,
        "faltantes_obligatorias": resultado.faltantes_obligatorias,
        "faltantes_opcionales": resultado.faltantes_opcionales,
        "columnas_nuevas": resultado.nuevas,
        "validado_ts": datetime.now(timezone.utc).replace(tzinfo=None),
    }
    (spark.createDataFrame([fila], ESQUEMA_CTL_ESQUEMAS).write.format("delta").mode("overwrite")
     .option("replaceWhere", f"run_id = '{cfg.run_id}' AND entidad = '{resultado.entidad}'")
     .saveAsTable(cfg.table("ctl_esquemas")))

    msg = f"[{cfg.run_id}] Contrato {resultado.entidad}: {resultado.estado} (huella {resultado.huella})"
    if resultado.estado == "INCUMPLE":
        log.error(f"{msg} faltan obligatorias {resultado.faltantes_obligatorias}")
        raise ValueError(f"Contrato incumplido en {resultado.entidad}: faltan {resultado.faltantes_obligatorias}")
    if resultado.estado == "CAMBIO_ESQUEMA":
        log.warning(f"{msg} nuevas={resultado.nuevas} faltantes_opcionales={resultado.faltantes_opcionales}")
    else:
        log.info(msg)