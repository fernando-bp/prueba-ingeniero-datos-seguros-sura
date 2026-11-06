"""Configuración central del pipeline.

Todos los parámetros (fecha de corte, modo, ambiente, rutas) se definen aquí.
Ningún notebook debe tener rutas ni nombres de tablas escritos a mano.
"""
import logging
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

MODOS_VALIDOS = ("incremental", "full", "reproceso")
AMBIENTES_VALIDOS = ("dev", "qa", "prod")


def _nuevo_run_id() -> str:
    """Identificador único de ejecución: fecha-hora UTC + sufijo aleatorio."""
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:8]


@dataclass(frozen=True)
class PipelineConfig:
    fecha_corte: str                 # 'YYYY-MM-DD' -> lee la carpeta raw/{fecha_corte}
    modo: str = "incremental"        # incremental | full | reproceso
    env: str = "dev"                 # dev | qa | prod
    catalog: str = "workspace"
    schema: str = "default"
    volume: str = "volume1"
    run_id: str = field(default_factory=_nuevo_run_id)

    def __post_init__(self):
        try:
            date.fromisoformat(self.fecha_corte)
        except ValueError as e:
            raise ValueError(f"fecha_corte inválida '{self.fecha_corte}', se espera YYYY-MM-DD") from e
        if self.modo not in MODOS_VALIDOS:
            raise ValueError(f"modo inválido '{self.modo}', opciones: {MODOS_VALIDOS}")
        if self.env not in AMBIENTES_VALIDOS:
            raise ValueError(f"env inválido '{self.env}', opciones: {AMBIENTES_VALIDOS}")

    @property
    def volume_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"

    @property
    def raw_path(self) -> str:
        """Carpeta de aterrizaje de la fecha de corte."""
        return f"{self.volume_path}/raw/{self.fecha_corte}"

    def table(self, nombre: str) -> str:
        """Nombre completo de tabla en Unity Catalog: catalog.schema.tabla"""
        return f"{self.catalog}.{self.schema}.{nombre}"


def get_logger(nombre: str) -> logging.Logger:
    """Logger con formato estándar. Se usa en lugar de print()."""
    logger = logging.getLogger(nombre)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger