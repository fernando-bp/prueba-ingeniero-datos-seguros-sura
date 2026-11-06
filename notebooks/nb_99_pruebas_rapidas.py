import sys, os
sys.path.append(os.path.abspath(".."))   # agrega la raíz del repo para poder importar src

from src.common.config import PipelineConfig, get_logger

cfg = PipelineConfig(fecha_corte="2026-11-05")
log = get_logger("prueba")

log.info(f"run_id = {cfg.run_id}")
log.info(f"raw_path = {cfg.raw_path}")
log.info(f"tabla = {cfg.table('bronze_clientes')}")
display(dbutils.fs.ls(cfg.raw_path))