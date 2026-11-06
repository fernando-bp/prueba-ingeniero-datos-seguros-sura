# Convenciones de nombres

Regla general: todo en minúsculas, `snake_case`, sin tildes ni espacios.

## Objetos de Databricks

| Objeto | Prefijo / patrón | Ejemplo |
|---|---|---|
| Notebook | `nb_<orden>_<capa>_<accion>` | `nb_01_bronze_ingesta` |
| Job / Workflow | `job_<dominio>_<frecuencia>` | `job_seguros_mensual` |
| Pipeline (Lakeflow/DLT) | `pl_<dominio>_<proposito>` | `pl_seguros_eventos` |
| Tarea dentro del job | `tsk_<capa>_<accion>` | `tsk_silver_polizas` |

## Tablas y objetos en Unity Catalog (`workspace.default`)

| Tipo | Patrón | Ejemplo |
|---|---|---|
| Bronze | `bronze_<entidad>` | `bronze_polizas` |
| Silver | `silver_<entidad>` | `silver_siniestros` |
| Gold | `gold_<producto>` | `gold_poliza_360` |
| Cuarentena | `quarantine_<entidad>` | `quarantine_recaudos` |
| Control / auditoría | `ctl_<proposito>` | `ctl_ejecuciones`, `ctl_watermark` |
| Vista | `vw_<proposito>` | `vw_poliza_360_enmascarada` |
| Función | `fn_<proposito>` | `fn_mask_documento` |
| Volumen | `volume1` (definido por la prueba) | `raw/{fecha_corte}/{entidad}.csv` |

Entidad siempre en plural y en español, igual que la fuente: `clientes`, `polizas`, `recaudos`, `siniestros`, `movimientos`.

## Columnas

- Negocio: `snake_case` en español, igual a la fuente (`poliza_id`, `fecha_pago`).
- Fechas: prefijo `fecha_`; montos: `valor_` o `prima`.
- Técnicas / metadatos: prefijo `_` para distinguirlas del negocio:
  `_source_file`, `_ingestion_ts`, `_run_id`, `_fecha_corte`, `_row_hash`.

## Código

| Elemento | Patrón | Ejemplo |
|---|---|---|
| Módulo Python | `<proposito>.py` | `src/ingestion/bronze.py` |
| Función | verbo en snake_case | `leer_csv()`, `estandarizar_fechas()` |
| Clase | PascalCase | `PipelineConfig` |
| Prueba | `tests/test_<modulo>.py` | `tests/test_config.py` |
| Parámetro del job | snake_case | `fecha_corte`, `modo`, `env` |

## Git

- Ramas: `dll` (principal), `feature/<tema>`, `fix/<tema>`.
- Commits (Conventional Commits): `feat:`, `fix:`, `docs:`, `test:`, `chore:`, `refactor:`.

## Ambientes (arquitectura objetivo)

En la prueba todo vive en `workspace.default`. En el escenario corporativo
el ambiente se separa por catálogo: `seguros_dev`, `seguros_qa`, `seguros_prod`,
con los mismos nombres de esquema y tabla en cada uno.