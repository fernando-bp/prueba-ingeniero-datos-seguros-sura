# Pipeline de pólizas y siniestros en Databricks

Solución end-to-end de ingeniería de datos que integra clientes, pólizas, recaudos, siniestros y movimientos en una arquitectura Medallion (Bronze, Silver, Gold) sobre Databricks, Delta Lake y Unity Catalog. Produce datos confiables, trazables e idempotentes para analítica, tableros y modelos.

| Indicador (fecha de corte 2026-11-05) | Valor |
| --- | --- |
| Registros procesados | 17.510 en 5 fuentes |
| Registros en cuarentena | 37 (0,21 %), con causa y ejecución |
| Reconciliación Bronze = Silver + Cuarentena | 100 % en filas y en montos |
| Reglas de calidad | 42 en 6 dimensiones |
| Pruebas | 11 unitarias + 10 postcondiciones en Gold |
| Duración del job | 3 a 8 minutos |

## Arquitectura

```
volume1/raw/{fecha_corte}/*.csv
        │  contrato de datos
        ▼
     BRONZE  copia fiel en texto + _source_file, _ingestion_ts, _run_id, _fecha_corte, _row_hash
        │  replaceWhere por fecha de corte
        ▼
     SILVER  tipos, deduplicación, cruces, 42 reglas ──► quarantine_* (causa + run_id)
        │  MERGE por clave de negocio
        ▼
      GOLD   poliza_360, siniestralidad_mensual, discrepancias, calidad_pipeline
             (se publica solo si pasan 10 postcondiciones)

Control: ctl_ejecuciones, ctl_cargas, ctl_esquemas, ctl_calidad_resultados → vw_obs_* → tablero
```

El detalle (arquitectura objetivo en Azure, ambientes, gobierno, observabilidad, decisiones y riesgos) está en el documento de arquitectura.

## Estructura del repositorio

```
src/
├── common/      config.py (parámetros), control.py (bitácora), contratos.py (data contracts),
│                gobierno.py (UC), observabilidad.py (vistas de monitoreo)
├── bronze/      ingesta.py
├── silver/      estandarizacion.py, entidades.py (reglas por entidad), transformacion.py, proceso.py
├── quality/     reglas.py (motor de calidad y cuarentena)
└── gold/        productos.py, postcondiciones.py, proceso.py
notebooks/       nb_01_bronze_ingesta, nb_02_silver_transformacion, nb_03_gold_publicacion,
                 nb_04_gobierno, nb_05_observabilidad, nb_90_tests, nb_99_pruebas_rapidas
tests/           conftest.py, test_estandarizacion.py, test_reglas.py, test_contratos.py, test_gobierno.py
resources/       job_seguros_mensual.yml (job como código)
.github/         workflows/ci.yml (pruebas en cada PR), workflows/cd.yml (despliegue por ambiente)
databricks.yml   Databricks Asset Bundle con destinos dev, qa y prod
docs/            convenciones_nombres.md
```

La lógica vive en `src/` como funciones reutilizables y probables; los notebooks solo reciben parámetros y llaman a `src/`.

## Requisitos

- Databricks (Free Edition o superior) con Unity Catalog y cómputo Serverless.
- Catálogo `workspace`, esquema `default` y volumen `workspace.default.volume1`.
- Para pruebas locales: Python 3.11, Java 17 y `pip install -r requirements.txt`.

## Guía de reproducción (10 minutos)

1. **Clonar el repositorio en Databricks.** Workspace → Create → Git folder → URL del repositorio → rama `feature/data-engineering` (o `dll` tras el merge).
2. **Cargar los datos.** Catalog → workspace → default → volume1 → crear la carpeta `raw/2026-11-05/` y subir `clientes.csv`, `polizas.csv`, `recaudos.csv`, `siniestros.csv` y `movimientos.csv`. Los datos no se versionan en Git.
3. **Correr las pruebas.** Abrir `notebooks/nb_90_tests` → Run all. Resultado esperado: `11 passed`.
4. **Ejecutar el pipeline.** Opción A: correr `nb_01`, `nb_02` y `nb_03` en orden con `fecha_corte = 2026-11-05`. Opción B: crear el job desde `resources/job_seguros_mensual.yml` (o en la interfaz) y usar *Run now with different parameters* con `fecha_corte = 2026-11-05`.
5. **Aplicar gobierno.** Crear los grupos `grp_ingenieria_datos`, `grp_analistas`, `grp_auditoria`, `grp_datos_sensibles` y `grp_finanzas`; luego correr `notebooks/nb_04_gobierno`.
6. **Activar observabilidad.** Correr `notebooks/nb_05_observabilidad`.
7. **Verificar.** Resultados esperados:

| Tabla | Filas |
| --- | --- |
| silver_clientes / quarantine_clientes | 501 / 1 |
| silver_polizas / quarantine_polizas | 1.491 / 11 |
| silver_recaudos / quarantine_recaudos | 4.987 / 15 |
| silver_siniestros / quarantine_siniestros | 2.501 / 1 |
| silver_movimientos / quarantine_movimientos | 7.993 / 9 |
| gold_poliza_360 | 1.491 |
| gold_siniestralidad_mensual | 1.751 |

Reejecutar cualquier paso con la misma `fecha_corte` no cambia estos conteos.

## Parámetros

| Parámetro | Valores | Uso |
| --- | --- | --- |
| `fecha_corte` | `YYYY-MM-DD` | Carpeta `raw/{fecha_corte}` y foto de Gold. En el job: `{{job.start_time.iso_date}}` |
| `modo` | `incremental`, `full`, `reproceso` | `full` reconstruye Silver desde Bronze |
| `env` | `dev`, `qa`, `prod` | Ambiente; en el bundle toma el destino del despliegue |
| `run_id` | texto | Identificador de la ejecución. En el job: `{{job.run_id}}`; vacío genera uno |

## Orquestación

Job `job_seguros_mensual`: `tsk_tests → tsk_bronze → tsk_silver → tsk_gold`, el día 5 de cada mes a las 06:00 (America/Bogota). Un reintento a los 5 minutos por tarea, timeout de 30 minutos, una ejecución concurrente y notificación por correo al fallar.

## Pruebas y CI/CD

- **Unitarias** (`tests/`): estandarización de fechas y montos, deduplicación con desempate determinístico, motor de reglas y umbrales, contratos de datos y generación de SQL de gobierno.
- **Postcondiciones** (`src/gold/postcondiciones.py`): una fila por póliza, prima igual a Silver, conteo de siniestros igual a Silver, costo = pagado + reserva, reconciliaciones OK.
- **CI** (`.github/workflows/ci.yml`): en cada push y Pull Request a `dll`, compila el código y corre las pruebas.
- **CD** (`.github/workflows/cd.yml` + `databricks.yml`): despliegue del bundle por ambiente con aprobación en producción (propuesta; en Free Edition solo existe dev).

Ejecución local:

```bash
pip install -r requirements.txt
pytest -v tests
```

## Objetos en Unity Catalog (`workspace.default`)

| Tipo | Objetos |
| --- | --- |
| Volumen | `volume1` |
| Bronze | `bronze_clientes`, `bronze_polizas`, `bronze_recaudos`, `bronze_siniestros`, `bronze_movimientos` |
| Silver | `silver_*` (mismas entidades) |
| Cuarentena | `quarantine_*` |
| Gold | `gold_poliza_360`, `gold_siniestralidad_mensual`, `gold_discrepancias`, `gold_calidad_pipeline` |
| Control | `ctl_ejecuciones`, `ctl_cargas`, `ctl_esquemas`, `ctl_calidad_resultados` |
| Vistas | `vw_clientes_protegido`, `vw_recaudos_protegido`, `vw_obs_*` |
| Funciones | `fn_mask_ultimos`, `fn_mask_correo` |

Convenciones de nombres en `docs/convenciones_nombres.md`.

## Supuestos principales

- Las fechas con barra son día/mes/año.
- Cada archivo mensual es un extracto completo de la fuente.
- Un recaudo negativo solo es válido si no está Aplicado; un pago negativo de siniestro se marca como posible recobro.
- La fecha de cancelación es la del primer evento `CANCELACION` en movimientos.

## Limitaciones y deuda técnica

- Free Edition: un solo ambiente, tablas managed, costo estimado por duración (sin tabla de facturación).
- Borrados en la fuente no se reflejan en Silver (requiere CDC).
- Silver guarda la versión vigente de clientes (SCD tipo 1); SCD tipo 2 queda propuesto.
- Sin columna de saldo a favor para sobrepagos.

## Seguridad

No hay credenciales, tokens ni datos en el repositorio. El token de GitHub vive en Linked accounts de Databricks; en CI/CD las credenciales van en GitHub Secrets. `.gitignore` excluye CSV, Parquet, checkpoints y archivos `.env`.
