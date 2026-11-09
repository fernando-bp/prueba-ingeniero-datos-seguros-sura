"""Construcción de productos Gold (funciones puras: DataFrames -> DataFrame).

Reglas de negocio comunes:
- Todo se calcula "a la fecha de corte": hechos con fecha posterior al corte no cuentan.
- Recaudo efectivo = transacciones con estado 'Aplicado'. Pendiente/Rechazado/Reversado no suman.
- Costo incurrido = valor pagado + reserva (pagos negativos = recobros, restan).
- Fin efectivo de vigencia = fecha de fin, o la fecha del evento CANCELACION si la póliza
  está Cancelada y existe ese evento en movimientos (anterior al fin).
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

DIAS_PROXIMA_A_VENCER = 30
NO_INFORMADO = "No informado"


def _corte(fecha_corte: str):
    return F.lit(fecha_corte).cast("date")


def fin_efectivo_polizas(polizas: DataFrame, movimientos: DataFrame) -> DataFrame:
    """Agrega fecha_cancelacion y fin_efectivo a cada póliza."""
    cancel = (movimientos
              .filter((F.col("entidad") == "POLIZA") & (F.col("tipo_evento") == "CANCELACION"))
              .groupBy(F.col("id_entidad").alias("poliza_id"))
              .agg(F.min("fecha_evento").alias("_evento_cancelacion")))
    return (polizas.join(cancel, "poliza_id", "left")
            .withColumn("fecha_cancelacion",
                        F.when((F.col("estado") == "Cancelada")
                               & (F.col("_evento_cancelacion") <= F.col("fecha_fin_vigencia")),
                               F.col("_evento_cancelacion")))
            .withColumn("fin_efectivo", F.coalesce("fecha_cancelacion", "fecha_fin_vigencia"))
            .drop("_evento_cancelacion"))


def estado_al_corte(fecha_corte: str):
    corte = _corte(fecha_corte)
    return (F.when((F.col("estado") == "Cancelada") & (F.col("fin_efectivo") <= corte), "Cancelada")
            .when(F.col("fecha_inicio_vigencia") > corte, "Futura")
            .when(F.col("fin_efectivo") < corte, "Vencida")
            .when(F.datediff(F.col("fin_efectivo"), corte) <= DIAS_PROXIMA_A_VENCER, "Proxima a vencer")
            .otherwise("Vigente"))


def construir_poliza_360(s: dict, fecha_corte: str) -> DataFrame:
    """Una fila por póliza. `s` = dict de DataFrames silver por entidad."""
    corte = _corte(fecha_corte)
    pol = fin_efectivo_polizas(s["polizas"], s["movimientos"])

    rec = (s["recaudos"].filter(F.col("fecha_pago") <= corte)
           .groupBy("poliza_id").agg(
               F.sum(F.when(F.col("estado_transaccion") == "Aplicado", F.col("valor")).otherwise(0)).alias("prima_recaudada"),
               F.sum(F.when(F.col("estado_transaccion") == "Pendiente", F.col("valor")).otherwise(0)).alias("recaudo_en_tramite"),
               F.count(F.lit(1)).alias("num_transacciones"),
               F.max(F.when(F.col("estado_transaccion") == "Aplicado", F.col("fecha_pago"))).alias("fecha_ultimo_pago"),
               F.max("fecha_pago").alias("_ult_rec")))

    sin = (s["siniestros"].filter(F.col("fecha_ocurrencia") <= corte)
           .groupBy("poliza_id").agg(
               F.count(F.lit(1)).alias("num_siniestros"),
               F.sum(F.when(F.col("estado").isin("Abierto", "En investigacion", "Reactivado"), 1).otherwise(0)).alias("siniestros_abiertos"),
               F.sum("valor_pagado").alias("valor_pagado_siniestros"),
               F.sum("valor_reserva").alias("valor_reserva_siniestros"),
               F.max(F.greatest("fecha_ocurrencia", "fecha_aviso", "fecha_actualizacion")).alias("_ult_sin")))

    mov = (s["movimientos"].filter((F.col("entidad") == "POLIZA") & (F.col("fecha_evento") <= corte))
           .groupBy(F.col("id_entidad").alias("poliza_id")).agg(
               F.max("fecha_evento").alias("_ult_mov"),
               F.max_by("tipo_evento", "fecha_evento").alias("ultimo_evento")))

    cli = s["clientes"].select("cliente_id", "segmento", F.col("ciudad").alias("ciudad_cliente"))

    df = (pol.join(cli, "cliente_id", "left").join(rec, "poliza_id", "left")
          .join(sin, "poliza_id", "left").join(mov, "poliza_id", "left"))
    ceros = ["prima_recaudada", "recaudo_en_tramite", "num_transacciones", "num_siniestros",
             "siniestros_abiertos", "valor_pagado_siniestros", "valor_reserva_siniestros"]
    df = df.fillna(0, subset=ceros)

    return df.select(
        "poliza_id", "cliente_id",
        F.coalesce("segmento", F.lit(NO_INFORMADO)).alias("segmento"),
        F.coalesce("ciudad_cliente", F.lit(NO_INFORMADO)).alias("ciudad_cliente"),
        F.coalesce("producto", F.lit(NO_INFORMADO)).alias("producto"),
        "sucursal", "canal",
        F.col("estado").alias("estado_fuente"),
        estado_al_corte(fecha_corte).alias("estado_al_corte"),
        "fecha_inicio_vigencia", "fecha_fin_vigencia", "fecha_cancelacion", "fin_efectivo",
        F.datediff("fin_efectivo", corte).alias("dias_para_vencer"),
        F.col("prima").alias("prima_emitida"),
        F.col("prima_recaudada").cast("decimal(18,2)").alias("prima_recaudada"),
        F.greatest(F.col("prima") - F.col("prima_recaudada"), F.lit(0)).cast("decimal(18,2)").alias("prima_pendiente"),
        F.col("recaudo_en_tramite").cast("decimal(18,2)").alias("recaudo_en_tramite"),
        "num_transacciones", "fecha_ultimo_pago",
        "num_siniestros", "siniestros_abiertos",
        F.col("valor_pagado_siniestros").cast("decimal(18,2)").alias("valor_pagado_siniestros"),
        F.col("valor_reserva_siniestros").cast("decimal(18,2)").alias("valor_reserva_siniestros"),
        (F.col("valor_pagado_siniestros") + F.col("valor_reserva_siniestros")).cast("decimal(18,2)").alias("costo_incurrido"),
        F.greatest("fecha_actualizacion", "_ult_rec", "_ult_sin", "_ult_mov").alias("fecha_ultima_actividad"),
        "ultimo_evento",
        F.col("segmento").isNotNull().alias("cliente_valido"),
        F.size("_fallas").alias("num_alertas_calidad"),
        F.col("_fallas").alias("alertas_calidad"),
        corte.alias("fecha_corte"),
    )


def construir_siniestralidad_mensual(s: dict, fecha_corte: str) -> DataFrame:
    """Una fila por mes, producto y sucursal.

    - polizas_expuestas: pólizas con al menos un día de vigencia efectiva en el mes.
    - prima_devengada: prima prorrateada por los días de exposición del mes (hasta el corte).
    - siniestros / costo_incurrido: por mes de ocurrencia (base accidente).
    - prima_emitida: por mes de inicio de vigencia; prima_recaudada: por mes de pago (Aplicado).
    """
    corte = _corte(fecha_corte)
    pol = (fin_efectivo_polizas(s["polizas"], s["movimientos"])
           .withColumn("producto", F.coalesce("producto", F.lit(NO_INFORMADO)))
           .withColumn("_dias_vigencia", F.greatest(F.datediff("fin_efectivo", "fecha_inicio_vigencia") + 1, F.lit(1))))
    dims = ["mes", "producto", "sucursal"]

    expo = (pol.filter(F.col("fecha_inicio_vigencia") <= corte)
            .withColumn("_hasta", F.least("fin_efectivo", corte))
            .filter(F.col("_hasta") >= F.col("fecha_inicio_vigencia"))
            .withColumn("mes", F.explode(F.sequence(F.trunc("fecha_inicio_vigencia", "month"),
                                                     F.trunc("_hasta", "month"), F.expr("interval 1 month"))))
            .withColumn("_dias_mes", F.datediff(F.least(F.last_day("mes"), F.col("_hasta")),
                                                F.greatest(F.col("mes"), F.col("fecha_inicio_vigencia"))) + 1)
            .groupBy(*dims).agg(
                F.countDistinct("poliza_id").alias("polizas_expuestas"),
                F.sum(F.col("prima") * F.col("_dias_mes") / F.col("_dias_vigencia")).alias("prima_devengada")))

    emi = (pol.filter(F.col("fecha_inicio_vigencia") <= corte)
           .withColumn("mes", F.trunc("fecha_inicio_vigencia", "month"))
           .groupBy(*dims).agg(F.sum("prima").alias("prima_emitida")))

    p_dim = pol.select("poliza_id", "producto", "sucursal")
    rec = (s["recaudos"].filter((F.col("fecha_pago") <= corte) & (F.col("estado_transaccion") == "Aplicado"))
           .join(p_dim, "poliza_id").withColumn("mes", F.trunc("fecha_pago", "month"))
           .groupBy(*dims).agg(F.sum("valor").alias("prima_recaudada")))

    sin = (s["siniestros"].filter(F.col("fecha_ocurrencia") <= corte)
           .join(p_dim, "poliza_id").withColumn("mes", F.trunc("fecha_ocurrencia", "month"))
           .groupBy(*dims).agg(
               F.count(F.lit(1)).alias("num_siniestros"),
               F.sum("valor_pagado").alias("valor_pagado"),
               F.sum("valor_reserva").alias("valor_reserva")))

    df = (expo.join(emi, dims, "full").join(rec, dims, "full").join(sin, dims, "full")
          .fillna(0, subset=["polizas_expuestas", "prima_devengada", "prima_emitida", "prima_recaudada",
                             "num_siniestros", "valor_pagado", "valor_reserva"]))
    costo = F.col("valor_pagado") + F.col("valor_reserva")
    return df.select(
        F.date_format("mes", "yyyy-MM").alias("periodo"), F.col("mes").alias("mes_inicio"),
        "producto", "sucursal", "polizas_expuestas",
        F.col("prima_emitida").cast("decimal(18,2)").alias("prima_emitida"),
        F.col("prima_recaudada").cast("decimal(18,2)").alias("prima_recaudada"),
        F.col("prima_devengada").cast("decimal(18,2)").alias("prima_devengada"),
        "num_siniestros",
        F.col("valor_pagado").cast("decimal(18,2)").alias("valor_pagado"),
        F.col("valor_reserva").cast("decimal(18,2)").alias("valor_reserva"),
        costo.cast("decimal(18,2)").alias("costo_incurrido"),
        F.when(F.col("prima_devengada") > 0, F.round(costo / F.col("prima_devengada"), 4)).alias("razon_siniestralidad"),
        corte.alias("fecha_corte"),
    )


def construir_discrepancias(s: dict, fecha_corte: str) -> DataFrame:
    """Una fila por (registro, discrepancia): pagos sin póliza, siniestros sin póliza válida,
    hechos fuera de vigencia y movimientos de póliza fuera de vigencia."""
    corte = _corte(fecha_corte)
    codigos = ["REC_SIN_POLIZA", "REC_FUERA_VIGENCIA", "SIN_SIN_POLIZA", "SIN_FUERA_VIGENCIA",
               "POL_SIN_CLIENTE", "MOV_ENTIDAD_INEXISTENTE"]

    def desde(df, entidad, id_col, pol_col, fecha_col):
        return (df.select(F.lit(entidad).alias("entidad"), F.col(id_col).alias("id_registro"),
                          F.col(pol_col).alias("poliza_id"), F.col(fecha_col).alias("fecha_hecho"),
                          F.explode("_fallas").alias("discrepancia"))
                .filter(F.col("discrepancia").isin(codigos)))

    pol = fin_efectivo_polizas(s["polizas"], s["movimientos"]).select(
        "poliza_id", F.col("fecha_inicio_vigencia").alias("_ini"), "fin_efectivo")
    mov_fuera = (s["movimientos"].filter(F.col("entidad") == "POLIZA")
                 .join(pol, F.col("id_entidad") == F.col("poliza_id"))
                 .filter((F.col("fecha_evento") < F.col("_ini")) | (F.col("fecha_evento") > F.col("fin_efectivo")))
                 .select(F.lit("movimientos").alias("entidad"), F.col("evento_id").alias("id_registro"),
                         "poliza_id", F.col("fecha_evento").alias("fecha_hecho"),
                         F.lit("MOV_POLIZA_FUERA_VIGENCIA").alias("discrepancia")))

    return (desde(s["recaudos"], "recaudos", "recaudo_id", "poliza_id", "fecha_pago")
            .unionByName(desde(s["siniestros"], "siniestros", "siniestro_id", "poliza_id", "fecha_ocurrencia"))
            .unionByName(desde(s["polizas"], "polizas", "poliza_id", "poliza_id", "fecha_inicio_vigencia"))
            .unionByName(desde(s["movimientos"], "movimientos", "evento_id",
                               "id_entidad", "fecha_evento"))
            .unionByName(mov_fuera)
            .withColumn("fecha_corte", corte))


def construir_calidad_pipeline(calidad: DataFrame, cargas: DataFrame) -> DataFrame:
    """Historial completo de calidad y reconciliación: una fila por (ejecución, dataset, control).

    - REGLA: cada resultado de ctl_calidad_resultados.
    - RECONCILIACION de conteos: por cada carga, filas leídas = escritas + rechazadas.
    - RECONCILIACION monetaria: por cada carga silver, total bronze = silver + cuarentena,
      comparando con la última carga bronze de la misma fecha de corte anterior a la silver.
    Se reconstruye completo en cada ejecución (es pequeño), por lo que nunca duplica.
    """
    reglas = calidad.select(
        "run_id", "fecha_corte", "etapa", "dataset", F.lit("REGLA").alias("tipo_control"),
        "regla", "dimension", "severidad", "accion", "descripcion", "evaluados", "fallidos",
        "pct_fallidos", "umbral_pct", "resultado", F.col("expresion").alias("detalle"),
        F.col("evaluado_ts").alias("control_ts"))

    conteos = cargas.filter(F.col("filas_leidas").isNotNull()).select(   # gold no tiene "entrada" única
        "run_id", "fecha_corte", F.col("capa").alias("etapa"),
        F.col("tabla").alias("dataset"), F.lit("RECONCILIACION").alias("tipo_control"),
        F.lit("CONTEO_ENTRADA_VS_SALIDA").alias("regla"), F.lit("consistencia").alias("dimension"),
        F.lit("BLOQUEO").alias("severidad"), F.lit("N/A").alias("accion"),
        F.lit("filas leídas = escritas + rechazadas").alias("descripcion"),
        F.col("filas_leidas").alias("evaluados"),
        F.abs(F.col("filas_leidas") - F.col("filas_escritas") - F.coalesce("filas_rechazadas", F.lit(0))).alias("fallidos"),
        F.lit(None).cast("double").alias("pct_fallidos"), F.lit(0.0).alias("umbral_pct"),
        F.when(F.col("filas_leidas") == F.col("filas_escritas") + F.coalesce("filas_rechazadas", F.lit(0)), "OK")
        .otherwise("FALLA").alias("resultado"),
        F.concat_ws(" ", F.lit("leidas="), F.col("filas_leidas"), F.lit("escritas="), F.col("filas_escritas"),
                    F.lit("rechazadas="), F.col("filas_rechazadas")).alias("detalle"),
        F.col("registrado_ts").alias("control_ts"))

    b = (cargas.filter(F.col("capa") == "bronze")
         .select(F.regexp_replace("tabla", "^bronze_", "").alias("ent"),
                 F.col("fecha_corte").alias("_fc"), F.col("registrado_ts").alias("_b_ts"),
                 F.explode("totales_monetarios").alias("columna", "total_bronze")))
    sv = (cargas.filter(F.col("capa") == "silver")
          .select(F.regexp_replace("tabla", "^silver_", "").alias("ent"), "run_id", "tabla",
                  "fecha_corte", "registrado_ts", "totales_monetarios"))
    w = Window.partitionBy("run_id", "tabla", "columna").orderBy(F.col("_b_ts").desc())
    dinero = (sv.join(b, (sv.ent == b.ent) & (sv.fecha_corte == b._fc) & (b._b_ts <= sv.registrado_ts))
              .withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
              .withColumn("total_silver", F.col("totales_monetarios")[F.col("columna")])
              .withColumn("total_cuar", F.coalesce(
                  F.col("totales_monetarios")[F.concat(F.col("columna"), F.lit("_cuarentena"))], F.lit(0)))
              .withColumn("dif", F.col("total_bronze") - F.col("total_silver") - F.col("total_cuar"))
              .select(
                  "run_id", "fecha_corte", F.lit("silver").alias("etapa"),
                  F.col("tabla").alias("dataset"), F.lit("RECONCILIACION").alias("tipo_control"),
                  F.concat(F.lit("MONTO_"), F.upper("columna")).alias("regla"),
                  F.lit("consistencia").alias("dimension"), F.lit("BLOQUEO").alias("severidad"),
                  F.lit("N/A").alias("accion"),
                  F.lit("total bronze = total silver + total cuarentena").alias("descripcion"),
                  F.lit(None).cast("long").alias("evaluados"), F.lit(None).cast("long").alias("fallidos"),
                  F.lit(None).cast("double").alias("pct_fallidos"), F.lit(0.0).alias("umbral_pct"),
                  F.when(F.abs(F.col("dif")) < 0.01, "OK").otherwise("FALLA").alias("resultado"),
                  F.concat_ws(" ", F.lit("bronze="), F.col("total_bronze"), F.lit("silver="), F.col("total_silver"),
                              F.lit("cuarentena="), F.col("total_cuar"), F.lit("dif="), F.col("dif")).alias("detalle"),
                  F.col("registrado_ts").alias("control_ts")))

    return (reglas.unionByName(conteos).unionByName(dinero)
            .withColumn("publicado_ts", F.current_timestamp()))