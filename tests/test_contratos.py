from src.common.contratos import CONTRATOS, PII_ALTA, validar_esquema


def test_contrato_cumple_con_columnas_tecnicas_ignoradas():
    cols = CONTRATOS["polizas"].nombres + ["_source_file", "_row_hash"]
    r = validar_esquema(cols, "polizas")
    assert r.estado == "CUMPLE" and r.nuevas == [] and r.faltantes_obligatorias == []


def test_contrato_detecta_faltante_obligatoria_y_columna_nueva():
    cols = [c for c in CONTRATOS["recaudos"].nombres if c != "valor"] + ["canal_pago"]
    r = validar_esquema(cols, "recaudos")
    assert r.estado == "INCUMPLE" and r.faltantes_obligatorias == ["valor"] and r.nuevas == ["canal_pago"]
    solo_nueva = validar_esquema(CONTRATOS["recaudos"].nombres + ["canal_pago"], "recaudos")
    assert solo_nueva.estado == "CAMBIO_ESQUEMA"                # se procesa, pero alerta
    assert solo_nueva.huella != validar_esquema(CONTRATOS["recaudos"].nombres, "recaudos").huella


def test_contrato_clasifica_columnas_sensibles():
    assert CONTRATOS["clientes"].sensibles()["numero_documento"] == PII_ALTA
    assert "referencia_bancaria" in CONTRATOS["recaudos"].sensibles()