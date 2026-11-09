from src.common.config import PipelineConfig
from src.common.gobierno import _q, entidades_con_sensibles, sql_documentacion, sql_vista_protegida

CFG = PipelineConfig(fecha_corte="2026-11-05", run_id="t")


def test_vista_enmascara_solo_columnas_sensibles():
    sql = sql_vista_protegida(CFG, "clientes")
    assert "fn_mask_ultimos(numero_documento, 4)" in sql
    assert "fn_mask_correo(correo)" in sql
    assert "year(fecha_nacimiento)" in sql
    assert "is_account_group_member('grp_datos_sensibles')" in sql
    assert "\n  ciudad,\n" in sql                      # columna interna: sin máscara
    assert set(entidades_con_sensibles()) == {"clientes", "recaudos"}


def test_documentacion_escapa_comillas_y_etiqueta_pii():
    assert _q("cliente's") == "cliente\\'s"
    sentencias = sql_documentacion(CFG)
    assert any("silver_clientes ALTER COLUMN numero_documento SET TAGS ('clasificacion' = 'pii_alta')" in s
               for s in sentencias)
    assert not any("ciudad SET TAGS" in s for s in sentencias)