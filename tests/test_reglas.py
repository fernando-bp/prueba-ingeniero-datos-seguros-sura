import pytest

from src.common.config import PipelineConfig
from src.quality.reglas import Regla, aplicar_reglas, resumen_calidad, separar, verificar_umbrales

REGLAS = [
    Regla("ID_NULO", "completitud", "BLOQUEO", "CUARENTENA", "id obligatorio", "id IS NULL", 50.0),
    Regla("MONTO_NEG", "validez", "BLOQUEO", "CUARENTENA", "monto >= 0", "monto < 0", 10.0),
    Regla("SIN_PADRE", "integridad", "ADVERTENCIA", "MARCAR", "padre existe", "NOT existe_padre", 10.0),
]


@pytest.fixture
def df(spark):
    return spark.createDataFrame(
        [("A", 10.0, True), (None, 5.0, True), ("C", -1.0, False), ("D", 3.0, False), ("E", None, True)],
        ["id", "monto", "existe_padre"],
    )


def test_separa_cuarentena_y_marcados(df):
    acept, rech = separar(aplicar_reglas(df, REGLAS))
    assert {r.id for r in acept.collect()} == {"A", "D", "E"}       # D solo se MARCA; E monto null no es falla
    assert sorted(r.id or "-" for r in rech.collect()) == ["-", "C"]
    d = acept.filter("id = 'D'").first()
    assert d._fallas == ["SIN_PADRE"]
    assert acept.count() + rech.count() == df.count()               # reconciliación: nada se pierde


def test_resumen_y_umbral_bloqueante(df):
    cfg = PipelineConfig(fecha_corte="2026-11-05", run_id="test")
    res = {r.regla: r for r in resumen_calidad(aplicar_reglas(df, REGLAS), REGLAS, "demo", cfg).collect()}
    assert res["ID_NULO"].fallidos == 1 and res["ID_NULO"].resultado == "OK_CON_FALLAS"   # 20% <= 50%
    assert res["MONTO_NEG"].resultado == "FALLA"                                          # 20% > 10%
    assert res["SIN_PADRE"].resultado == "ALERTA"                                         # 40% > 10%
    with pytest.raises(ValueError, match="MONTO_NEG"):
        verificar_umbrales(resumen_calidad(aplicar_reglas(df, REGLAS), REGLAS, "demo", cfg), "demo")