"""Pruebas de las funciones de normalización, fuera del runtime de Databricks.

    pip install pyspark==4.0.1 pytest
    pytest tests/

Corren con Spark local y ANSI activo, igual que serverless.
"""

import sys
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "electrocasa"))
from utils import normalizacion as N  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("tests")
         .config("spark.sql.ansi.enabled", "true").config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()


def _valores(spark, columna, entradas):
    df = spark.createDataFrame([(v,) for v in entradas], "x string")
    return [r[0] for r in df.select(columna).collect()]


def test_fecha_flexible_tres_formatos(spark):
    res = _valores(spark, N.fecha_flexible("x"), ["2026-06-07", "07/06/2026", "2026-06-07T14:30:00", "", "basura", None])
    assert [str(r) if r else None for r in res] == ["2026-06-07"] * 3 + [None] * 3


def test_monto_con_coma_decimal(spark):
    res = _valores(spark, N.monto_decimal("x"), ["1.234,50", "407,98", "6792.35", "", "-1602.5"])
    assert res == [Decimal("1234.50"), Decimal("407.98"), Decimal("6792.35"), None, Decimal("-1602.50")]


def test_precio_con_moneda(spark):
    res = _valores(spark, N.precio_decimal("x"), ["S/ 1,299.90", "3107.28", "S/ 999.99"])
    assert res == [Decimal("1299.90"), Decimal("3107.28"), Decimal("999.99")]


@pytest.mark.parametrize("entrada,esperado", [
    ("Línea blanca", "linea_blanca"), ("LINEA BLANCA", "linea_blanca"), ("linea_blanca", "linea_blanca"),
    ("Climatización", "climatizacion"), ("CLIMATIZACION", "climatizacion"), ("Electrónica", "electronica"),
])
def test_categorias(spark, entrada, esperado):
    assert _valores(spark, N.mapear(N.clave_texto("x"), N.CATEGORIAS), [entrada]) == [esperado]


@pytest.mark.parametrize("entrada,esperado", [
    ("TC", "tarjeta"), ("td", "tarjeta"), ("Tarjeta de credito", "tarjeta"), ("EFV", "efectivo"),
    ("TRF", "transferencia"), ("PLIN", "plin"), ("yape", "yape"), ("bitcoin", None),
])
def test_metodos_pago(spark, entrada, esperado):
    assert _valores(spark, N.mapear(N.clave_texto("x"), N.METODOS_PAGO), [entrada]) == [esperado]


@pytest.mark.parametrize("entrada,esperado", [
    ("Cambio Salario", "cambio_salario"), ("CAMBIO_SALARIO", "cambio_salario"),
    ("cese", "baja"), ("BAJA", "baja"), ("Alta", "alta"), ("TRANSFERENCIA", "transferencia"),
])
def test_tipos_evento(spark, entrada, esperado):
    assert _valores(spark, N.mapear(N.clave_texto("x"), N.TIPOS_EVENTO), [entrada]) == [esperado]


@pytest.mark.parametrize("entrada,esperado", [
    ("En camino", "en_transito"), ("EN_CAMINO", "en_transito"), ("en_transito", "en_transito"),
    ("ENTREGADO", "entregado"), ("Devuelto", "devuelto"), ("Pendiente", "pendiente"),
])
def test_estados_entrega(spark, entrada, esperado):
    assert _valores(spark, N.mapear(N.clave_texto("x"), N.ESTADOS_ENTREGA), [entrada]) == [esperado]
