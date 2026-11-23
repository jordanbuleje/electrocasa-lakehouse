"""Silver de Ventas y Devoluciones.

Patrón por fuente:
  1. vista temporal *_preparadas: normaliza tipos y valores, deduplica reingestas y
     marca las referencias huérfanas. Guarda la fila original de Bronze en _raw.
  2. tabla Silver: lee la vista y aplica las expectativas de utils/calidad.py
     (drop para reglas críticas, warn para advertencias).
  3. silver.cuarentena (silver_cuarentena.py) lee la misma vista y guarda lo que violó
     alguna regla, con la regla, la fuente, el lote y la fecha de proceso.

Silver usa Materialized Views porque las reglas necesitan ver toda la historia:
la deduplicación de reingestas y la validación contra Ventas/Catálogo comparan
filas de lotes distintos.
"""

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

from utils import calidad, config
from utils import normalizacion as N


def _ultima_por(df, llave: str):
    """Deduplica reingestas: una fila por llave, la primera que llegó."""
    w = Window.partitionBy(llave).orderBy(F.col("_fecha_ingesta"), F.col("_archivo_origen"))
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


# ---------------------------------------------------------------------------
# Ventas
# ---------------------------------------------------------------------------

@dp.temporary_view(name="ventas_preparadas")
def ventas_preparadas():
    bronze = spark.read.table(config.tabla("bronze", "ventas"))
    productos = spark.read.table(config.tabla("silver", "catalogo")).select("producto_id").withColumn("_en_cat", F.lit(True))
    columnas_raw = [c for c in bronze.columns if not c.startswith("_")]
    df = (
        bronze
        .withColumn("_raw", F.struct(*columnas_raw))
        .select(
            N.texto_limpio("venta_id").alias("venta_id"),
            N.texto_limpio("sucursal_id").alias("sucursal_id"),
            N.texto_limpio("producto_id").alias("producto_id"),
            N.texto_limpio("vendedor_id").alias("vendedor_id"),
            N.entero("cantidad").alias("cantidad"),
            N.monto_decimal("monto_total").alias("monto_total"),
            N.mapear(N.clave_texto("metodo_pago"), N.METODOS_PAGO).alias("metodo_pago"),
            N.fecha_flexible("fecha_venta").alias("fecha_venta"),
            N.mapear(N.clave_texto("canal"), N.CANALES).alias("canal"),
            "_raw", "_archivo_origen", "_lote", "_fecha_ingesta",
        )
    )
    df = _ultima_por(df, "venta_id")
    return (
        df.join(productos, "producto_id", "left")
          .withColumn("producto_en_catalogo", F.coalesce("_en_cat", F.lit(False)))
          .drop("_en_cat")
    )


@dp.materialized_view(
    name=config.tabla("silver", "ventas"),
    comment="Ventas limpias y deduplicadas (una fila por venta_id). Ver contratos/ventas.yml.",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(calidad.reglas_drop("ventas"))
@dp.expect_all(calidad.reglas_warn("ventas"))
def silver_ventas():
    return spark.read.table("ventas_preparadas").select(
        "venta_id", "sucursal_id", "producto_id", "vendedor_id", "cantidad",
        "monto_total", "metodo_pago", "fecha_venta", "canal", "producto_en_catalogo",
        F.col("_archivo_origen").alias("archivo_origen"),
        F.col("_lote").alias("lote"),
        F.col("_fecha_ingesta").alias("fecha_ingesta"),
    )


# ---------------------------------------------------------------------------
# Devoluciones
# ---------------------------------------------------------------------------

@dp.temporary_view(name="devoluciones_preparadas")
def devoluciones_preparadas():
    """Una expectativa evalúa una fila a la vez: por eso el join con Ventas se hace
    aquí, y la regla 'reembolso_valido' se declara sobre esta vista."""
    bronze = spark.read.table(config.tabla("bronze", "devoluciones"))
    ventas = spark.read.table(config.tabla("silver", "ventas")).select(
        "venta_id", F.col("monto_total").alias("monto_venta")
    )
    columnas_raw = [c for c in bronze.columns if not c.startswith("_")]
    df = (
        bronze
        .withColumn("_raw", F.struct(*columnas_raw))
        .select(
            N.texto_limpio("devolucion_id").alias("devolucion_id"),
            N.texto_limpio("venta_id").alias("venta_id"),
            N.texto_limpio("sucursal_id").alias("sucursal_id"),
            N.texto_limpio("producto_id").alias("producto_id"),
            F.coalesce(N.texto_limpio("motivo"), F.lit("no_informado")).alias("motivo"),
            N.monto_decimal("monto_reembolso").alias("monto_reembolso"),
            N.fecha_flexible("fecha_devolucion").alias("fecha_devolucion"),
            "_raw", "_archivo_origen", "_lote", "_fecha_ingesta",
        )
    )
    return _ultima_por(df, "devolucion_id").join(ventas, "venta_id", "left")


@dp.materialized_view(
    name=config.tabla("silver", "devoluciones"),
    comment="Devoluciones válidas: venta de origen existente y reembolso entre 0 y el monto de la venta.",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(calidad.reglas_drop("devoluciones"))
@dp.expect_all(calidad.reglas_warn("devoluciones"))
def silver_devoluciones():
    return spark.read.table("devoluciones_preparadas").select(
        "devolucion_id", "venta_id", "sucursal_id", "producto_id", "motivo",
        "monto_reembolso", "monto_venta", "fecha_devolucion",
        F.col("_archivo_origen").alias("archivo_origen"),
        F.col("_lote").alias("lote"),
        F.col("_fecha_ingesta").alias("fecha_ingesta"),
    )
