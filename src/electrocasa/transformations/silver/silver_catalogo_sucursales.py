"""Silver de Catálogo y Sucursales.

Las dos fuentes llegan por COPY INTO (fuera del pipeline) a tablas Delta de Bronze.
El pipeline las lee en batch desde una Materialized View: COPY INTO solo agrega filas,
y un snapshot completo del catálogo se procesa entero en cada corrida.
"""

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

from utils import calidad, config
from utils import normalizacion as N


# ---------------------------------------------------------------------------
# Catálogo
# ---------------------------------------------------------------------------

@dp.temporary_view(name="catalogo_preparado")
def catalogo_preparado():
    """Un producto por fila, con los datos del último snapshot en que aparece.

    - snapshot = fecha del nombre del archivo (catalogo_productos_2026-08-01.json).
    - En el snapshot de marzo hay productos repetidos con precios distintos: dentro del
      mismo snapshot gana la fila con precio válido y, entre válidas, el precio mayor
      (decisión documentada en contratos/catalogo.yml).
    - Los descontinuados (activo = false) se conservan marcados: siguen teniendo ventas
      y reseñas históricas que necesitan su categoría.
    """
    bronze = spark.read.table(config.tabla("bronze", "catalogo"))
    columnas_raw = [c for c in bronze.columns if not c.startswith("_")]
    df = (
        bronze
        .withColumn("_raw", F.struct(*columnas_raw))
        .select(
            N.texto_limpio("producto_id").alias("producto_id"),
            N.texto_limpio("nombre_producto").alias("nombre_producto"),
            N.mapear(N.clave_texto("categoria"), N.CATEGORIAS).alias("categoria"),
            N.texto_limpio("marca").alias("marca"),
            N.precio_decimal("precio_lista").alias("precio_lista"),
            F.lower(F.trim("activo")).try_cast("boolean").alias("activo"),
            F.to_date(F.col("_lote")).alias("fecha_snapshot"),
            "_raw", "_archivo_origen", "_lote", "_fecha_ingesta",
        )
    )
    precio_ok = F.when(F.col("precio_lista") > 0, 1).otherwise(0)
    w = Window.partitionBy("producto_id").orderBy(
        F.col("fecha_snapshot").desc(), precio_ok.desc(), F.col("precio_lista").desc_nulls_last()
    )
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


@dp.materialized_view(
    name=config.tabla("silver", "catalogo"),
    comment="Maestro de productos: un producto por fila con los datos de su último snapshot. Incluye descontinuados (activo = false).",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(calidad.reglas_drop("catalogo"))
@dp.expect_all(calidad.reglas_warn("catalogo"))
def silver_catalogo():
    return spark.read.table("catalogo_preparado").select(
        "producto_id", "nombre_producto", "categoria", "marca", "precio_lista",
        "activo", "fecha_snapshot",
        F.col("_archivo_origen").alias("archivo_origen"),
        F.col("_fecha_ingesta").alias("fecha_ingesta"),
    )


# ---------------------------------------------------------------------------
# Sucursales
# ---------------------------------------------------------------------------

@dp.temporary_view(name="sucursales_preparadas")
def sucursales_preparadas():
    bronze = spark.read.table(config.tabla("bronze", "sucursales"))
    columnas_raw = [c for c in bronze.columns if not c.startswith("_")]
    df = (
        bronze
        .withColumn("_raw", F.struct(*columnas_raw))
        .select(
            N.texto_limpio("sucursal_id").alias("sucursal_id"),
            N.texto_limpio("nombre_sucursal").alias("nombre_sucursal"),
            N.texto_limpio("distrito").alias("distrito"),
            F.initcap(F.lower(N.texto_limpio("ciudad"))).alias("ciudad"),   # 'AREQUIPA' -> 'Arequipa'
            N.texto_limpio("region").alias("region"),
            N.fecha_flexible("fecha_apertura").alias("fecha_apertura"),
            "_raw", "_archivo_origen", "_lote", "_fecha_ingesta",
        )
    )
    w = Window.partitionBy("sucursal_id").orderBy(F.col("_fecha_ingesta").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


@dp.materialized_view(
    name=config.tabla("silver", "sucursales"),
    comment="Tabla de referencia de las 40 sucursales, con ciudad en casing uniforme.",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(calidad.reglas_drop("sucursales"))
@dp.expect_all(calidad.reglas_warn("sucursales"))
def silver_sucursales():
    return spark.read.table("sucursales_preparadas").select(
        "sucursal_id", "nombre_sucursal", "distrito", "ciudad", "region", "fecha_apertura"
    )
