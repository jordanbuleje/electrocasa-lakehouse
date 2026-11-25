"""Silver de Reseñas y Tracking de envíos."""

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

from utils import calidad, config
from utils import normalizacion as N


# ---------------------------------------------------------------------------
# Reseñas
# ---------------------------------------------------------------------------

@dp.temporary_view(name="resenas_preparadas")
def resenas_preparadas():
    bronze = spark.read.table(config.tabla("bronze", "resenas"))
    productos = spark.read.table(config.tabla("silver", "catalogo")).select("producto_id").withColumn("_en_cat", F.lit(True))
    columnas_raw = [c for c in bronze.columns if not c.startswith("_")]
    df = (
        bronze
        .withColumn("_raw", F.struct(*columnas_raw))
        .select(
            N.texto_limpio("resena_id").alias("resena_id"),
            N.texto_limpio("producto_id").alias("producto_id"),
            N.texto_limpio("cliente_id").alias("cliente_id"),
            N.entero("calificacion").alias("calificacion"),
            N.texto_limpio("comentario").alias("comentario"),
            F.coalesce(F.col("tags"), F.array().cast("array<string>")).alias("tags"),   # null -> []
            F.transform(
                F.coalesce(F.col("respuestas"), F.array().cast("array<struct<autor:string,texto:string,fecha:string>>")),
                lambda r: F.struct(
                    F.coalesce(r["autor"], F.lit("no_informado")).alias("autor"),
                    r["texto"].alias("texto"),
                    r["fecha"].alias("fecha"),
                ),
            ).alias("respuestas"),
            N.fecha_flexible("fecha_resena").alias("fecha_resena"),
            F.col("metadata"),
            F.get_json_object("metadata", "$.origen").alias("origen"),
            "_raw", "_archivo_origen", "_lote", "_fecha_ingesta",
        )
    )
    w = Window.partitionBy("resena_id").orderBy("_fecha_ingesta", "_archivo_origen")
    df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    return (
        df.join(productos, "producto_id", "left")
          .withColumn("producto_en_catalogo", F.coalesce("_en_cat", F.lit(False)))
          .drop("_en_cat")
    )


@dp.materialized_view(
    name=config.tabla("silver", "resenas"),
    comment="Reseñas válidas (calificación 1-5 y producto existente), deduplicadas por resena_id.",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(calidad.reglas_drop("resenas"))
@dp.expect_all(calidad.reglas_warn("resenas"))
def silver_resenas():
    return spark.read.table("resenas_preparadas").select(
        "resena_id", "producto_id", "cliente_id", "calificacion", "comentario",
        "tags", "respuestas", "fecha_resena", "origen", "metadata", "producto_en_catalogo",
        F.col("_archivo_origen").alias("archivo_origen"),
        F.col("_lote").alias("lote"),
        F.col("_fecha_ingesta").alias("fecha_ingesta"),
    )


# ---------------------------------------------------------------------------
# Tracking de envíos (Bronze = tabla destino del conector de Lakeflow Connect)
# ---------------------------------------------------------------------------

@dp.temporary_view(name="tracking_preparado")
def tracking_preparado():
    """Estandariza estado y courier y elimina mensajes reprocesados.

    Mensaje reprocesado = el mismo cambio de estado del mismo envío registrado dos
    veces con distinto evento_id. Se conserva el primero (menor fecha_modificacion).
    Las métricas de Gold usan el primer 'pendiente', el primer 'entregado' y si hubo
    algún 'devuelto', así que quedarse con el primero no cambia ningún resultado.
    """
    bronze = spark.read.table(config.tabla_tracking())
    ventas_ecom = (
        spark.read.table(config.tabla("silver", "ventas"))
        .select("venta_id").withColumn("_en_ventas", F.lit(True))
    )
    columnas_raw = [c for c in bronze.columns if not c.startswith("_")]
    df = (
        bronze
        .withColumn("_raw", F.struct(*columnas_raw))
        .select(
            F.col("evento_id").cast("bigint").alias("evento_id"),
            F.trim(F.col("tracking_id").cast("string")).alias("tracking_id"),
            F.trim(F.col("venta_id").cast("string")).alias("venta_id"),
            F.initcap(N.clave_texto("courier")).alias("courier"),
            N.mapear(N.clave_texto("estado_entrega"), N.ESTADOS_ENTREGA).alias("estado_entrega"),
            N.clave_texto("estado_entrega").alias("estado_origen"),
            F.trim(F.col("sucursal_origen").cast("string")).alias("sucursal_origen"),
            F.col("fecha_actualizacion").cast("date").alias("fecha_actualizacion"),
            F.col("fecha_modificacion").cast("timestamp").alias("fecha_modificacion"),
            "_raw",
            F.lit("dbo.TrackingEnvios").alias("_archivo_origen"),
            F.lit("conector_lakeflow").alias("_lote"),
            F.current_timestamp().alias("_fecha_ingesta"),
        )
    )
    w = Window.partitionBy("tracking_id", "estado_entrega").orderBy("fecha_modificacion", "evento_id")
    df = (
        df.withColumn("_rn", F.row_number().over(w))
          .withColumn("es_reprocesado", (F.col("_rn") > 1) & F.col("estado_entrega").isNotNull())
          .drop("_rn")
    )
    return (
        df.join(ventas_ecom, "venta_id", "left")
          .withColumn("venta_en_ventas", F.coalesce("_en_ventas", F.lit(False)))
          .drop("_en_ventas")
    )


@dp.materialized_view(
    name=config.tabla("silver", "tracking_envios"),
    comment="Eventos de tracking con estado y courier estandarizados, sin mensajes reprocesados.",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(calidad.reglas_drop("tracking"))
@dp.expect_all(calidad.reglas_warn("tracking"))
def silver_tracking_envios():
    return (
        spark.read.table("tracking_preparado")
        .filter(~F.col("es_reprocesado"))
        .select(
            "evento_id", "tracking_id", "venta_id", "courier", "estado_entrega",
            "sucursal_origen", "fecha_actualizacion", "fecha_modificacion", "venta_en_ventas",
        )
    )
