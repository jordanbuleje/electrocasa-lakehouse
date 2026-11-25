"""Gold: una tabla por pregunta de negocio (período marzo a octubre de 2026).

Todas leen Silver, es decir, datos ya limpios y deduplicados: los registros
rechazados (silver.cuarentena con politica = 'drop') no cuentan.
"""

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

from utils import config

PROPS = {"quality": "gold"}


def _en_periodo(c: str):
    return F.col(c).between(F.lit(config.INICIO_PERIODO).cast("date"), F.lit(config.FIN_PERIODO).cast("date"))


def _mes(c: str):
    return F.date_trunc("month", F.col(c)).cast("date")


def _sucursales():
    return spark.read.table(config.tabla("silver", "sucursales")).select(
        "sucursal_id", "nombre_sucursal", "ciudad", "region"
    )


# ---------------------------------------------------------------------------
# P1. Ventas netas por sucursal y mes
# ---------------------------------------------------------------------------

@dp.materialized_view(
    name=config.tabla("gold", "ventas_netas_sucursal_mes"),
    comment="P1. Ventas, devoluciones, ventas netas, cantidad de ventas y ticket promedio por sucursal y mes (ambos canales).",
    table_properties=PROPS,
)
def gold_ventas_netas_sucursal_mes():
    ventas = (
        spark.read.table(config.tabla("silver", "ventas"))
        .filter(_en_periodo("fecha_venta"))
        .groupBy("sucursal_id", _mes("fecha_venta").alias("mes"))
        .agg(
            F.sum("monto_total").alias("ventas"),
            F.countDistinct("venta_id").alias("cantidad_ventas"),
        )
    )
    devoluciones = (
        spark.read.table(config.tabla("silver", "devoluciones"))
        .filter(_en_periodo("fecha_devolucion"))
        .groupBy("sucursal_id", _mes("fecha_devolucion").alias("mes"))
        .agg(F.sum("monto_reembolso").alias("devoluciones"))
    )
    return (
        ventas.join(devoluciones, ["sucursal_id", "mes"], "full_outer")
        .join(_sucursales(), "sucursal_id", "left")
        .select(
            "sucursal_id", "nombre_sucursal", "ciudad", "region", "mes",
            F.coalesce("ventas", F.lit(0)).cast("decimal(14,2)").alias("ventas"),
            F.coalesce("devoluciones", F.lit(0)).cast("decimal(14,2)").alias("devoluciones"),
            (F.coalesce("ventas", F.lit(0)) - F.coalesce("devoluciones", F.lit(0))).cast("decimal(14,2)").alias("ventas_netas"),
            F.coalesce("cantidad_ventas", F.lit(0)).alias("cantidad_ventas"),
            F.round(F.col("ventas") / F.col("cantidad_ventas"), 2).cast("decimal(14,2)").alias("ticket_promedio"),
        )
    )


# ---------------------------------------------------------------------------
# P2. Productos: top 10 por unidades y top 10 por tasa de devolución
# ---------------------------------------------------------------------------

def _productos():
    return spark.read.table(config.tabla("silver", "catalogo")).select(
        "producto_id", "nombre_producto", "categoria", "marca"
    )


@dp.materialized_view(
    name=config.tabla("gold", "top_productos_unidades"),
    comment="P2a. Top 10 de productos más vendidos por unidades en todo el período.",
    table_properties=PROPS,
)
def gold_top_productos_unidades():
    agg = (
        spark.read.table(config.tabla("silver", "ventas"))
        .filter(_en_periodo("fecha_venta"))
        .groupBy("producto_id")
        .agg(F.sum("cantidad").alias("unidades"), F.countDistinct("venta_id").alias("cantidad_ventas"))
    )
    w = Window.orderBy(F.col("unidades").desc(), F.col("cantidad_ventas").desc(), F.col("producto_id"))
    return (
        agg.withColumn("ranking", F.row_number().over(w))
        .filter("ranking <= 10")
        .join(_productos(), "producto_id", "left")
        .select("ranking", "producto_id", "nombre_producto", "categoria", "marca", "unidades", "cantidad_ventas")
    )


@dp.materialized_view(
    name=config.tabla("gold", "top_productos_tasa_devolucion"),
    comment="P2b. Top 10 de productos con mayor tasa de devolución = devoluciones del producto / ventas del producto.",
    table_properties=PROPS,
)
def gold_top_productos_tasa_devolucion():
    ventas = (
        spark.read.table(config.tabla("silver", "ventas"))
        .filter(_en_periodo("fecha_venta"))
        .groupBy("producto_id").agg(F.countDistinct("venta_id").alias("cantidad_ventas"))
    )
    devoluciones = (
        spark.read.table(config.tabla("silver", "devoluciones"))
        .filter(_en_periodo("fecha_devolucion"))
        .groupBy("producto_id").agg(F.countDistinct("devolucion_id").alias("cantidad_devoluciones"))
    )
    agg = (
        ventas.join(devoluciones, "producto_id", "left")
        .withColumn("cantidad_devoluciones", F.coalesce("cantidad_devoluciones", F.lit(0)))
        .withColumn("tasa_devolucion", F.round(F.col("cantidad_devoluciones") / F.col("cantidad_ventas"), 4))
    )
    w = Window.orderBy(F.col("tasa_devolucion").desc(), F.col("cantidad_devoluciones").desc(), F.col("producto_id"))
    return (
        agg.withColumn("ranking", F.row_number().over(w))
        .filter("ranking <= 10")
        .join(_productos(), "producto_id", "left")
        .select("ranking", "producto_id", "nombre_producto", "categoria", "marca",
                "cantidad_ventas", "cantidad_devoluciones", "tasa_devolucion")
    )


# ---------------------------------------------------------------------------
# P3. Dotación: empleados activos por sucursal al último día de cada mes
# ---------------------------------------------------------------------------

@dp.materialized_view(
    name=config.tabla("gold", "dotacion_sucursal_mes"),
    comment="P3. Empleados activos por sucursal al cierre de cada mes (marzo a octubre), según la historia SCD2 de silver.empleados.",
    table_properties=PROPS,
)
def gold_dotacion_sucursal_mes():
    cierres = spark.sql(
        f"SELECT last_day(m) AS fecha_cierre FROM (SELECT explode(sequence("
        f"DATE'{config.INICIO_PERIODO}', DATE'{config.FIN_PERIODO}', INTERVAL 1 MONTH)) AS m)"
    )
    historia = spark.read.table(config.tabla("silver", "empleados")).select(
        "id_empleado", "sucursal_id", "valido_desde", "valido_hasta"
    )
    # Activo en la fecha d: la versión empezó en d o antes y no terminó hasta d.
    # Una baja en d cierra la versión con valido_hasta = d, así que ese día ya no cuenta.
    activos = historia.join(
        cierres,
        (F.col("valido_desde") <= F.col("fecha_cierre"))
        & (F.col("valido_hasta").isNull() | (F.col("valido_hasta") > F.col("fecha_cierre"))),
    )
    return (
        activos.groupBy("sucursal_id", "fecha_cierre")
        .agg(F.countDistinct("id_empleado").alias("empleados_activos"))
        .join(_sucursales(), "sucursal_id", "left")
        .select(_mes("fecha_cierre").alias("mes"), "fecha_cierre", "sucursal_id",
                "nombre_sucursal", "ciudad", "region", "empleados_activos")
    )


# ---------------------------------------------------------------------------
# P4. Ventas por vendedor (tienda física), con la sucursal vigente en la fecha de venta
# ---------------------------------------------------------------------------

@dp.materialized_view(
    name=config.tabla("gold", "ventas_vendedor_sucursal_mes"),
    comment=(
        "P4. Ventas de tienda física por vendedor, sucursal y mes. La sucursal es la que tenía "
        "el vendedor en la fecha de la venta (SCD2); sin versión vigente -> SIN_ASIGNAR."
    ),
    table_properties=PROPS,
)
def gold_ventas_vendedor_sucursal_mes():
    ventas = (
        spark.read.table(config.tabla("silver", "ventas"))
        .filter((F.col("canal") == "tienda_fisica") & _en_periodo("fecha_venta"))
        .select("venta_id", "vendedor_id", "monto_total", "fecha_venta")
    )
    historia = spark.read.table(config.tabla("silver", "empleados")).select(
        F.col("id_empleado"), F.col("sucursal_id").alias("sucursal_vendedor"), "valido_desde", "valido_hasta"
    )
    con_sucursal = ventas.join(
        historia,
        (F.col("vendedor_id") == F.col("id_empleado"))
        & (F.col("valido_desde") <= F.col("fecha_venta"))
        & (F.col("valido_hasta").isNull() | (F.col("valido_hasta") > F.col("fecha_venta"))),
        "left",
    ).withColumn("sucursal_id", F.coalesce("sucursal_vendedor", F.lit("SIN_ASIGNAR")))
    return (
        con_sucursal.groupBy("vendedor_id", "sucursal_id", _mes("fecha_venta").alias("mes"))
        .agg(
            F.sum("monto_total").cast("decimal(14,2)").alias("monto_ventas"),
            F.countDistinct("venta_id").alias("cantidad_ventas"),
        )
        .join(_sucursales(), "sucursal_id", "left")
        .select("vendedor_id", "sucursal_id", "nombre_sucursal", "mes", "monto_ventas", "cantidad_ventas")
    )


# ---------------------------------------------------------------------------
# P5. Reseñas negativas por categoría y mes
# ---------------------------------------------------------------------------

@dp.materialized_view(
    name=config.tabla("gold", "resenas_negativas_categoria_mes"),
    comment="P5. % de reseñas negativas (calificación 1 o 2) por categoría (último snapshot del catálogo) y mes de la reseña.",
    table_properties=PROPS,
)
def gold_resenas_negativas_categoria_mes():
    resenas = spark.read.table(config.tabla("silver", "resenas")).filter(_en_periodo("fecha_resena"))
    return (
        resenas.join(_productos().select("producto_id", "categoria"), "producto_id", "inner")
        .groupBy("categoria", _mes("fecha_resena").alias("mes"))
        .agg(
            F.count("*").alias("total_resenas"),
            F.sum(F.when(F.col("calificacion") <= 2, 1).otherwise(0)).alias("resenas_negativas"),
        )
        .withColumn("pct_negativas", F.round(100 * F.col("resenas_negativas") / F.col("total_resenas"), 2))
    )


# ---------------------------------------------------------------------------
# P6. Despachos por courier
# ---------------------------------------------------------------------------

@dp.materialized_view(
    name=config.tabla("gold", "despachos_courier"),
    comment=(
        "P6. Por courier: horas promedio entre el primer 'pendiente' y el primer 'entregado' "
        "(solo envíos entregados) y % de envíos devueltos sobre envíos en estado final."
    ),
    table_properties=PROPS,
)
def gold_despachos_courier():
    eventos = spark.read.table(config.tabla("silver", "tracking_envios"))
    por_envio = eventos.groupBy("tracking_id").agg(
        F.min_by("courier", "fecha_modificacion").alias("courier"),
        F.min(F.when(F.col("estado_entrega") == "pendiente", F.col("fecha_modificacion"))).alias("primer_pendiente"),
        F.min(F.when(F.col("estado_entrega") == "entregado", F.col("fecha_modificacion"))).alias("primer_entregado"),
        F.max(F.when(F.col("estado_entrega") == "devuelto", 1).otherwise(0)).alias("fue_devuelto"),
    )
    por_envio = (
        por_envio
        .withColumn("horas_entrega",
                    (F.unix_timestamp("primer_entregado") - F.unix_timestamp("primer_pendiente")) / 3600)
        .withColumn("estado_final", (F.col("primer_entregado").isNotNull() | (F.col("fue_devuelto") == 1)).cast("int"))
    )
    return (
        por_envio.groupBy("courier")
        .agg(
            F.countDistinct("tracking_id").alias("envios"),
            F.sum(F.col("primer_entregado").isNotNull().cast("int")).alias("envios_entregados"),
            F.round(F.avg(F.when(F.col("primer_entregado").isNotNull() & F.col("primer_pendiente").isNotNull(),
                                 F.col("horas_entrega"))), 2).alias("horas_promedio_entrega"),
            F.sum("fue_devuelto").alias("envios_devueltos"),
            F.sum("estado_final").alias("envios_en_estado_final"),
        )
        .withColumn("pct_devueltos", F.round(100 * F.col("envios_devueltos") / F.col("envios_en_estado_final"), 2))
    )


# ---------------------------------------------------------------------------
# DIAGNÓSTICO TEMPORAL (borrar después de corregir)
# ---------------------------------------------------------------------------

@dp.materialized_view(
    name=config.tabla("gold", "diag_ventas"),
    table_properties=PROPS,
)
def gold_diag_ventas():
    base = spark.read.table(config.tabla("silver", "ventas"))
    return base.agg(
        F.count("*").alias("total"),
        F.count(F.when(_en_periodo("fecha_venta"), 1)).alias("en_periodo"),
        F.count("sucursal_id").alias("con_sucursal"),
        F.countDistinct("sucursal_id").alias("sucursales_distintas"),
        F.min("fecha_venta").cast("string").alias("min_fecha"),
        F.max("fecha_venta").cast("string").alias("max_fecha"),
    )