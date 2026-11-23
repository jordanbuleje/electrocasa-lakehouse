"""Bronze con Auto Loader: ventas, empleados, devoluciones y reseñas.

Estas cuatro fuentes llegan por lotes incrementales a su carpeta del volume, y cada
lote trae solo filas nuevas (más algunas reingestas). Auto Loader recuerda qué archivos
ya leyó, así que cada corrida procesa únicamente los archivos nuevos.

- Sin lógica de negocio: los valores se guardan tal como llegan (texto).
- Columnas técnicas de auditoría: archivo de origen, lote y momento de ingesta.
- El checkpoint y la ubicación del esquema los gestiona el pipeline (no se configuran rutas).
- Sin particionamiento por fecha de negocio.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utils import config, esquemas


def _auditoria(df):
    """Columnas técnicas. _metadata.file_name en vez de input_file_name() (deprecado)."""
    return (
        df.withColumn("_archivo_origen", F.col("_metadata.file_name"))
          .withColumn("_lote", F.regexp_extract(F.col("_metadata.file_name"), r"(lote_\d+)", 1))
          .withColumn("_fecha_ingesta", F.current_timestamp())
    )


def _auto_loader_csv(fuente: str, esquema):
    return _auditoria(
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(esquema)
        .load(config.landing(fuente))
    )


PROPIEDADES = {"quality": "bronze", "delta.appendOnly": "true"}


@dp.table(
    name=config.tabla("bronze", "ventas"),
    comment="Ventas crudas de las 40 sucursales (tienda física y ecommerce). Auto Loader, CSV por lote.",
    table_properties=PROPIEDADES,
)
def bronze_ventas():
    return _auto_loader_csv("ventas", esquemas.VENTAS)


@dp.table(
    name=config.tabla("bronze", "empleados"),
    comment="Eventos crudos de RR.HH. (altas, transferencias, cambios de salario, bajas). Un empleado aparece una vez por evento.",
    table_properties=PROPIEDADES,
)
def bronze_empleados():
    return _auto_loader_csv("empleados", esquemas.EMPLEADOS)


@dp.table(
    name=config.tabla("bronze", "devoluciones"),
    comment="Devoluciones crudas con la venta de origen, el motivo y el monto reembolsado.",
    table_properties=PROPIEDADES,
)
def bronze_devoluciones():
    return _auto_loader_csv("devoluciones", esquemas.DEVOLUCIONES)


@dp.table(
    name=config.tabla("bronze", "resenas"),
    comment="Reseñas crudas (JSON Lines). metadata se guarda como texto JSON porque su estructura varía según el origen.",
    table_properties=PROPIEDADES,
)
def bronze_resenas():
    return _auditoria(
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(esquemas.RESENAS)
        .load(config.landing("resenas"))
    )
