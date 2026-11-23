"""silver.cuarentena: todo lo que violó una regla de calidad, de todas las fuentes.

Reto "Cuarentena con trazabilidad completa". Cada fila guarda:
  fuente, llave del registro, política (drop = no llegó a Silver, warn = llegó con
  advertencia), reglas que fallaron, archivo y lote de origen, momento de ingesta,
  momento de proceso y la fila original de Bronze como JSON (registro).

Reprocesar: se corrige el archivo de origen (o se reescribe la fila desde `registro`),
se deja en la carpeta del volume como un lote nuevo y se vuelve a correr el job.
Como Silver deduplica por llave y es una Materialized View, la fila corregida entra
y la fila rechazada desaparece de la cuarentena en la siguiente actualización.

Las reglas son las mismas que usan las expectativas (utils/calidad.py), así que los
conteos de esta tabla coinciden con las métricas de expectations del event log.
"""

from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utils import calidad, config

FUENTES = [
    # (fuente de reglas, vista preparada, llave)
    ("ventas", "ventas_preparadas", "venta_id"),
    ("devoluciones", "devoluciones_preparadas", "devolucion_id"),
    ("catalogo", "catalogo_preparado", "producto_id"),
    ("sucursales", "sucursales_preparadas", "sucursal_id"),
    ("empleados", "empleados_preparados", "id_empleado"),
    ("resenas", "resenas_preparadas", "resena_id"),
    ("tracking", "tracking_preparado", "evento_id"),
]


@dp.materialized_view(
    name=config.tabla("silver", "cuarentena"),
    comment="Registros rechazados (drop) o con advertencias (warn) por fuente, con la regla que falló, el lote y la fila original.",
    table_properties={"quality": "silver"},
)
def silver_cuarentena():
    partes = []
    for fuente, vista, llave in FUENTES:
        df = spark.read.table(vista)
        if "es_reprocesado" in df.columns:
            df = df.filter(~F.col("es_reprocesado"))   # duplicado técnico, no un rechazo
        partes.append(calidad.filas_cuarentena(df, fuente, llave))
    return reduce(lambda a, b: a.unionByName(b), partes)
