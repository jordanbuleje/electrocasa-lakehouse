"""Esquemas explícitos de Bronze (nunca inferSchema).

Bronze guarda los valores tal como llegan: todo texto, salvo las estructuras
anidadas de Reseñas. Los tipos de negocio se aplican recién en Silver.
"""

from pyspark.sql.types import ArrayType, StringType, StructField, StructType


def _texto(*columnas: str) -> StructType:
    return StructType([StructField(c, StringType(), True) for c in columnas])


VENTAS = _texto(
    "venta_id", "sucursal_id", "producto_id", "vendedor_id", "cantidad",
    "monto_total", "metodo_pago", "fecha_venta", "canal",
)

EMPLEADOS = _texto(
    "id_empleado", "nombre", "dni", "email", "salario", "sucursal_id",
    "cargo", "tipo_evento", "fecha_evento", "fecha_registro",
)

DEVOLUCIONES = _texto(
    "devolucion_id", "venta_id", "sucursal_id", "producto_id",
    "motivo", "monto_reembolso", "fecha_devolucion",
)

RESENAS = StructType([
    StructField("resena_id", StringType(), True),
    StructField("producto_id", StringType(), True),
    StructField("cliente_id", StringType(), True),
    StructField("calificacion", StringType(), True),
    StructField("comentario", StringType(), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField("respuestas", ArrayType(StructType([
        StructField("autor", StringType(), True),
        StructField("texto", StringType(), True),
        StructField("fecha", StringType(), True),
    ])), True),
    StructField("fecha_resena", StringType(), True),
    # metadata cambia de forma según el origen (app, web, kiosko): se guarda como texto JSON.
    StructField("metadata", StringType(), True),
])

# Las tablas de COPY INTO (catalogo, sucursales) se crean en notebooks/00_setup.ipynb.
