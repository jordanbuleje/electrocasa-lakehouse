"""gold.empleados_vigentes: tabla derivada de Empleados para demostrar el masking.

Se calcula a partir de silver.empleados (enmascarada). Como el dueño del pipeline es
miembro de electrocasa_ingenieria, el refresh lee los valores reales y los guarda.
La máscara vuelve a declararse aquí porque electrocasa_analistas y
electrocasa_auditoria tienen SELECT sobre gold: sin MASK verían el dni y el salario.

Prueba (notebooks/05_verificacion_masking.ipynb):
  - un miembro de electrocasa_ingenieria ve dni y salario reales;
  - cualquier otro usuario ve el dni con los últimos 3 dígitos y el salario en NULL.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utils import config


def _schema():
    cat = config.catalogo()
    return f"""
        id_empleado STRING NOT NULL,
        nombre STRING,
        dni STRING MASK {cat}.silver.mascara_dni,
        email STRING,
        salario DECIMAL(10,2) MASK {cat}.silver.mascara_salario,
        sucursal_id STRING,
        nombre_sucursal STRING,
        cargo STRING,
        vigente_desde DATE
    """


@dp.materialized_view(
    name=config.tabla("gold", "empleados_vigentes"),
    comment="Empleados con su versión vigente (sin baja). dni y salario enmascarados salvo para electrocasa_ingenieria.",
    table_properties={"quality": "gold"},
    schema=_schema(),
)
def gold_empleados_vigentes():
    sucursales = spark.read.table(config.tabla("silver", "sucursales")).select("sucursal_id", "nombre_sucursal")
    return (
        spark.read.table(config.tabla("silver", "empleados"))
        .filter(F.col("es_vigente"))
        .join(sucursales, "sucursal_id", "left")
        .select("id_empleado", "nombre", "dni", "email", "salario", "sucursal_id",
                "nombre_sucursal", "cargo", F.col("valido_desde").alias("vigente_desde"))
    )
