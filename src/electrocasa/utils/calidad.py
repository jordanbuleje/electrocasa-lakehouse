"""Reglas de calidad y cuarentena.

Cada fuente declara sus reglas UNA sola vez en REGLAS. De ese diccionario salen:
  - las expectativas del pipeline (dp.expect_all_or_drop / dp.expect_all),
  - las filas de la tabla silver.cuarentena (qué regla falló, fuente, lote, fecha),
  - y el contrato de datos de contratos/<fuente>.yml, que repite los mismos nombres.

Las condiciones están escritas para no dar NULL nunca (siempre 'x IS NOT NULL AND ...'),
así la expectativa y la cuarentena evalúan exactamente lo mismo.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REGLAS = {
    "ventas": {
        "drop": {
            "venta_id_informado": "venta_id IS NOT NULL",
            "monto_positivo": "monto_total IS NOT NULL AND monto_total > 0",
            "cantidad_positiva": "cantidad IS NOT NULL AND cantidad > 0",
            "sucursal_informada": "sucursal_id IS NOT NULL",
            "fecha_valida": "fecha_venta IS NOT NULL",
            "metodo_pago_conocido": "metodo_pago IS NOT NULL",
            "canal_conocido": "canal IS NOT NULL",
        },
        "warn": {
            "producto_en_catalogo": "producto_en_catalogo",
        },
    },
    "catalogo": {
        "drop": {
            "producto_id_informado": "producto_id IS NOT NULL",
            "categoria_conocida": "categoria IS NOT NULL",
        },
        "warn": {
            "precio_valido": "precio_lista IS NOT NULL AND precio_lista > 0",
            "marca_informada": "marca IS NOT NULL",
        },
    },
    "empleados": {
        "drop": {
            "evento_ubicable": "id_empleado IS NOT NULL AND fecha_evento IS NOT NULL",
            "tipo_evento_conocido": "tipo_evento IS NOT NULL",
            "sucursal_informada": "sucursal_id IS NOT NULL",
        },
        "warn": {
            "dni_informado": "dni IS NOT NULL",
            "dni_unico": "NOT dni_duplicado",
            "salario_en_rango": "NOT salario_fuera_de_rango",
        },
    },
    "resenas": {
        "drop": {
            "resena_id_informado": "resena_id IS NOT NULL",
            "calificacion_en_escala": "calificacion IS NOT NULL AND calificacion BETWEEN 1 AND 5",
            "producto_en_catalogo": "producto_en_catalogo",
            "fecha_valida": "fecha_resena IS NOT NULL",
        },
        "warn": {
            "comentario_informado": "comentario IS NOT NULL",
        },
    },
    "devoluciones": {
        "drop": {
            "devolucion_id_informado": "devolucion_id IS NOT NULL",
            "venta_existe": "monto_venta IS NOT NULL",
            "reembolso_valido": (
                "monto_reembolso IS NOT NULL AND monto_venta IS NOT NULL "
                "AND monto_reembolso >= 0 AND monto_reembolso <= monto_venta"
            ),
            "fecha_valida": "fecha_devolucion IS NOT NULL",
        },
        "warn": {
            "motivo_informado": "motivo <> 'no_informado'",
        },
    },
    "tracking": {
        "drop": {
            "tracking_id_informado": "tracking_id IS NOT NULL",
            "estado_conocido": "estado_entrega IN ('pendiente', 'en_transito', 'entregado', 'devuelto')",
            "fecha_modificacion_informada": "fecha_modificacion IS NOT NULL",
        },
        "warn": {
            "venta_existe": "venta_en_ventas",
            "courier_informado": "courier IS NOT NULL",
        },
    },
    "sucursales": {
        "drop": {
            "sucursal_id_informado": "sucursal_id IS NOT NULL",
        },
        "warn": {
            "region_informada": "region IS NOT NULL",
        },
    },
}


def reglas_drop(fuente: str) -> dict:
    return REGLAS[fuente]["drop"]


def reglas_warn(fuente: str) -> dict:
    return REGLAS[fuente]["warn"]


def _cumple(cond: str):
    return F.coalesce(F.expr(cond), F.lit(False))


def con_reglas_fallidas(df: DataFrame, fuente: str) -> DataFrame:
    """Agrega dos columnas: reglas_drop_fallidas y reglas_warn_fallidas (arrays de nombres)."""
    def fallidas(reglas: dict):
        return F.filter(
            F.array(*[F.when(~_cumple(c), F.lit(n)) for n, c in reglas.items()]),
            lambda x: x.isNotNull(),
        )
    return (
        df.withColumn("reglas_drop_fallidas", fallidas(reglas_drop(fuente)))
          .withColumn("reglas_warn_fallidas", fallidas(reglas_warn(fuente)))
    )


def filas_cuarentena(df: DataFrame, fuente: str, llave: str) -> DataFrame:
    """Filas que violan alguna regla de la fuente, en el formato común de silver.cuarentena.

    - politica = 'drop' si la fila no llegó a Silver (violó una regla crítica),
      'warn' si llegó a Silver pero con una advertencia.
    - registro guarda la fila ORIGINAL de Bronze (columna _raw) como JSON, para poder
      corregirla y volver a cargarla.
    """
    marcado = con_reglas_fallidas(df, fuente)
    drop_ = F.size("reglas_drop_fallidas") > 0
    warn_ = F.size("reglas_warn_fallidas") > 0
    return (
        marcado.filter(drop_ | warn_)
        .select(
            F.lit(fuente).alias("fuente"),
            F.col(llave).cast("string").alias("llave"),
            F.when(drop_, F.lit("drop")).otherwise(F.lit("warn")).alias("politica"),
            F.when(drop_, F.col("reglas_drop_fallidas"))
             .otherwise(F.col("reglas_warn_fallidas")).alias("reglas_fallidas"),
            F.col("_archivo_origen").alias("archivo_origen"),
            F.col("_lote").alias("lote"),
            F.col("_fecha_ingesta").alias("fecha_ingesta"),
            F.current_timestamp().alias("fecha_proceso"),
            F.to_json(F.col("_raw")).alias("registro"),
        )
    )
