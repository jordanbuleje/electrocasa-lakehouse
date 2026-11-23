"""Silver de Empleados: eventos limpios + historia SCD Type 2.

Por qué intervalos armados a mano y no AUTO CDC:
  - El orden real de los cambios sale de DOS columnas de la fuente: fecha_evento
    (cuándo ocurrió) y fecha_registro (cuándo lo cargó RR.HH.), que desempata los
    eventos del mismo día. Los intervalos se arman con una ventana ordenada por
    (fecha_evento, fecha_registro): nunca con current_timestamp().
  - Silver de Empleados es una Materialized View: en cada corrida recalcula los
    intervalos con TODOS los eventos. Un evento de junio que RR.HH. registró en
    octubre (llega en el lote 3) queda en su lugar y corta la versión anterior.

Masking: dni y salario llevan la cláusula MASK en el schema de cada tabla del pipeline.
Las funciones de máscara las crea notebooks/00_setup.ipynb antes del primer despliegue.
El dueño del pipeline tiene que ser miembro de electrocasa_ingenieria; si no, las tablas
derivadas de una tabla enmascarada guardarían los valores ya enmascarados.
"""

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

from utils import calidad, config
from utils import normalizacion as N


def _mascaras():
    cat = config.catalogo()
    return f"{cat}.silver.mascara_dni", f"{cat}.silver.mascara_salario"


@dp.temporary_view(name="empleados_preparados")
def empleados_preparados():
    bronze = spark.read.table(config.tabla("bronze", "empleados"))
    columnas_raw = [c for c in bronze.columns if not c.startswith("_")]
    salario = N.monto_decimal("salario", "decimal(10,2)")
    df = (
        bronze
        .withColumn("_raw", F.struct(*columnas_raw))
        .select(
            N.texto_limpio("id_empleado").alias("id_empleado"),
            F.initcap(N.texto_limpio("nombre")).alias("nombre"),
            N.texto_limpio("dni").alias("dni"),
            F.lower(N.texto_limpio("email")).alias("email"),
            salario.alias("salario_origen"),
            N.texto_limpio("sucursal_id").alias("sucursal_id"),
            N.texto_limpio("cargo").alias("cargo"),
            N.mapear(N.clave_texto("tipo_evento"), N.TIPOS_EVENTO).alias("tipo_evento"),
            N.fecha_flexible("fecha_evento").alias("fecha_evento"),
            N.timestamp_flexible("fecha_registro").alias("fecha_registro"),
            "_raw", "_archivo_origen", "_lote", "_fecha_ingesta",
        )
        # Salarios con un orden de magnitud de más (246616.0 en vez de ~2466): se anula
        # el salario y se conserva el evento, porque la sucursal sigue siendo válida.
        .withColumn(
            "salario_fuera_de_rango",
            F.coalesce(~F.col("salario_origen").between(N.SALARIO_MIN, N.SALARIO_MAX), F.lit(False)),
        )
        .withColumn("salario", F.when(~F.col("salario_fuera_de_rango"), F.col("salario_origen")))
    )
    # Reingestas exactas del mismo evento.
    w_dup = Window.partitionBy("id_empleado", "tipo_evento", "fecha_evento", "fecha_registro").orderBy("_fecha_ingesta")
    df = df.withColumn("_rn", F.row_number().over(w_dup)).filter("_rn = 1").drop("_rn")
    # Mismo dni en dos empleados distintos: error de digitación. Se conservan los dos
    # empleados (son personas distintas con id propio) y se marca el dni.
    w_dni = Window.partitionBy("dni")
    return df.withColumn(
        "dni_duplicado",
        F.when(F.col("dni").isNull(), F.lit(False))
         .otherwise(F.size(F.collect_set("id_empleado").over(w_dni)) > 1),
    )


def _schema_eventos():
    m_dni, m_sal = _mascaras()
    return f"""
        id_empleado STRING NOT NULL,
        nombre STRING,
        dni STRING MASK {m_dni},
        email STRING,
        salario DECIMAL(10,2) MASK {m_sal},
        sucursal_id STRING,
        cargo STRING,
        tipo_evento STRING,
        fecha_evento DATE,
        fecha_registro TIMESTAMP,
        dni_duplicado BOOLEAN,
        salario_fuera_de_rango BOOLEAN,
        archivo_origen STRING,
        lote STRING,
        fecha_ingesta TIMESTAMP
    """


@dp.materialized_view(
    name=config.tabla("silver", "empleados_eventos"),
    comment="Eventos de RR.HH. limpios: tipo_evento estandarizado, fechas tipadas, salarios fuera de rango anulados. dni y salario enmascarados.",
    table_properties={"quality": "silver"},
    schema=_schema_eventos(),
)
@dp.expect_all_or_drop(calidad.reglas_drop("empleados"))
@dp.expect_all(calidad.reglas_warn("empleados"))
def silver_empleados_eventos():
    return spark.read.table("empleados_preparados").select(
        "id_empleado", "nombre", "dni", "email", "salario", "sucursal_id", "cargo",
        "tipo_evento", "fecha_evento", "fecha_registro", "dni_duplicado",
        "salario_fuera_de_rango",
        F.col("_archivo_origen").alias("archivo_origen"),
        F.col("_lote").alias("lote"),
        F.col("_fecha_ingesta").alias("fecha_ingesta"),
    )


def _schema_historia():
    m_dni, m_sal = _mascaras()
    return f"""
        id_empleado STRING NOT NULL,
        version INT,
        nombre STRING,
        dni STRING MASK {m_dni},
        email STRING,
        salario DECIMAL(10,2) MASK {m_sal},
        sucursal_id STRING,
        cargo STRING,
        evento_origen STRING,
        valido_desde DATE,
        valido_hasta DATE,
        es_vigente BOOLEAN,
        fecha_registro TIMESTAMP
    """


@dp.materialized_view(
    name=config.tabla("silver", "empleados"),
    comment=(
        "Empleados historizados como SCD Type 2. Una fila por versión, vigente en "
        "[valido_desde, valido_hasta). valido_hasta NULL = versión actual. "
        "Una baja cierra la versión anterior y no abre una nueva."
    ),
    table_properties={"quality": "silver", "scd_type": "2"},
    schema=_schema_historia(),
)
def silver_empleados():
    eventos = spark.read.table(config.tabla("silver", "empleados_eventos"))
    orden = Window.partitionBy("id_empleado").orderBy("fecha_evento", "fecha_registro")
    con_siguiente = eventos.withColumn("valido_hasta", F.lead("fecha_evento").over(orden))
    versiones = con_siguiente.filter(F.col("tipo_evento") != "baja")
    return versiones.select(
        "id_empleado",
        F.row_number().over(orden).cast("int").alias("version"),
        "nombre", "dni", "email", "salario", "sucursal_id", "cargo",
        F.col("tipo_evento").alias("evento_origen"),
        F.col("fecha_evento").alias("valido_desde"),
        "valido_hasta",
        F.col("valido_hasta").isNull().alias("es_vigente"),
        "fecha_registro",
    )
