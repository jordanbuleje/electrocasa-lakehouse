"""Parámetros del pipeline.

Los valores llegan desde resources/electrocasa_pipeline.yml (bloque `configuration`),
que a su vez los toma de las variables del bundle según el target (dev / prod).
"""

from pyspark.sql import SparkSession


def _conf(clave: str, defecto: str = None) -> str:
    spark = SparkSession.getActiveSession()
    return spark.conf.get(clave, defecto)


def catalogo() -> str:
    return _conf("electrocasa.catalog", "electrocasa_dev")


def tabla(schema: str, nombre: str) -> str:
    return f"{catalogo()}.{schema}.{nombre}"


def landing(fuente: str) -> str:
    return f"/Volumes/{catalogo()}/default/vol_landing/{fuente}/"


def tabla_tracking() -> str:
    """Tabla destino del conector query-based de Lakeflow Connect."""
    return _conf("electrocasa.tracking_table", tabla("bronze", "tracking_envios"))


# Período de análisis de las preguntas de negocio (marzo a octubre de 2026).
INICIO_PERIODO = "2026-03-01"
FIN_PERIODO = "2026-10-31"
