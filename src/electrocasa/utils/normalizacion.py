"""Funciones de normalización compartidas por las transformaciones de Silver.

Todas devuelven expresiones de columna de PySpark y nunca fallan con ANSI activo:
usan try_cast / try_to_timestamp, que devuelven NULL si el valor
no tiene el formato esperado en vez de romper la consulta.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Texto
# ---------------------------------------------------------------------------

_CON_TILDE = "áéíóúÁÉÍÓÚñÑ"
_SIN_TILDE = "aeiouAEIOUnN"


def texto_limpio(c: str) -> Column:
    """trim + vacío -> NULL."""
    return F.when(F.trim(F.col(c)) == "", F.lit(None)).otherwise(F.trim(F.col(c)))


def clave_texto(c: str) -> Column:
    """Minúsculas, sin tildes, con '_' y espacios unificados en un solo espacio.

    'Línea blanca', 'LINEA BLANCA' y 'linea_blanca' quedan todas como 'linea blanca'.
    """
    base = F.lower(F.translate(F.trim(F.col(c)), _CON_TILDE, _SIN_TILDE))
    return F.trim(F.regexp_replace(base, r"[_\-\s]+", " "))


def mapear(clave: Column, mapa: dict) -> Column:
    """Traduce una clave normalizada a su valor canónico. Lo que no está en el mapa queda NULL."""
    expr = None
    for origen, destino in mapa.items():
        cond = clave == F.lit(origen)
        expr = F.when(cond, F.lit(destino)) if expr is None else expr.when(cond, F.lit(destino))
    return expr.otherwise(F.lit(None))


# ---------------------------------------------------------------------------
# Números
# ---------------------------------------------------------------------------


def monto_decimal(c: str, tipo: str = "decimal(12,2)") -> Column:
    """'1.234,50' -> 1234.50 ; '407,98' -> 407.98 ; '6792.35' -> 6792.35 ; '' -> NULL.

    Si el texto trae coma, la coma es el separador decimal y los puntos son de miles.
    """
    s = F.trim(F.col(c))
    con_coma = F.regexp_replace(F.regexp_replace(s, r"\.", ""), ",", ".")
    normal = F.when(F.instr(s, ",") > 0, con_coma).otherwise(s)
    return F.when(s == "", F.lit(None)).otherwise(normal.try_cast(tipo))


def precio_decimal(c: str, tipo: str = "decimal(12,2)") -> Column:
    """'S/ 1,299.90' -> 1299.90 ; '3107.28' -> 3107.28. Aquí la coma es de miles."""
    s = F.regexp_replace(F.regexp_replace(F.trim(F.col(c)), r"(?i)s/\s*", ""), ",", "")
    return F.when(F.trim(F.col(c)) == "", F.lit(None)).otherwise(s.try_cast(tipo))


def entero(c: str) -> Column:
    return F.trim(F.col(c)).try_cast("int")


# ---------------------------------------------------------------------------
# Fechas
# ---------------------------------------------------------------------------


def _try_ts(c: str, formato: str) -> Column:
    # try_to_timestamp devuelve NULL si el texto no calza con el formato (no falla con ANSI).
    return F.expr(f'try_to_timestamp(trim({c}), "{formato}")')


def fecha_flexible(c: str) -> Column:
    """Acepta 'yyyy-MM-dd', 'dd/MM/yyyy' y 'yyyy-MM-ddTHH:mm:ss'. Devuelve DATE o NULL."""
    return F.to_date(F.coalesce(
        _try_ts(c, "yyyy-MM-dd"),
        _try_ts(c, "dd/MM/yyyy"),
        _try_ts(c, "yyyy-MM-dd'T'HH:mm:ss"),
    ))


def timestamp_flexible(c: str) -> Column:
    return F.coalesce(
        _try_ts(c, "yyyy-MM-dd HH:mm:ss"),
        _try_ts(c, "yyyy-MM-dd'T'HH:mm:ss"),
    )


# ---------------------------------------------------------------------------
# Catálogos cerrados (los valores salen de la exploración de Bronze; ver contratos/)
# ---------------------------------------------------------------------------

METODOS_PAGO = {
    # 17 escrituras -> 5 métodos
    "tarjeta": "tarjeta",
    "tarjeta de credito": "tarjeta",
    "tc": "tarjeta",
    "td": "tarjeta",
    "efectivo": "efectivo",
    "efv": "efectivo",
    "yape": "yape",
    "plin": "plin",
    "transferencia": "transferencia",
    "trf": "transferencia",
}

CATEGORIAS = {
    # 17 escrituras -> 5 categorías
    "linea blanca": "linea_blanca",
    "cocina": "cocina",
    "electronica": "electronica",
    "climatizacion": "climatizacion",
    "entretenimiento": "entretenimiento",
}

TIPOS_EVENTO = {
    # 13 escrituras -> 4 tipos; 'cese' es sinónimo de baja
    "alta": "alta",
    "transferencia": "transferencia",
    "cambio salario": "cambio_salario",
    "baja": "baja",
    "cese": "baja",
}

ESTADOS_ENTREGA = {
    # La fuente mezcla casing, '_' y espacios. Si al explorar bronze.tracking_envios
    # aparece una variante nueva, se agrega aquí y en contratos/tracking_envios.yml.
    "pendiente": "pendiente",
    "pending": "pendiente",
    "creado": "pendiente",
    "registrado": "pendiente",
    "en transito": "en_transito",
    "en camino": "en_transito",
    "transito": "en_transito",
    "in transit": "en_transito",
    "en ruta": "en_transito",
    "entregado": "entregado",
    "delivered": "entregado",
    "devuelto": "devuelto",
    "returned": "devuelto",
    "devolucion": "devuelto",
}

CANALES = {"tienda fisica": "tienda_fisica", "ecommerce": "ecommerce", "e commerce": "ecommerce"}

# Rango de salario mensual plausible en soles. Fuera de rango -> se anula el salario
# (la fila se conserva porque la sucursal sigue siendo válida para dotación).
SALARIO_MIN = 900
SALARIO_MAX = 20000
