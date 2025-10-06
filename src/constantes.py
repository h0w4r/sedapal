"""Constantes compartidas para el ETL de SEDAPAL.

CLAVE_MICROZONA define los campos que identifican una microzona operativa de SEDAPAL.
"""

from __future__ import annotations

CLAVE_MICROZONA: list[str] = [
    "ubigeo",
    "distrito",
    "gerencia_servicios",
    "equipo_comercial",
    "anio",
    "mes",
]

__all__ = [
    "CLAVE_MICROZONA",
]
