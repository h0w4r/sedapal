"""Configuración compartida de pytest para asegurar importaciones del paquete de la API y el ETL."""

from __future__ import annotations

import sys
from pathlib import Path

RAIZ_PROYECTO = Path(__file__).resolve().parent.parent
if str(RAIZ_PROYECTO) not in sys.path:
    sys.path.insert(0, str(RAIZ_PROYECTO))
