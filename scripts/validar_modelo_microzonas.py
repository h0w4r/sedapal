"""Script para validar que cada microzona del dataset puede inicializar el modelo `MicrozonaRespuesta` (clase que valida campos de una microzona)."""

from __future__ import annotations

from pathlib import Path
import sys
from typing import Tuple

RAIZ_PROYECTO = Path(__file__).resolve().parent.parent
if str(RAIZ_PROYECTO) not in sys.path:
    sys.path.insert(0, str(RAIZ_PROYECTO))

from app.models import MicrozonaRespuesta
from app.services.criticos import _construir_microzona_respuesta
from app.dependencies import obtener_dataset_microzonas

def validar_microzonas() -> Tuple[int, int]:
    """Itera por cada fila del dataset y valida la construcción del modelo."""
    dataset = obtener_dataset_microzonas()
    total = len(dataset)
    errores = 0

    for indice, fila in dataset.iterrows():
        microzona_dict = _construir_microzona_respuesta(fila)
        try:
            MicrozonaRespuesta(**microzona_dict)
        except Exception as exc:  # noqa: BLE001
            errores += 1
            print(f"Error en la fila {indice} (ubigeo={microzona_dict.get('ubigeo')}): {exc}")
            print("Tipos detectados en la microzona fallida:")
            tipos = {clave: type(valor).__name__ for clave, valor in microzona_dict.items()}
            for clave, tipo in tipos.items():
                print(f" - {clave}: {tipo}")
            break

    return total, errores

if __name__ == "__main__":
    total_microzonas, total_errores = validar_microzonas()
    print(f"Total evaluadas: {total_microzonas}")
    print(f"Errores detectados: {total_errores}")
