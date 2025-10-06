"""Script temporal para inspeccionar tipos y valores dentro del dataset de microzonas utilizado por la API."""

from __future__ import annotations

from pathlib import Path
from pprint import pprint
import sys

RAIZ_PROYECTO = Path(__file__).resolve().parent.parent
if str(RAIZ_PROYECTO) not in sys.path:
    sys.path.insert(0, str(RAIZ_PROYECTO))

from app.dependencies import obtener_dataset_microzonas


def mostrar_tipos_y_valores() -> None:
    """Muestra los tipos de datos y valores de la primera fila para detectar incompatibilidades."""
    dataset = obtener_dataset_microzonas()
    if dataset.empty:
        print("El dataset de microzonas está vacío.")
        return

    fila = dataset.iloc[0]
    tipos = {columna: type(fila[columna]).__name__ for columna in dataset.columns}
    valores = {columna: fila[columna] for columna in dataset.columns}

    print("Tipos de la primera fila:")
    pprint(tipos)
    print("\nValores de la primera fila:")
    pprint(valores)


if __name__ == "__main__":
    mostrar_tipos_y_valores()
