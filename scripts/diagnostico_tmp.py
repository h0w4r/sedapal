"""Script temporal para inspeccionar el CSV generado por ejecutar_etl y analizar sus filas."""

from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

RAIZ_PROYECTO = Path(__file__).resolve().parent.parent
if str(RAIZ_PROYECTO) not in sys.path:
    sys.path.insert(0, str(RAIZ_PROYECTO))

from src.etl_sedapal import ejecutar_etl

def ejecutar_diagnostico() -> None:
    """Ejecuta el ETL con fixtures de prueba y muestra microzonas generadas."""
    ruta_raiz = Path(__file__).resolve().parent.parent
    ruta_fixtures = ruta_raiz / "tests" / "fixtures"
    ruta_temporal = ruta_raiz / "tmp_microzonas.csv"

    ruta_salida = ejecutar_etl(
        ruta_fixtures / "conexiones.csv",
        ruta_fixtures / "longitudes.csv",
        ruta_fixtures / "proyectos.csv",
        ruta_temporal,
    )

    tabla = pd.read_csv(ruta_salida)
    print("Columnas:", tabla.columns.tolist())
    print("Total filas:", len(tabla))
    print("Ubigeos únicos:", tabla["ubigeo"].unique())
    if "distrito" in tabla.columns:
        print("Distritos únicos:", tabla["distrito"].unique())
    print("Primeras filas:\n", tabla.head())

if __name__ == "__main__":
    ejecutar_diagnostico()
