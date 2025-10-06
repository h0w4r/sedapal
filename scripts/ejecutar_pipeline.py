"""Script auxiliar para ejecutar el pipeline ETL completo con datasets reales.

Este script reutiliza la función `ejecutar_etl` de `src.etl_sedapal` para limpiar, integrar y generar
el archivo final `data/processed/microzonas_sedapal.csv`.
"""

from pathlib import Path
import sys

# Se agrega el directorio raíz del repositorio al sys.path para resolver el paquete `src`.
sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.etl_sedapal import ejecutar_etl

def main() -> None:
    """Ejecuta la secuencia de limpieza, integración y exportación del ETL."""
    ruta_conexiones = Path("data/raw/conexiones.csv")
    ruta_longitudes = Path("data/raw/longitudes.csv")
    ruta_proyectos = Path("data/raw/proyectos.csv")
    ruta_salida = Path("data/processed/microzonas_sedapal.csv")

    ruta_salida.parent.mkdir(parents=True, exist_ok=True)

    resultado = ejecutar_etl(
        ruta_conexiones=ruta_conexiones,
        ruta_longitudes=ruta_longitudes,
        ruta_proyectos=ruta_proyectos,
        ruta_salida=ruta_salida,
    )
    print(f"CSV final generado en: {resultado.resolve()}")

if __name__ == "__main__":
    main()
