"""Script auxiliar para descargar los datasets oficiales de SEDAPAL.

Este módulo utiliza la función `descargar_dataset` definida en `src.etl_sedapal` (gestiona descargas HTTP con reintentos).
"""

from pathlib import Path
import sys

# Se agrega el directorio raíz del repositorio al sys.path para encontrar el paquete `src`.
sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.etl_sedapal import descargar_dataset

# Diccionario que define el nombre local deseado en data/raw/ y la URL fuente correspondiente.
URLS_OFICIALES: dict[str, str] = {
    "conexiones.csv": "https://www.datosabiertos.gob.pe/sites/default/files/DataSet-Conexiones-APO-ALC.csv",
    "proyectos.csv": "https://www.datosabiertos.gob.pe/sites/default/files/Dataset-proyectos_3.csv",
    "longitudes.csv": "https://www.datosabiertos.gob.pe/sites/default/files/DataSet-Longitud-de-Redes-APO-ALC.csv",
}

def main() -> None:
    """Descarga cada dataset y lo guarda en la carpeta `data/raw/`."""
    ruta_raw = Path("data/raw")
    ruta_raw.mkdir(parents=True, exist_ok=True)

    for nombre_archivo, url in URLS_OFICIALES.items():
        destino = ruta_raw / nombre_archivo
        print(f"Descargando {url} -> {destino}")
        descargar_dataset(url, destino)

if __name__ == "__main__":
    main()
