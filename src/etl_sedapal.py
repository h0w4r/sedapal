from __future__ import annotations

import logging
import time
import importlib
import csv
from pathlib import Path
from typing import Final

import pandas as pd
import requests
from pandas import DataFrame
from requests import Response
from requests.adapters import HTTPAdapter

def _resolver_retry() -> type:
    """Obtiene la clase Retry desde urllib3 o, en su defecto, desde requests.packages."""
    try:
        modulo = importlib.import_module("urllib3.util.retry")
    except ImportError:
        modulo = importlib.import_module("requests.packages.urllib3.util.retry")
    return getattr(modulo, "Retry")

Retry = _resolver_retry()

from .constantes import CLAVE_MICROZONA
from .limpieza_conexiones import cargar_conexiones, limpiar_conexiones
from .limpieza_longitudes import cargar_longitudes, limpiar_longitudes
from .limpieza_proyectos import cargar_proyectos, limpiar_proyectos

CABECERAS_SEDAPAL: Final[dict[str, str]] = {
    "User-Agent": "Mozilla/5.0 (compatible; ETL-Sedapal/1.0; +https://avatar.4elementors.pe)",
    "Accept": "text/csv,application/octet-stream;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}

LOG = logging.getLogger(__name__)

def descargar_dataset(url: str, ruta_destino: Path, reintentos: int = 3, espera_segundos: float = 3.0) -> Path:
    """Descarga un dataset remoto manejando cabeceras personalizadas y reintentos.

    Parámetros
    ----------
    url : str
        Dirección HTTP del recurso CSV a descargar.
    ruta_destino : Path
        Ruta local donde se guardará el archivo.
    reintentos : int, opcional
        Número de reintentos ante fallos transitorios (por defecto 3).
    espera_segundos : float, opcional
        Tiempo base de espera entre reintentos (por defecto 3 segundos).

    Retorna
    -------
    Path
        Ruta del archivo descargado listo para su procesamiento.
    """
    ruta_destino = ruta_destino.resolve()
    ruta_destino.parent.mkdir(parents=True, exist_ok=True)

    sesion = requests.Session()
    estrategia = Retry(
        total=reintentos,
        backoff_factor=espera_segundos,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset({"GET"}),
    )
    adaptador = HTTPAdapter(max_retries=estrategia)
    sesion.mount("http://", adaptador)
    sesion.mount("https://", adaptador)

    intento = 0
    while True:
        intento += 1
        try:
            LOG.info("Descargando %s (intento %s/%s)", url, intento, reintentos)
            respuesta: Response = sesion.get(url, headers=CABECERAS_SEDAPAL, timeout=60)
            respuesta.raise_for_status()
            ruta_destino.write_bytes(respuesta.content)
            return ruta_destino
        except requests.RequestException as error:
            LOG.warning("Fallo al descargar %s: %s", url, error)
            if intento >= reintentos:
                raise
            tiempo_espera = espera_segundos * intento
            LOG.info("Reintentando en %.1f segundos…", tiempo_espera)
            time.sleep(tiempo_espera)

def enriquecer_microzonas(
    conexiones: DataFrame,
    longitudes: DataFrame,
    proyectos: DataFrame,
) -> DataFrame:
    """Integra las tablas limpias respetando la clave de microzona."""
    df_base = conexiones.copy()

    columnas_longitudes = [columna for columna in longitudes.columns if columna not in CLAVE_MICROZONA]
    df_base = df_base.merge(longitudes, on=CLAVE_MICROZONA, how="left")

    proyectos_agrupados = _agrupar_proyectos(proyectos)
    df_base = df_base.merge(proyectos_agrupados, on=CLAVE_MICROZONA, how="left")

    for columna in columnas_longitudes:
        if columna in df_base:
            df_base[columna] = df_base[columna].fillna(0.0)

    columnas_proyectos = ["conteo_proyectos_activos", "avance_promedio_proyectos", "faltan_datos_proyectos"]
    for columna in columnas_proyectos:
        if columna in df_base:
            df_base[columna] = df_base[columna].fillna(0 if columna != "avance_promedio_proyectos" else 0.0)

    return df_base

def calcular_indicadores(tabla_microzonas: DataFrame) -> DataFrame:
    """Calcula métricas e indicadores de calidad para cada microzona."""
    df_indicadores = tabla_microzonas.copy()

    df_indicadores["ratio_conexiones_alcantarillado"] = df_indicadores.apply(
        lambda fila: float(fila["conexiones_alcantarillado"]) / float(fila["conexiones_agua"])
        if float(fila["conexiones_agua"] or 0) > 0
        else 0.0,
        axis=1,
    )

    for clase in ("agua", "desague"):
        suma_columna = df_indicadores.get(f"longitud_total_{clase}", 0.0)
        conexiones = df_indicadores["conexiones_agua"].replace({0: pd.NA})
        df_indicadores[f"densidad_red_{clase}"] = suma_columna / conexiones
        df_indicadores[f"densidad_red_{clase}"] = df_indicadores[f"densidad_red_{clase}"].fillna(0.0)

    df_indicadores["faltan_datos_longitud"] = (
        df_indicadores.filter(regex=r"^longitud_total_").isna().any(axis=1).astype(int)
    )
    df_indicadores["faltan_datos_proyectos"] = df_indicadores["faltan_datos_proyectos"].fillna(0).astype(int)
    df_indicadores["registros_inconsistentes"] = (
        df_indicadores["conexiones_agua"].isna()
        | (df_indicadores["conexiones_agua"] < df_indicadores["conexiones_alcantarillado"])
    ).astype(int)

    return df_indicadores

def guardar_resultados(tabla_indicadores: DataFrame, ruta_salida: Path) -> Path:
    """Guarda el DataFrame consolidado en CSV UTF-8 sin índice, preservando columnas usadas por la API de microzonas."""
    ruta_salida = ruta_salida.resolve()
    ruta_salida.parent.mkdir(parents=True, exist_ok=True)

    tabla_salida = tabla_indicadores.copy()

    columnas_texto = {"distrito", "gerencia_servicios", "equipo_comercial"}
    for columna in columnas_texto:
        if columna in tabla_salida.columns:
            serie_texto = tabla_salida[columna].astype("string").fillna("").str.strip()
            tabla_salida[columna] = serie_texto.apply(lambda valor: str(valor))

    if "ubigeo" in tabla_salida.columns:
        serie_ubigeo = tabla_salida["ubigeo"].astype("Int64")
        tabla_salida["ubigeo"] = serie_ubigeo.apply(
            lambda valor: "" if pd.isna(valor) else f"{int(valor):06d}"
        )

    tabla_salida.to_csv(ruta_salida, index=False, encoding="utf-8", quoting=csv.QUOTE_ALL)
    return ruta_salida

def ejecutar_etl(
    ruta_conexiones: Path,
    ruta_longitudes: Path,
    ruta_proyectos: Path,
    ruta_salida: Path,
) -> Path:
    """Ejecuta la secuencia de limpieza, integración y generación del CSV final."""
    df_conexiones = limpiar_conexiones(cargar_conexiones(ruta_conexiones))
    df_longitudes = limpiar_longitudes(cargar_longitudes(ruta_longitudes))
    df_proyectos = limpiar_proyectos(cargar_proyectos(ruta_proyectos))

    df_microzonas = enriquecer_microzonas(df_conexiones, df_longitudes, df_proyectos)
    df_indicadores = calcular_indicadores(df_microzonas)

    return guardar_resultados(df_indicadores, ruta_salida)

def _agrupar_proyectos(df_proyectos: DataFrame) -> DataFrame:
    """Agrupa los proyectos por microzona para obtener métricas agregadas."""
    df_trabajo = df_proyectos.copy()

    df_trabajo["es_proyecto_activo"] = df_trabajo["etapa"].fillna("").astype(str).str.upper() != "CERRADO"

    agregaciones = {
        "conteo_proyectos_activos": ("es_proyecto_activo", "sum"),
        "avance_promedio_proyectos": ("avance_fisico", "mean"),
    }
    resumen = df_trabajo.groupby(CLAVE_MICROZONA, dropna=False).agg(**agregaciones).reset_index()

    resumen["avance_promedio_proyectos"] = resumen["avance_promedio_proyectos"].fillna(0.0)
    resumen["faltan_datos_proyectos"] = (
        df_trabajo.groupby(CLAVE_MICROZONA, dropna=False)["ubigeo"]
        .apply(lambda serie: int(serie.isna().any()))
        .reset_index(drop=True)
    )

    return resumen

__all__ = [
    "CABECERAS_SEDAPAL",
    "descargar_dataset",
    "enriquecer_microzonas",
    "calcular_indicadores",
    "guardar_resultados",
    "ejecutar_etl",
]
