"""Dependencias compartidas que centralizan carga de datos y configuración de la API."""

from __future__ import annotations

from functools import lru_cache
from typing import Dict, Tuple

from pandas import DataFrame

from app.configuracion import ConfiguracionServicio, obtener_configuracion
from config.criterios import CriteriosCriticidad, criterios_por_defecto
from src.analytics.microzonas import (
    anotar_indicadores,
    calcular_percentiles,
    cargar_microzonas,
)

@lru_cache(maxsize=1)
def obtener_configuracion_servicio() -> ConfiguracionServicio:
    """Entrega la configuración global del servicio para su reutilización."""
    return obtener_configuracion()

@lru_cache(maxsize=1)
def obtener_criterios_servicio() -> CriteriosCriticidad:
    """Devuelve los criterios de criticidad con caché para evitar recrearlos."""
    return criterios_por_defecto()

@lru_cache(maxsize=1)
def _cargar_dataset_enriquecido() -> Tuple[DataFrame, Dict[str, float]]:
    """Carga el dataset desde disco, calcula percentiles y anota indicadores.

    Returns:
        tuple: Contiene un DataFrame enriquecido y un diccionario con percentiles útiles.
    """
    configuracion = obtener_configuracion_servicio()
    criterios = obtener_criterios_servicio()

    microzonas_base = cargar_microzonas(configuracion.ruta_csv_microzonas)
    percentiles = calcular_percentiles(microzonas_base)
    microzonas_enriquecidas = anotar_indicadores(microzonas_base, criterios)

    return microzonas_enriquecidas, percentiles

def obtener_dataset_microzonas() -> DataFrame:
    """Entrega una copia del DataFrame enriquecido para su consumo en la API."""
    microzonas, _ = _cargar_dataset_enriquecido()
    return microzonas.copy()

def obtener_percentiles_microzonas() -> Dict[str, float]:
    """Devuelve los percentiles calculados para acompañar respuestas agregadas."""
    _, percentiles = _cargar_dataset_enriquecido()
    return dict(percentiles)

def limpiar_caches() -> None:
    """Permite limpiar las memorias caché, útil en pruebas automatizadas."""
    _cargar_dataset_enriquecido.cache_clear()
    obtener_configuracion_servicio.cache_clear()
    obtener_criterios_servicio.cache_clear()
