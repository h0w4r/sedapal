"""Rutinas analíticas para enriquecer microzonas con indicadores de criticidad."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List

import pandas as pd
from pandas import DataFrame, Series

if TYPE_CHECKING:
    from config.criterios import CriteriosCriticidad
else:  # pragma: no cover - protección mientras se define el módulo de criterios.
    CriteriosCriticidad = Any


def cargar_microzonas(ruta: Path | str) -> DataFrame:
    """Carga el CSV de microzonas en un DataFrame (tabla de la librería pandas para análisis de datos).

    Args:
        ruta: Ruta al archivo CSV que contiene las microzonas.

    Returns:
        DataFrame: Tabla con columnas tipificadas y valores limpios.

    Raises:
        FileNotFoundError: Si el archivo no existe en la ruta indicada.
    """
    ruta_archivo = Path(ruta)
    if not ruta_archivo.exists():
        raise FileNotFoundError(f"No se encontró el archivo de microzonas en {ruta_archivo}")

    # Se lee el CSV y luego se tipifican columnas críticas.
    microzonas = pd.read_csv(ruta_archivo)

    columnas_texto = [
        "ubigeo",
        "distrito",
        "gerencia_servicios",
        "equipo_comercial",
        "departamento",
        "provincia",
        "tarifa_predominante",
    ]
    for columna in columnas_texto:
        if columna in microzonas.columns:
            microzonas[columna] = microzonas[columna].fillna("").astype("string").str.strip()

    if "fecha_corte" in microzonas.columns:
        microzonas["fecha_corte"] = pd.to_datetime(microzonas["fecha_corte"], errors="coerce")

    columnas_enteras = [
        "anio",
        "mes",
        "conexiones_agua",
        "conexiones_alcantarillado",
        "conteo_proyectos_activos",
        "faltan_datos_proyectos",
        "faltan_datos_longitud",
        "registros_inconsistentes",
    ]
    for columna in columnas_enteras:
        if columna in microzonas.columns:
            serie_entera = pd.to_numeric(microzonas[columna], errors="coerce")
            microzonas[columna] = serie_entera.round().astype("Int64")

    columnas_flotantes = [
        "longitud_total_agua",
        "longitud_total_desague",
        "avance_promedio_proyectos",
        "ratio_conexiones_alcantarillado",
        "densidad_red_agua",
        "densidad_red_desague",
    ]
    for columna in columnas_flotantes:
        if columna in microzonas.columns:
            microzonas[columna] = pd.to_numeric(microzonas[columna], errors="coerce")

    return microzonas


def calcular_percentiles(microzonas: DataFrame) -> Dict[str, float]:
    """Calcula percentiles y métricas descriptivas de conexiones y ratios.

    Args:
        microzonas: DataFrame con la columna ``conexiones_agua`` y métricas relacionadas.

    Returns:
        dict: Diccionario con percentiles (valores por debajo de los cuales cae un porcentaje de observaciones) y
        estadísticas de soporte.
    """
    resultados: Dict[str, float] = {"total_registros": int(len(microzonas))}

    if "conexiones_agua" in microzonas.columns:
        serie_conexiones = pd.to_numeric(microzonas["conexiones_agua"], errors="coerce").dropna()
    else:
        serie_conexiones = Series(dtype="float64")
    if serie_conexiones.empty:
        resultados.update(
            {
                "p10_conexiones": float("nan"),
                "p25_conexiones": float("nan"),
                "mediana_conexiones": float("nan"),
                "p75_conexiones": float("nan"),
            }
        )
    else:
        percentiles = serie_conexiones.quantile([0.10, 0.25, 0.50, 0.75], interpolation="linear")
        resultados["p10_conexiones"] = float(percentiles.loc[0.10])
        resultados["p25_conexiones"] = float(percentiles.loc[0.25])
        resultados["mediana_conexiones"] = float(percentiles.loc[0.50])
        resultados["p75_conexiones"] = float(percentiles.loc[0.75])

    if "ratio_conexiones_alcantarillado" in microzonas.columns:
        serie_ratio = pd.to_numeric(
            microzonas["ratio_conexiones_alcantarillado"], errors="coerce"
        ).dropna()
    else:
        serie_ratio = Series(dtype="float64")
    if serie_ratio.empty:
        resultados["mediana_ratio"] = float("nan")
        resultados["maximo_ratio"] = float("nan")
    else:
        resultados["mediana_ratio"] = float(serie_ratio.median())
        resultados["maximo_ratio"] = float(serie_ratio.max())

    return resultados


def anotar_indicadores(microzonas: DataFrame, criterios: "CriteriosCriticidad") -> DataFrame:
    """Agrega el índice crítico y categorizaciones basadas en los criterios provistos.

    Args:
        microzonas: DataFrame con las columnas necesarias para los cálculos.
        criterios: Objeto con pesos, percentiles y umbrales de la clase ``CriteriosCriticidad``.

    Returns:
        DataFrame: Copia del DataFrame original con columnas ``indice_critico``, ``categoria_microzona`` y
        ``advertencias_datos``.
    """
    if microzonas.empty:
        microzonas_vacias = microzonas.copy()
        microzonas_vacias["indice_critico"] = pd.Series(dtype="float64")
        microzonas_vacias["categoria_microzona"] = pd.Series(dtype="string")
        microzonas_vacias["advertencias_datos"] = pd.Series(dtype=object)
        return microzonas_vacias

    enriquecido = microzonas.copy()

    if "ratio_conexiones_alcantarillado" in enriquecido.columns:
        serie_ratio = pd.to_numeric(
            enriquecido["ratio_conexiones_alcantarillado"], errors="coerce"
        )
    else:
        serie_ratio = Series(dtype="float64")
    serie_ratio_normalizada = serie_ratio.fillna(1.0).clip(upper=1.0)

    percentil_referencia = getattr(criterios, "percentil_conexiones_critico", float("nan"))
    if not pd.notna(percentil_referencia) or percentil_referencia <= 0:
        percentiles_estimados = calcular_percentiles(enriquecido)
        percentil_referencia = percentiles_estimados.get("p75_conexiones", float("nan"))
        if not pd.notna(percentil_referencia) or percentil_referencia <= 0:
            percentil_referencia = 1.0

    if "conexiones_agua" in enriquecido.columns:
        serie_conexiones = pd.to_numeric(enriquecido["conexiones_agua"], errors="coerce")
    else:
        serie_conexiones = Series(dtype="float64")
    serie_cobertura = (serie_conexiones / percentil_referencia).fillna(1.0).clip(upper=1.0)

    peso_ratio = getattr(criterios, "peso_ratio", 0.6)
    peso_conexiones = getattr(criterios, "peso_conexiones", 0.4)
    suma_pesos = peso_ratio + peso_conexiones
    if pd.notna(suma_pesos) and suma_pesos != 1:
        # Se normalizan los pesos para conservar la escala del índice.
        peso_ratio = peso_ratio / suma_pesos
        peso_conexiones = peso_conexiones / suma_pesos

    indice = peso_ratio * (1 - serie_ratio_normalizada) + peso_conexiones * (1 - serie_cobertura)
    enriquecido["indice_critico"] = indice.round(3)

    umbral_alerta = getattr(criterios, "umbral_categoria_alerta", 0.3)
    umbral_critica = getattr(criterios, "umbral_categoria_critica", 0.6)
    if umbral_alerta > umbral_critica:
        umbral_alerta, umbral_critica = umbral_critica, umbral_alerta

    def clasificar_microzona(valor: float) -> str:
        """Clasifica la microzona en función del índice."""
        if pd.isna(valor):
            return "SIN_DATOS"
        if valor >= umbral_critica:
            return "CRITICA"
        if valor >= umbral_alerta:
            return "VIGILANCIA"
        return "ESTABLE"

    enriquecido["categoria_microzona"] = (
        enriquecido["indice_critico"].apply(clasificar_microzona).astype("string")
    )

    def generar_advertencias(fila: pd.Series) -> List[str]:
        """Construye advertencias simples sobre carencias de información."""
        advertencias: List[str] = []
        longitud_agua = fila.get("longitud_total_agua")
        if pd.isna(longitud_agua) or float(longitud_agua) <= 0:
            advertencias.append("Sin longitud de red de agua reportada.")
        longitud_desague = fila.get("longitud_total_desague")
        if pd.isna(longitud_desague) or float(longitud_desague) <= 0:
            advertencias.append("Sin longitud de red de desagüe reportada.")
        proyectos_activos = fila.get("conteo_proyectos_activos")
        if pd.isna(proyectos_activos) or float(proyectos_activos) <= 0:
            advertencias.append("Sin proyectos activos registrados para la microzona.")
        conexiones_agua = fila.get("conexiones_agua")
        if pd.isna(conexiones_agua) or float(conexiones_agua) <= 0:
            advertencias.append("Sin conexiones de agua registradas.")
        ratio_valor = fila.get("ratio_conexiones_alcantarillado")
        if pd.notna(ratio_valor) and float(ratio_valor) > 1:
            advertencias.append("El ratio de alcantarillado supera la unidad; revisar consistencia.")
        return advertencias

    enriquecido["advertencias_datos"] = enriquecido.apply(generar_advertencias, axis=1)

    return enriquecido
