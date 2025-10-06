"""Servicios especializados para consultar microzonas y evaluar su criticidad."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pandas as pd
from fastapi import HTTPException, status
from pandas import DataFrame

from app.models import FiltroMicrozona, MensajeServicio, Paginacion

def filtrar_microzonas(
    microzonas: DataFrame,
    filtros: FiltroMicrozona,
    paginacion: Paginacion,
    limite_maximo: int,
) -> Tuple[List[Dict[str, Any]], int, List[MensajeServicio]]:
    """Filtra el DataFrame de microzonas y construye las estructuras de respuesta.

    Args:
        microzonas: DataFrame con microzonas ya enriquecidas.
        filtros: Criterios de filtrado solicitados.
        paginacion: Parámetros de paginación (limit y offset).
        limite_maximo: Límite absoluto permitido para la consulta.

    Returns:
        tuple: Lista de microzonas normalizadas, total de elementos y mensajes sobre la calidad de datos.
    """
    tabla_filtrada = microzonas.copy()

    if filtros.gerencia:
        tabla_filtrada = tabla_filtrada[
            tabla_filtrada["gerencia_servicios"].str.lower()
            == filtros.gerencia.strip().lower()
        ]

    if filtros.conexiones_minimas is not None:
        tabla_filtrada = tabla_filtrada[
            tabla_filtrada["conexiones_agua"].fillna(0) >= filtros.conexiones_minimas
        ]

    if filtros.conexiones_maximas is not None:
        tabla_filtrada = tabla_filtrada[
            tabla_filtrada["conexiones_agua"].fillna(0) <= filtros.conexiones_maximas
        ]

    if filtros.ratio_maximo is not None:
        tabla_filtrada = tabla_filtrada[
            tabla_filtrada["ratio_conexiones_alcantarillado"].fillna(0) <= filtros.ratio_maximo
        ]

    total_filtrado = int(len(tabla_filtrada))

    limite_normalizado = min(paginacion.limite, limite_maximo)
    inicio = paginacion.desplazamiento
    fin = inicio + limite_normalizado

    segmento = tabla_filtrada.iloc[inicio:fin]

    microzonas_respuesta = [
        _construir_microzona_respuesta(fila) for _, fila in segmento.iterrows()
    ]

    mensajes = _construir_mensajes_calidad(tabla_filtrada)

    return microzonas_respuesta, total_filtrado, mensajes

def obtener_microzona(microzonas: DataFrame, ubigeo: str) -> Dict[str, Any]:
    """Busca una microzona específica por su ubigeo (código geográfico)."""
    consulta = microzonas[microzonas["ubigeo"] == ubigeo]
    if consulta.empty:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "codigo": "MICROZONA_NO_ENCONTRADA",
                "detalle": "No se halló la microzona solicitada.",
                "explicacion_simple": "Revisa el código UBIGEO proporcionado.",
            },
        )

    fila = consulta.iloc[0]
    return _construir_microzona_respuesta(fila)

def obtener_estadisticas(
    microzonas: DataFrame,
    percentiles: Dict[str, float],
) -> Dict[str, Any]:
    """Genera estadísticas generales para contextualizar los resultados."""
    advertencias_globales = _detectar_limitaciones_globales(microzonas)

    return {
        "total_microzonas": int(len(microzonas)),
        "percentiles_conexiones": {
            clave: percentiles.get(clave)
            for clave in [
                "p10_conexiones",
                "p25_conexiones",
                "mediana_conexiones",
                "p75_conexiones",
            ]
        },
        "mediana_ratio": percentiles.get("mediana_ratio"),
        "maximo_ratio": percentiles.get("maximo_ratio"),
        "advertencias_globales": advertencias_globales,
    }

def _construir_microzona_respuesta(fila: pd.Series) -> Dict[str, Any]:
    """Convierte una fila del DataFrame en la estructura esperada por los modelos Pydantic."""
    datos_base = fila.drop(
        labels=["indice_critico", "categoria_microzona", "advertencias_datos"],
        errors="ignore",
    ).to_dict()

    campos_enteros = [
        "anio",
        "mes",
        "conexiones_agua",
        "conexiones_alcantarillado",
        "conteo_proyectos_activos",
        "faltan_datos_proyectos",
        "faltan_datos_longitud",
        "registros_inconsistentes",
    ]
    for campo in campos_enteros:
        if campo in datos_base:
            valor_campo = datos_base[campo]
            if pd.isna(valor_campo):
                datos_base[campo] = None
            else:
                try:
                    datos_base[campo] = int(valor_campo)
                except (TypeError, ValueError):
                    datos_base[campo] = None

    if "fecha_corte" in datos_base and pd.isna(datos_base["fecha_corte"]):
        datos_base["fecha_corte"] = None

    advertencias = _normalizar_advertencias(fila.get("advertencias_datos"))
    banderas = _generar_banderas(advertencias)

    return {
        **datos_base,
        "indicadores": {
            "indice_critico": fila.get("indice_critico"),
            "categoria_microzona": fila.get("categoria_microzona") or "SIN_DATOS",
            "advertencias_datos": advertencias,
        },
        "metadatos_calidad": {
            "total_advertencias": len(advertencias),
            "banderas": banderas,
        },
    }

def _normalizar_advertencias(valor: Any) -> List[str]:
    """Asegura que las advertencias se representen como lista de cadenas."""
    if valor is None:
        return []
    if isinstance(valor, list):
        return [str(item) for item in valor]
    if isinstance(valor, (set, tuple)):
        return [str(item) for item in valor]
    return [str(valor)]

def _generar_banderas(advertencias: List[str]) -> List[str]:
    """Deriva banderas breves a partir de textos de advertencia."""
    banderas: List[str] = []
    for advertencia in advertencias:
        texto = advertencia.lower()
        if "agua" in texto and "longitud" in texto:
            banderas.append("SIN_LONGITUD_AGUA")
        elif "desagüe" in texto and "longitud" in texto:
            banderas.append("SIN_LONGITUD_DESAGUE")
        elif "proyectos" in texto:
            banderas.append("SIN_PROYECTOS")
        elif "conexiones de agua" in texto:
            banderas.append("SIN_CONEXIONES_AGUA")
        elif "supera la unidad" in texto:
            banderas.append("RATIO_MAYOR_UNO")
        else:
            banderas.append("ADVERTENCIA")
    return banderas

def _construir_mensajes_calidad(tabla: DataFrame) -> List[MensajeServicio]:
    """Genera mensajes contextualizados sobre las carencias de la tabla actual."""
    mensajes: List[MensajeServicio] = []

    if tabla.empty:
        mensajes.append(
            MensajeServicio(
                codigo="SIN_RESULTADOS",
                detalle="La combinación de filtros no devolvió microzonas.",
                explicacion_simple="No hay registros que cumplan los criterios seleccionados.",
            )
        )
        return mensajes

    if (tabla["longitud_total_agua"].fillna(0) <= 0).all():
        mensajes.append(
            MensajeServicio(
                codigo="LONGITUD_AGUA_CERO",
                detalle="Todas las microzonas seleccionadas reportan longitud de red de agua igual a cero.",
                explicacion_simple="No se cuenta con información de longitud de red de agua para estas zonas.",
            )
        )

    if (tabla["longitud_total_desague"].fillna(0) <= 0).all():
        mensajes.append(
            MensajeServicio(
                codigo="LONGITUD_DESAGUE_CERO",
                detalle="Todas las microzonas seleccionadas reportan longitud de red de desagüe igual a cero.",
                explicacion_simple="No se cuenta con información de red de desagüe para estas zonas.",
            )
        )

    if (tabla["conteo_proyectos_activos"].fillna(0) <= 0).all():
        mensajes.append(
            MensajeServicio(
                codigo="SIN_PROYECTOS_ACTIVOS",
                detalle="No existen proyectos activos registrados para las microzonas devueltas.",
                explicacion_simple="No hay intervenciones en curso para estas microzonas.",
            )
        )

    if (tabla["ratio_conexiones_alcantarillado"].dropna() > 1).any():
        mensajes.append(
            MensajeServicio(
                codigo="RATIO_SUPERIOR_UNO",
                detalle="Existen microzonas con ratio de alcantarillado mayor a 1; podría indicar inconsistencias.",
                explicacion_simple="Algunas zonas muestran más conexiones de desagüe que de agua.",
            )
        )

    return mensajes

def _detectar_limitaciones_globales(microzonas: DataFrame) -> List[str]:
    """Detecta limitaciones relevantes del dataset completo."""
    advertencias: List[str] = []

    if microzonas.empty:
        advertencias.append("El conjunto de microzonas está vacío.")
        return advertencias

    if (microzonas["longitud_total_agua"].fillna(0) <= 0).all():
        advertencias.append("Todas las microzonas reportan longitud de red de agua igual a cero.")

    if (microzonas["longitud_total_desague"].fillna(0) <= 0).all():
        advertencias.append("Todas las microzonas reportan longitud de red de desagüe igual a cero.")

    if (microzonas["conteo_proyectos_activos"].fillna(0) <= 0).sum() >= len(microzonas) - 1:
        advertencias.append("Solo se registran proyectos activos en una microzona o ninguna.")

    return advertencias
