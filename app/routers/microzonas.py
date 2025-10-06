"""Endpoints de la API para gestionar microzonas críticas."""

from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Depends, Query

import app.dependencies as dependencias
from app.models import (
    FiltroMicrozona,
    ListadoMicrozonas,
    MicrozonaRespuesta,
    Paginacion,
    ResumenEstadisticas,
)
from app.services.criticos import (
    filtrar_microzonas,
    obtener_estadisticas,
    obtener_microzona,
)

router = APIRouter(prefix="/microzonas", tags=["microzonas"])

def obtener_parametros_paginacion(
    limite: int = Query(
        default=50,
        ge=1,
        le=200,
        alias="limit",
        description="Cantidad máxima de registros a devolver.",
    ),
    desplazamiento: int = Query(
        default=0,
        ge=0,
        alias="offset",
        description="Número de registros a omitir antes de iniciar la página.",
    ),
) -> Paginacion:
    """Construye el objeto de paginación a partir de parámetros de consulta."""
    return Paginacion.model_validate({"limit": limite, "offset": desplazamiento})

def obtener_parametros_filtro(
    gerencia: Optional[str] = Query(
        default=None,
        alias="gerencia_servicios",
        description="Filtra por gerencia de servicios concreta.",
    ),
    conexiones_min: Optional[int] = Query(
        default=None,
        ge=0,
        alias="conexiones_min",
        description="Número mínimo de conexiones de agua requeridas.",
    ),
    conexiones_max: Optional[int] = Query(
        default=None,
        ge=0,
        alias="conexiones_max",
        description="Número máximo de conexiones de agua permitidas.",
    ),
    ratio_max: Optional[float] = Query(
        default=None,
        ge=0,
        alias="ratio_max",
        description="Valor máximo aceptado para el ratio de alcantarillado.",
    ),
) -> FiltroMicrozona:
    """Genera el filtro compuesto a partir de parámetros opcionales."""
    return FiltroMicrozona.model_validate(
        {
            "gerencia_servicios": gerencia,
            "conexiones_min": conexiones_min,
            "conexiones_max": conexiones_max,
            "ratio_max": ratio_max,
        }
    )

@router.get(
    "",
    response_model=ListadoMicrozonas,
    summary="Listar microzonas con filtros dinámicos.",
)
def listar_microzonas(
    paginacion: Paginacion = Depends(obtener_parametros_paginacion),
    filtros: FiltroMicrozona = Depends(obtener_parametros_filtro),
) -> ListadoMicrozonas:
    """Retorna microzonas con filtros, paginación y advertencias asociadas."""
    configuracion = dependencias.obtener_configuracion_servicio()
    microzonas = dependencias.obtener_dataset_microzonas()

    microzonas_respuesta, total, mensajes = filtrar_microzonas(
        microzonas,
        filtros,
        paginacion,
        configuracion.maximo_limite,
    )

    microzonas_modelos = [MicrozonaRespuesta(**microzona) for microzona in microzonas_respuesta]

    return ListadoMicrozonas(
        total=total,
        microzonas=microzonas_modelos,
        paginacion=paginacion,
        filtros=filtros,
        mensajes=mensajes,
    )

@router.get(
    "/criticas",
    response_model=ListadoMicrozonas,
    summary="Priorizar microzonas críticas según el índice calculado.",
)
def listar_microzonas_criticas(
    paginacion: Paginacion = Depends(obtener_parametros_paginacion),
    filtros: FiltroMicrozona = Depends(obtener_parametros_filtro),
) -> ListadoMicrozonas:
    """Devuelve únicamente microzonas clasificadas como críticas."""
    configuracion = dependencias.obtener_configuracion_servicio()
    microzonas = dependencias.obtener_dataset_microzonas()

    if "categoria_microzona" in microzonas.columns:
        microzonas = microzonas[microzonas["categoria_microzona"] == "CRITICA"].copy()
        microzonas.sort_values(by="indice_critico", ascending=False, inplace=True)

    microzonas_respuesta, total, mensajes = filtrar_microzonas(
        microzonas,
        filtros,
        paginacion,
        configuracion.maximo_limite,
    )

    microzonas_modelos = [MicrozonaRespuesta(**microzona) for microzona in microzonas_respuesta]

    return ListadoMicrozonas(
        total=total,
        microzonas=microzonas_modelos,
        paginacion=paginacion,
        filtros=filtros,
        mensajes=mensajes,
    )

@router.get(
    "/resumen",
    response_model=ResumenEstadisticas,
    summary="Obtener estadísticas de contexto para las microzonas.",
)
def obtener_resumen_microzonas() -> ResumenEstadisticas:
    """Entrega métricas globales y advertencias sobre el dataset."""
    microzonas = dependencias.obtener_dataset_microzonas()
    percentiles = dependencias.obtener_percentiles_microzonas()
    resumen = obtener_estadisticas(microzonas, percentiles)
    return ResumenEstadisticas(**resumen)

@router.get(
    "/{ubigeo}",
    response_model=MicrozonaRespuesta,
    summary="Recuperar el detalle completo de una microzona específica.",
)
def detalle_microzona(ubigeo: str) -> MicrozonaRespuesta:
    """Busca una microzona por su UBIGEO y devuelve su información enriquecida."""
    microzonas = dependencias.obtener_dataset_microzonas()
    microzona = obtener_microzona(microzonas, ubigeo)
    return MicrozonaRespuesta(**microzona)
