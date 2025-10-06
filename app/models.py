"""Modelos de datos validados con Pydantic (biblioteca que crea clases con validaciones automáticas)."""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

class MensajeServicio(BaseModel):
    """Representa mensajes generales de la API para informar advertencias o notas."""

    codigo: str = Field(..., description="Identificador corto de la advertencia.")
    detalle: str = Field(..., description="Texto descriptivo de la advertencia para el público técnico.")
    explicacion_simple: str = Field(
        ...,
        description="Descripción en lenguaje cotidiano para facilitar la comprensión del mensaje.",
    )

class Paginacion(BaseModel):
    """Controla la paginación (técnica para dividir resultados en páginas)."""

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    limite: int = Field(
        default=50,
        alias="limit",
        ge=1,
        le=200,
        description="Cantidad máxima de microzonas por página.",
    )
    desplazamiento: int = Field(
        default=0,
        alias="offset",
        ge=0,
        description="Número de registros a saltar antes de empezar la página.",
    )

class FiltroMicrozona(BaseModel):
    """Agrupa filtros que se pueden aplicar sobre las microzonas."""

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    gerencia: Optional[str] = Field(
        default=None,
        alias="gerencia_servicios",
        description="Nombre de la gerencia de servicios solicitada.",
    )
    conexiones_minimas: Optional[int] = Field(
        default=None,
        alias="conexiones_min",
        description="Cantidad mínima de conexiones de agua exigida.",
    )
    conexiones_maximas: Optional[int] = Field(
        default=None,
        alias="conexiones_max",
        description="Cantidad máxima de conexiones de agua permitida.",
    )
    ratio_maximo: Optional[float] = Field(
        default=None,
        alias="ratio_max",
        description="Valor máximo aceptado del ratio de conexiones con alcantarillado.",
    )

class MicrozonaBase(BaseModel):
    """Modelo base que refleja las columnas originales del CSV (archivo de valores separados por comas)."""

    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
        str_strip_whitespace=True,
        from_attributes=True,
    )

    ubigeo: str = Field(alias="ubigeo", description="Código geográfico único de la microzona.")
    distrito: Optional[str] = Field(
        default=None, alias="distrito", description="Nombre del distrito asociado."
    )
    gerencia_servicios: Optional[str] = Field(
        default=None,
        alias="gerencia_servicios",
        description="Gerencia comercial responsable de la zona.",
    )
    equipo_comercial: Optional[str] = Field(
        default=None,
        alias="equipo_comercial",
        description="Equipo comercial asignado por Sedapal.",
    )
    anio: Optional[int] = Field(default=None, alias="anio", description="Año del registro.")
    mes: Optional[int] = Field(default=None, alias="mes", description="Mes del registro.")
    conexiones_agua: Optional[int] = Field(
        default=None,
        alias="conexiones_agua",
        description="Número de conexiones activas de agua.",
    )
    conexiones_alcantarillado: Optional[int] = Field(
        default=None,
        alias="conexiones_alcantarillado",
        description="Número de conexiones activas de alcantarillado.",
    )
    fecha_corte: Optional[date] = Field(
        default=None,
        alias="fecha_corte",
        description="Fecha de corte informada en el dataset.",
    )
    departamento: Optional[str] = Field(
        default=None, alias="departamento", description="Departamento geográfico."
    )
    provincia: Optional[str] = Field(
        default=None, alias="provincia", description="Provincia geográfica."
    )
    tarifa_predominante: Optional[str] = Field(
        default=None,
        alias="tarifa_predominante",
        description="Tarifa más común aplicada a la microzona.",
    )
    longitud_total_agua: Optional[float] = Field(
        default=None,
        alias="longitud_total_agua",
        description="Longitud total de la red de agua reportada.",
    )
    longitud_total_desague: Optional[float] = Field(
        default=None,
        alias="longitud_total_desague",
        description="Longitud total de la red de desagüe reportada.",
    )
    conteo_proyectos_activos: Optional[int] = Field(
        default=None,
        alias="conteo_proyectos_activos",
        description="Cantidad de proyectos activos vigentes.",
    )
    avance_promedio_proyectos: Optional[float] = Field(
        default=None,
        alias="avance_promedio_proyectos",
        description="Porcentaje de avance promedio en los proyectos.",
    )
    faltan_datos_proyectos: Optional[int] = Field(
        default=None,
        alias="faltan_datos_proyectos",
        description="Indicador (1 ó 0) que señala si faltan datos de proyectos.",
    )
    ratio_conexiones_alcantarillado: Optional[float] = Field(
        default=None,
        alias="ratio_conexiones_alcantarillado",
        description=(
            "Relación entre conexiones de alcantarillado y de agua; valores mayores a 1 indican inconsistencia."
        ),
    )
    densidad_red_agua: Optional[float] = Field(
        default=None,
        alias="densidad_red_agua",
        description="Densidad de la red de agua en la microzona.",
    )
    densidad_red_desague: Optional[float] = Field(
        default=None,
        alias="densidad_red_desague",
        description="Densidad de la red de desagüe en la microzona.",
    )
    faltan_datos_longitud: Optional[int] = Field(
        default=None,
        alias="faltan_datos_longitud",
        description="Indicador (1 ó 0) de ausencia de longitud de redes.",
    )
    registros_inconsistentes: Optional[int] = Field(
        default=None,
        alias="registros_inconsistentes",
        description="Indicador (1 ó 0) que marca registros con inconsistencias detectadas.",
    )

class IndicadoresMicrozona(BaseModel):
    """Incluye los indicadores derivados para la evaluación de criticidad."""

    indice_critico: Optional[float] = Field(
        default=None,
        description="Valor del índice de criticidad (0 a 1, donde 1 es más crítico).",
    )
    categoria_microzona: str = Field(
        default="SIN_DATOS",
        description="Etiqueta cualitativa resultante (ESTABLE, VIGILANCIA o CRITICA).",
    )
    advertencias_datos: List[str] = Field(
        default_factory=list,
        description="Listado de advertencias específicas sobre la calidad de los datos.",
    )

class MetadatosCalidad(BaseModel):
    """Resume información adicional sobre la calidad del registro."""

    total_advertencias: int = Field(
        default=0,
        description="Cantidad total de advertencias generadas para la microzona.",
    )
    banderas: List[str] = Field(
        default_factory=list,
        description="Etiquetas breves que resumen las principales advertencias.",
    )

class MicrozonaRespuesta(MicrozonaBase):
    """Modelo enriquecido que combina datos crudos con indicadores y metadatos de calidad."""

    indicadores: IndicadoresMicrozona = Field(
        ..., description="Indicadores numéricos y cualitativos calculados para la microzona."
    )
    metadatos_calidad: MetadatosCalidad = Field(
        default_factory=MetadatosCalidad,
        description="Resumen complementario sobre advertencias y consistencia del registro.",
    )

class ListadoMicrozonas(BaseModel):
    """Estructura paginada que agrupa múltiples microzonas y el contexto de la consulta."""

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    total: int = Field(..., description="Total de microzonas que cumplen los filtros.")
    microzonas: List[MicrozonaRespuesta] = Field(
        ..., description="Colección de microzonas en la página actual."
    )
    paginacion: Paginacion = Field(
        ..., alias="paginacion", description="Información de paginación aplicada."
    )
    filtros: FiltroMicrozona = Field(
        ..., description="Filtros efectivos utilizados en la consulta."
    )
    mensajes: List[MensajeServicio] = Field(
        default_factory=list,
        description="Mensajes adicionales generados durante la consulta.",
    )

class ResumenEstadisticas(BaseModel):
    """Expone estadísticas generales para contextualizar la criticidad a nivel global."""

    total_microzonas: int = Field(..., description="Cantidad total de microzonas analizadas.")
    percentiles_conexiones: Dict[str, Optional[float]] = Field(
        ...,
        description="Percentiles relevantes de conexiones de agua.",
    )
    mediana_ratio: Optional[float] = Field(
        default=None,
        description="Mediana del ratio de alcantarillado.",
    )
    maximo_ratio: Optional[float] = Field(
        default=None,
        description="Valor máximo del ratio de alcantarillado observado.",
    )
    advertencias_globales: List[str] = Field(
        default_factory=list,
        description="Mensajes acerca de carencias globales, como longitudes en cero.",
    )
