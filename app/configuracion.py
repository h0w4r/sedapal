"""Configuración de la API usando pydantic-settings (extensión que carga variables desde entorno y archivos)."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class ConfiguracionServicio(BaseSettings):
    """Aglutina rutas y parámetros de la API de microzonas en un objeto centralizado."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="SEDAPAL_",
        extra="ignore",
    )

    ruta_csv_microzonas: Path = Field(
        default=Path("data/processed/microzonas_sedapal.csv"),
        description="Ubicación por defecto del dataset procesado de microzonas.",
    )
    limite_por_defecto: int = Field(
        default=50,
        ge=1,
        le=200,
        description="Cantidad estándar de microzonas por página.",
    )
    maximo_limite: int = Field(
        default=200,
        ge=50,
        le=500,
        description="Límite duro permitido para la paginación.",
    )
    origenes_permitidos: Optional[str] = Field(
        default=None,
        description=(
            "Cadena separada por comas con los orígenes (dominios) autorizados para CORS "
            "(sigla de Cross-Origin Resource Sharing, política de intercambio entre dominios)."
        ),
    )

def obtener_configuracion() -> ConfiguracionServicio:
    """Devuelve una instancia única de la configuración compartida."""
    return ConfiguracionServicio()
