from __future__ import annotations

"""Conjunto de pruebas automatizadas con pytest (marco de pruebas unitarias para Python)
para validar las transformaciones del ETL (Extracción, Transformación y Carga)."""

from pathlib import Path
from typing import cast

import pandas as pd
import pytest

from src.constantes import CLAVE_MICROZONA
from src.etl_sedapal import (
    calcular_indicadores,
    ejecutar_etl,
    enriquecer_microzonas,
)
from src.limpieza_conexiones import cargar_conexiones, limpiar_conexiones
from src.limpieza_longitudes import cargar_longitudes, limpiar_longitudes
from src.limpieza_proyectos import cargar_proyectos, limpiar_proyectos

@pytest.fixture()
def ruta_fixtures() -> Path:
    """Entrega la ruta de los archivos CSV (valores separados por comas) sintéticos."""
    return Path(__file__).resolve().parent / "fixtures"

@pytest.fixture()
def tabla_conexiones_cruda(ruta_fixtures: Path) -> pd.DataFrame:
    """Carga las conexiones en un DataFrame (tabla en memoria con filas y columnas homogéneas)."""
    return cargar_conexiones(ruta_fixtures / "conexiones.csv")

@pytest.fixture()
def tabla_longitudes_cruda(ruta_fixtures: Path) -> pd.DataFrame:
    """Carga las longitudes de redes en un DataFrame (tabla en memoria con datos tabulares)."""
    return cargar_longitudes(ruta_fixtures / "longitudes.csv")

@pytest.fixture()
def tabla_proyectos_cruda(ruta_fixtures: Path) -> pd.DataFrame:
    """Carga los proyectos en un DataFrame (tabla en memoria con registros estructurados)."""
    return cargar_proyectos(ruta_fixtures / "proyectos.csv")

@pytest.fixture()
def tabla_conexiones_limpia(tabla_conexiones_cruda: pd.DataFrame) -> pd.DataFrame:
    """Aplica la limpieza de conexiones para dejar datos listos para la integración."""
    return limpiar_conexiones(tabla_conexiones_cruda)

@pytest.fixture()
def tabla_longitudes_limpia(tabla_longitudes_cruda: pd.DataFrame) -> pd.DataFrame:
    """Estandariza las longitudes para facilitar los cálculos posteriores."""
    return limpiar_longitudes(tabla_longitudes_cruda)

@pytest.fixture()
def tabla_proyectos_limpia(tabla_proyectos_cruda: pd.DataFrame) -> pd.DataFrame:
    """Normaliza los proyectos asegurando etapas, costos y fechas consistentes."""
    return limpiar_proyectos(tabla_proyectos_cruda)

@pytest.fixture()
def tabla_microzonas_integrada(
    tabla_conexiones_limpia: pd.DataFrame,
    tabla_longitudes_limpia: pd.DataFrame,
    tabla_proyectos_limpia: pd.DataFrame,
) -> pd.DataFrame:
    """Integra las tablas limpias respetando la clave de microzona."""
    return enriquecer_microzonas(
        tabla_conexiones_limpia,
        tabla_longitudes_limpia,
        tabla_proyectos_limpia,
    )

@pytest.fixture()
def tabla_indicadores_calculada(
    tabla_microzonas_integrada: pd.DataFrame,
) -> pd.DataFrame:
    """Calcula los indicadores finales a partir de la tabla integrada."""
    return calcular_indicadores(tabla_microzonas_integrada)

def test_limpiar_conexiones_agrega_y_estandariza(tabla_conexiones_limpia: pd.DataFrame) -> None:
    """Verifica que la limpieza de conexiones consolide y normalice los campos clave."""
    columnas_necesarias = {
        *CLAVE_MICROZONA,
        "conexiones_agua",
        "conexiones_alcantarillado",
        "fecha_corte",
    }
    assert columnas_necesarias.issubset(set(tabla_conexiones_limpia.columns))

    registros_san_miguel = (
        tabla_conexiones_limpia.loc[
            tabla_conexiones_limpia["ubigeo"] == "150132"
        ]
        .reset_index(drop=True)
    )
    assert not registros_san_miguel.empty

    conexiones_agua = cast(int, registros_san_miguel.at[0, "conexiones_agua"])
    conexiones_alcantarillado = cast(int, registros_san_miguel.at[0, "conexiones_alcantarillado"])
    anio_registro = cast(int, registros_san_miguel.at[0, "anio"])
    mes_registro = cast(int, registros_san_miguel.at[0, "mes"])
    fecha_corte = cast(pd.Timestamp, registros_san_miguel.at[0, "fecha_corte"])

    assert conexiones_agua == 150
    assert conexiones_alcantarillado == 120
    assert anio_registro == 2024
    assert mes_registro == 12
    assert fecha_corte.year == 2024
    assert fecha_corte.month == 12

def test_limpiar_longitudes_crea_totales(tabla_longitudes_limpia: pd.DataFrame) -> None:
    """Confirma que las longitudes generen totales y no dejen valores faltantes."""
    registros_ate = (
        tabla_longitudes_limpia.loc[
            tabla_longitudes_limpia["ubigeo"] == "150101"
        ]
        .reset_index(drop=True)
    )
    assert not registros_ate.empty

    red_secundaria_desague = cast(float, registros_ate.at[0, "red_secundaria_desague"])
    longitud_total_agua = cast(float, registros_ate.at[0, "longitud_total_agua"])
    longitud_total_desague = cast(float, registros_ate.at[0, "longitud_total_desague"])

    assert red_secundaria_desague == pytest.approx(0.0)
    assert longitud_total_agua == pytest.approx(450.0)
    assert longitud_total_desague == pytest.approx(210.0)

def test_limpiar_proyectos_estandariza_etapas(tabla_proyectos_limpia: pd.DataFrame) -> None:
    """Asegura que las etapas y montos de proyectos se normalicen correctamente."""
    assert tabla_proyectos_limpia.shape[0] == 3

    etapas_unicas = set(tabla_proyectos_limpia["etapa"].unique())
    assert etapas_unicas == {"OBRA", "CERRADO"}

    costos_san_miguel = tabla_proyectos_limpia.loc[
        tabla_proyectos_limpia["ubigeo"] == "150132",
        "costo_total",
    ]
    costo_maximo = cast(float, costos_san_miguel.max())
    assert costo_maximo == pytest.approx(1_500_000.50)

def test_enriquecer_microzonas_completa_datos(
    tabla_microzonas_integrada: pd.DataFrame,
) -> None:
    """Revisa que la integración aporte columnas de proyectos y longitudes."""
    registros_san_miguel = (
        tabla_microzonas_integrada.loc[
            tabla_microzonas_integrada["ubigeo"] == "150132"
        ]
        .reset_index(drop=True)
    )
    assert not registros_san_miguel.empty

    conteo_proyectos = cast(int, registros_san_miguel.at[0, "conteo_proyectos_activos"])
    avance_promedio = cast(float, registros_san_miguel.at[0, "avance_promedio_proyectos"])
    longitud_total_agua = cast(float, registros_san_miguel.at[0, "longitud_total_agua"])
    faltan_datos = cast(int, registros_san_miguel.at[0, "faltan_datos_proyectos"])

    assert conteo_proyectos == 2
    assert avance_promedio == pytest.approx(80.0)
    assert longitud_total_agua == pytest.approx(370.5)
    assert faltan_datos == 0

def test_calcular_indicadores_deriva_metricas(
    tabla_indicadores_calculada: pd.DataFrame,
) -> None:
    """Comprueba que los indicadores resultantes reflejen los cálculos esperados."""
    registros_san_miguel = (
        tabla_indicadores_calculada.loc[
            tabla_indicadores_calculada["ubigeo"] == "150132"
        ]
        .reset_index(drop=True)
    )
    assert not registros_san_miguel.empty

    ratio_alcantarillado = cast(float, registros_san_miguel.at[0, "ratio_conexiones_alcantarillado"])
    densidad_red_agua = cast(float, registros_san_miguel.at[0, "densidad_red_agua"])
    densidad_red_desague = cast(float, registros_san_miguel.at[0, "densidad_red_desague"])
    registros_inconsistentes = cast(int, registros_san_miguel.at[0, "registros_inconsistentes"])

    assert ratio_alcantarillado == pytest.approx(0.8)
    assert densidad_red_agua == pytest.approx(370.5 / 150)
    assert densidad_red_desague == pytest.approx(320.0 / 150)
    assert registros_inconsistentes == 0

def test_ejecutar_etl_generar_csv(
    ruta_fixtures: Path,
    tmp_path: Path,
) -> None:
    """Valida que `ejecutar_etl` genere un CSV de salida en el directorio temporal.

    `tmp_path` es un directorio temporal que pytest (marco de pruebas) provee para aislar archivos.
    """
    ruta_salida = tmp_path / "microzonas_sedapal.csv"
    ruta_generada = ejecutar_etl(
        ruta_fixtures / "conexiones.csv",
        ruta_fixtures / "longitudes.csv",
        ruta_fixtures / "proyectos.csv",
        ruta_salida,
    )

    assert ruta_generada.exists()

    tabla_generada = pd.read_csv(
        ruta_generada,
        dtype={"ubigeo": "string"},
    )
    tabla_generada["ubigeo"] = tabla_generada["ubigeo"].fillna("").str.strip()
    columnas_minimas = {
        *CLAVE_MICROZONA,
        "conexiones_agua",
        "conexiones_alcantarillado",
        "ratio_conexiones_alcantarillado",
        "densidad_red_agua",
        "densidad_red_desague",
    }
    assert columnas_minimas.issubset(set(tabla_generada.columns))

    registros_san_miguel = (
        tabla_generada.loc[
            tabla_generada["ubigeo"] == "150132"
        ]
        .reset_index(drop=True)
    )
    assert not registros_san_miguel.empty

    ratio_generado = cast(float, registros_san_miguel.at[0, "ratio_conexiones_alcantarillado"])
    densidad_generada = cast(float, registros_san_miguel.at[0, "densidad_red_agua"])

    assert ratio_generado == pytest.approx(0.8)
    assert densidad_generada == pytest.approx(370.5 / 150)
