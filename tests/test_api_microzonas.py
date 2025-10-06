"""Pruebas de integración para la API de microzonas críticas."""

from __future__ import annotations

from typing import Dict, Generator, List

import pandas as pd
import pytest
from fastapi.testclient import TestClient
from pandas import DataFrame

import app.dependencies as dependencias
from app.main import app

@pytest.fixture
def datos_microzonas() -> DataFrame:
    """Crea un conjunto controlado de microzonas para escenarios de prueba."""
    registros: List[Dict[str, object]] = [
        {
            "ubigeo": "150101",
            "distrito": "LIMA",
            "gerencia_servicios": "GERENCIA CENTRO",
            "equipo_comercial": "CS BRENA",
            "anio": 2023,
            "mes": 5,
            "conexiones_agua": 1000,
            "conexiones_alcantarillado": 600,
            "fecha_corte": "2023-05-05",
            "departamento": "LIMA",
            "provincia": "LIMA",
            "tarifa_predominante": "COMERCIAL",
            "longitud_total_agua": 0.0,
            "longitud_total_desague": 0.0,
            "conteo_proyectos_activos": 0,
            "avance_promedio_proyectos": 0.0,
            "faltan_datos_proyectos": 1,
            "ratio_conexiones_alcantarillado": 0.6,
            "densidad_red_agua": 0.0,
            "densidad_red_desague": 0.0,
            "faltan_datos_longitud": 1,
            "registros_inconsistentes": 0,
            "indice_critico": 0.76,
            "categoria_microzona": "CRITICA",
            "advertencias_datos": [
                "Sin longitud de red de agua reportada.",
                "Sin proyectos activos registrados para la microzona.",
            ],
        },
        {
            "ubigeo": "150102",
            "distrito": "BELLAVISTA",
            "gerencia_servicios": "GERENCIA NORTE",
            "equipo_comercial": "CS CALLAO",
            "anio": 2023,
            "mes": 5,
            "conexiones_agua": 300,
            "conexiones_alcantarillado": 360,
            "fecha_corte": "2023-05-05",
            "departamento": "LIMA",
            "provincia": "CALLAO",
            "tarifa_predominante": "COMERCIAL",
            "longitud_total_agua": 0.0,
            "longitud_total_desague": 0.0,
            "conteo_proyectos_activos": 0,
            "avance_promedio_proyectos": 0.0,
            "faltan_datos_proyectos": 1,
            "ratio_conexiones_alcantarillado": 1.2,
            "densidad_red_agua": 0.0,
            "densidad_red_desague": 0.0,
            "faltan_datos_longitud": 1,
            "registros_inconsistentes": 1,
            "indice_critico": 0.25,
            "categoria_microzona": "ESTABLE",
            "advertencias_datos": [
                "Sin longitud de red de agua reportada.",
                "El ratio de alcantarillado supera la unidad; revisar consistencia.",
            ],
        },
    ]
    datos = pd.DataFrame(registros)
    datos["fecha_corte"] = pd.to_datetime(datos["fecha_corte"])
    return datos

@pytest.fixture
def cliente_api(
    monkeypatch: pytest.MonkeyPatch,
    datos_microzonas: DataFrame,
) -> Generator[TestClient, None, None]:
    """Configura un cliente de pruebas con dependencias controladas."""
    dependencias.limpiar_caches()

    percentiles_simulados: Dict[str, float] = {
        "p10_conexiones": 200.0,
        "p25_conexiones": 300.0,
        "mediana_conexiones": 650.0,
        "p75_conexiones": 800.0,
        "mediana_ratio": 0.9,
        "maximo_ratio": 1.2,
        "total_registros": 2,
    }

    def cargar_dataset_simulado() -> DataFrame:
        """Entrega una copia del dataset simulado para evitar mutaciones entre pruebas."""
        return datos_microzonas.copy()

    def cargar_percentiles_simulados() -> Dict[str, float]:
        """Retorna métricas descriptivas prefijadas para los endpoints agregados."""
        return percentiles_simulados

    monkeypatch.setattr(dependencias, "obtener_dataset_microzonas", cargar_dataset_simulado)
    monkeypatch.setattr(dependencias, "obtener_percentiles_microzonas", cargar_percentiles_simulados)

    with TestClient(app) as cliente:
        yield cliente

    dependencias.limpiar_caches()

def test_listado_microzonas_devuelve_paginado(cliente_api: TestClient) -> None:
    """Verifica que el listado base retorne estructura paginada y advertencias."""
    respuesta = cliente_api.get("/microzonas", params={"limit": 1, "offset": 0})
    assert respuesta.status_code == 200

    cuerpo = respuesta.json()
    assert cuerpo["total"] == 2
    assert len(cuerpo["microzonas"]) == 1
    assert cuerpo["microzonas"][0]["indicadores"]["categoria_microzona"] == "CRITICA"
    codigos_mensajes = [mensaje["codigo"] for mensaje in cuerpo["mensajes"]]
    assert "LONGITUD_AGUA_CERO" in codigos_mensajes
    assert "SIN_PROYECTOS_ACTIVOS" in codigos_mensajes

def test_microzonas_criticas_aplica_umbral(cliente_api: TestClient) -> None:
    """Comprueba que el endpoint crítico reduzca el universo a microzonas prioritarias."""
    respuesta = cliente_api.get("/microzonas/criticas")
    assert respuesta.status_code == 200

    cuerpo = respuesta.json()
    categorias = {elemento["indicadores"]["categoria_microzona"] for elemento in cuerpo["microzonas"]}
    assert categorias == {"CRITICA"}
    assert cuerpo["total"] == 1

def test_detalle_microzona_inexistente(cliente_api: TestClient) -> None:
    """Confirma que se retorne 404 cuando el UBIGEO no está presente."""
    respuesta = cliente_api.get("/microzonas/999999")
    assert respuesta.status_code == 404
    detalle = respuesta.json()["detail"]
    assert detalle["codigo"] == "MICROZONA_NO_ENCONTRADA"

def test_estadisticas_generales_reflejan_limites(cliente_api: TestClient) -> None:
    """Valida que el resumen agregue advertencias sobre carencias del dataset."""
    respuesta = cliente_api.get("/microzonas/resumen")
    assert respuesta.status_code == 200

    cuerpo = respuesta.json()
    assert cuerpo["total_microzonas"] == 2
    assert cuerpo["mediana_ratio"] == 0.9
    advertencias = set(cuerpo["advertencias_globales"])
    assert "Todas las microzonas reportan longitud de red de agua igual a cero." in advertencias
    assert "Solo se registran proyectos activos en una microzona o ninguna." in advertencias
