"""Punto de entrada de la API FastAPI que expone indicadores de microzonas críticas."""

from __future__ import annotations

from typing import List

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.dependencies import limpiar_caches, obtener_configuracion_servicio
from app.routers.microzonas import router as router_microzonas

def crear_aplicacion() -> FastAPI:
    """Crea y configura la aplicación FastAPI con sus middlewares y routers."""
    configuracion = obtener_configuracion_servicio()

    aplicacion = FastAPI(
        title="API de Microzonas Críticas Sedapal",
        description=(
            "Servicio que calcula el índice de criticidad para microzonas de Sedapal, "
            "exponiendo filtros, advertencias y métricas globales."
        ),
        version="1.0.0",
        contact={
            "name": "Equipo de Analítica Sedapal",
            "email": "analitica@sedapal.pe",
        },
    )

    origenes: List[str] = ["*"]
    if configuracion.origenes_permitidos:
        origenes.extend(origen.strip() for origen in configuracion.origenes_permitidos.split(",") if origen.strip())
        origenes = list(dict.fromkeys(origenes))

    # Se agrega el middleware de CORS permitiendo cualquier dominio al incluir '*' en allow_origins.
    # Se deshabilitan las credenciales porque con '*' Starlette/FastAPI no permiten allow_credentials=True.
    aplicacion.add_middleware(
        CORSMiddleware,
        allow_origins=origenes,
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    aplicacion.include_router(router_microzonas)

    @aplicacion.get(
        "/",
        summary="Verificar estado del servicio.",
        tags=["salud"],
    )
    def estado_servicio() -> dict[str, str]:
        """Devuelve un mensaje simple para comprobar que la API responde correctamente."""
        return {"estado": "operativo", "descripcion": "API de microzonas críticas lista para recibir solicitudes."}

    @aplicacion.on_event("shutdown")
    def limpiar_recursos() -> None:
        """Limpia caches en el apagado para asegurar un estado consistente en futuras ejecuciones."""
        limpiar_caches()

    return aplicacion

app = crear_aplicacion()
