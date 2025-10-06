from __future__ import annotations

import importlib
from pathlib import Path
from typing import Iterable, Protocol, cast

import pandas as pd

class _ModuloChardet(Protocol):
    """Contrato mínimo requerido del módulo chardet (detecta codificaciones)."""

    def detect(self, data: bytes, **kwargs: object) -> dict[str, object]:
        ...

COLUMNAS_OBLIGATORIAS_PROYECTO: set[str] = {
    "gerencia_servicios",
    "equipo_comercial",
    "departamento",
    "provincia",
    "distrito",
    "ubigeo",
    "nombre_proyecto",
    "etapa",
    "avance_fisico",
    "fecha_inicio",
    "fecha_fin",
    "fecha_corte",
    "costo_total",
    "contratista_consultor",
}

ETAPAS_CANONICAS: dict[str, str] = {
    "EXPEDIENTE TÉCNICO": "EXPEDIENTE TECNICO",
    "EXPEDIENTE TECNICO": "EXPEDIENTE TECNICO",
    "OBRA": "OBRA",
    "EJECUCION": "OBRA",
    "LIQUIDACION": "LIQUIDACION",
    "CERRADO": "CERRADO",
    "PARALIZADO": "PARALIZADO",
    "ESTUDIO DEFINITIVO": "EXPEDIENTE TECNICO",
}

def cargar_proyectos(ruta_archivo: Path) -> pd.DataFrame:
    """Carga el CSV de proyectos detectando codificación automáticamente con chardet.

    chardet es una librería que identifica la codificación (encoding) de archivos de texto.
    """
    ruta: Path = _asegurar_path(ruta_archivo)
    if not ruta.exists():
        raise FileNotFoundError(f"No se encontró el archivo de proyectos en {ruta}")

    encoding_detectado = _detectar_codificacion(ruta)
    return pd.read_csv(ruta, encoding=encoding_detectado, dtype=str)

def limpiar_proyectos(tabla_proyectos: pd.DataFrame) -> pd.DataFrame:
    """Normaliza nombres, fechas, montos y etapas para preparar los proyectos."""
    df_proyectos = tabla_proyectos.copy()

    df_proyectos.columns = [columna.strip().lower() for columna in df_proyectos.columns]

    columnas_faltantes_toleradas = {"gerencia_servicios", "equipo_comercial"}
    for columna in columnas_faltantes_toleradas:
        if columna not in df_proyectos.columns:
            df_proyectos[columna] = pd.NA

    _validar_columnas(df_proyectos.columns)

    _normalizar_texto(
        df_proyectos,
        columnas=["gerencia_servicios", "equipo_comercial", "departamento", "provincia"],
    )
    df_proyectos = _normalizar_distritos(df_proyectos)
    _sanear_ubigeo(df_proyectos)
    _normalizar_nombre_proyecto(df_proyectos)
    _normalizar_contratista(df_proyectos)
    _normalizar_etapas(df_proyectos)
    _normalizar_avance(df_proyectos)
    _normalizar_costo(df_proyectos)
    _parsear_fechas(df_proyectos, columnas=["fecha_inicio", "fecha_fin", "fecha_corte"])
    _completar_componentes_temporales(df_proyectos)

    return df_proyectos.reset_index(drop=True)

def _asegurar_path(ruta: Path | str) -> Path:
    """Convierte rutas en objetos Path para manejo uniforme."""
    return ruta if isinstance(ruta, Path) else Path(ruta)

def _detectar_codificacion(ruta: Path, tamanio_muestra: int = 100_000) -> str:
    """Determina la codificación del archivo leyendo una muestra de bytes."""
    modulo_chardet = _obtener_modulo_chardet()
    if modulo_chardet is None:
        return "latin-1"

    with ruta.open("rb") as archivo:
        muestra = archivo.read(tamanio_muestra)

    resultado = modulo_chardet.detect(muestra)
    encoding = cast(str | None, resultado.get("encoding")) if isinstance(resultado, dict) else None
    return encoding or "latin-1"

def _obtener_modulo_chardet() -> _ModuloChardet | None:
    """Carga perezosamente el módulo chardet para evitar importaciones duras en tiempo de carga."""
    try:
        return cast(_ModuloChardet, importlib.import_module("chardet"))
    except ImportError:
        return None

def _validar_columnas(columnas: Iterable[str]) -> None:
    """Confirma que el DataFrame incluya todas las columnas obligatorias."""
    faltantes = COLUMNAS_OBLIGATORIAS_PROYECTO.difference(columnas)
    if faltantes:
        raise ValueError(f"Faltan columnas obligatorias en proyectos: {sorted(faltantes)}")

def _normalizar_texto(df: pd.DataFrame, columnas: Iterable[str]) -> None:
    """Limpia y estandariza texto en mayúsculas."""
    for columna in columnas:
        if columna not in df:
            continue
        df[columna] = (
            df[columna]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.upper()
            .str.replace(r"\s+", " ", regex=True)
        )

def _normalizar_distritos(df: pd.DataFrame) -> pd.DataFrame:
    """Divide valores con múltiples distritos separados por '/' y expande filas."""
    if "distrito" not in df:
        return df
    df["distrito"] = (
        df["distrito"]
        .fillna("")
        .astype(str)
        .str.upper()
        .str.replace(r"\s+", " ", regex=True)
    )
    df["distrito"] = df["distrito"].str.replace(" Y ", "/", regex=False)
    df = df.assign(distrito=df["distrito"].str.split("/"))
    df = df.explode("distrito")
    df["distrito"] = df["distrito"].str.strip()
    df.reset_index(drop=True, inplace=True)
    return df

def _sanear_ubigeo(df: pd.DataFrame) -> None:
    """Garantiza UBIGEO de seis dígitos."""
    serie = (
        df["ubigeo"]
        .fillna("")
        .astype(str)
        .str.replace(r"\D", "", regex=True)
        .str.zfill(6)
    )
    mascara_valida = serie.str.fullmatch(r"\d{6}")
    df["ubigeo"] = serie.where(mascara_valida, pd.NA)

def _normalizar_nombre_proyecto(df: pd.DataFrame) -> None:
    """Elimina caracteres especiales y homogeniza nombres de proyectos."""
    if "nombre_proyecto" not in df:
        return
    df["nombre_proyecto"] = (
        df["nombre_proyecto"]
        .fillna("")
        .astype(str)
        .str.replace(r"[^\w\sÁÉÍÓÚÑÜ-]", " ", regex=True)
        .str.upper()
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )

def _normalizar_contratista(df: pd.DataFrame) -> None:
    """Normaliza la codificación del contratista/consultor."""
    if "contratista_consultor" not in df:
        return
    df["contratista_consultor"] = (
        df["contratista_consultor"]
        .fillna("")
        .astype(str)
        .str.encode("latin-1", errors="ignore")
        .str.decode("latin-1")
        .str.upper()
        .str.strip()
    )

def _normalizar_etapas(df: pd.DataFrame) -> None:
    """Mapea etapas a categorías controladas."""
    if "etapa" not in df:
        return
    df["etapa"] = (
        df["etapa"]
        .fillna("")
        .astype(str)
        .str.upper()
        .str.strip()
    )
    df["etapa"] = df["etapa"].map(ETAPAS_CANONICAS).fillna("SIN ETAPA")

def _normalizar_avance(df: pd.DataFrame) -> None:
    """Convierte el avance físico a porcentaje entre 0 y 100."""
    if "avance_fisico" not in df:
        return
    serie = (
        df["avance_fisico"]
        .fillna("")
        .astype(str)
        .str.replace(",", ".", regex=False)
    )
    serie_num = cast(pd.Series, pd.to_numeric(serie, errors="coerce"))
    serie_num = serie_num.clip(lower=0.0, upper=100.0)
    df["avance_fisico"] = serie_num.fillna(0.0)

def _normalizar_costo(df: pd.DataFrame) -> None:
    """Convierte el costo total a float eliminando símbolos monetarios."""
    if "costo_total" not in df:
        return
    serie = (
        df["costo_total"]
        .fillna("")
        .astype(str)
        .str.replace(r"[^\d,.-]", "", regex=True)
        .str.replace(",", ".", regex=False)
    )
    df["costo_total"] = cast(pd.Series, pd.to_numeric(serie, errors="coerce")).fillna(0.0)

def _parsear_fechas(df: pd.DataFrame, columnas: Iterable[str]) -> None:
    """Convierte columnas de fecha a datetime flexibles."""
    for columna in columnas:
        if columna not in df:
            continue
        df[columna] = pd.to_datetime(
            df[columna].astype(str).str.strip(),
            errors="coerce",
            format="%Y-%m-%d",
        )

def _completar_componentes_temporales(df: pd.DataFrame) -> None:
    """Deriva año y mes desde la fecha de corte."""
    if "anio" not in df:
        df["anio"] = pd.NA
    if "mes" not in df:
        df["mes"] = pd.NA

    serie_anio = cast(pd.Series, pd.to_numeric(df["anio"], errors="coerce"))
    serie_mes = cast(pd.Series, pd.to_numeric(df["mes"], errors="coerce"))

    serie_anio = serie_anio.where(serie_anio.between(2000, 2100), other=pd.NA)
    serie_mes = serie_mes.where(serie_mes.between(1, 12), other=pd.NA)

    serie_anio = serie_anio.fillna(df["fecha_corte"].dt.year)
    serie_mes = serie_mes.fillna(df["fecha_corte"].dt.month)

    df["anio"] = serie_anio.round().astype("Int64")
    df["mes"] = serie_mes.round().astype("Int64")

__all__ = [
    "cargar_proyectos",
    "limpiar_proyectos",
]
