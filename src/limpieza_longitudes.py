from __future__ import annotations

from pathlib import Path
from typing import Iterable, cast

import pandas as pd

from .constantes import CLAVE_MICROZONA

COLUMNAS_OBLIGATORIAS_LONGITUD: set[str] = {
    "gerencia_servicios",
    "equipo_comercial",
    "departamento",
    "provincia",
    "distrito",
    "ubigeo",
    "clase",
    "red_primaria",
    "red_secundaria",
    "anio",
    "mes",
}

CLASES_VALIDAS: set[str] = {"AGUA", "DESAGUE"}

def cargar_longitudes(ruta_archivo: Path) -> pd.DataFrame:
    """Carga el archivo de longitudes de redes como DataFrame de pandas (tabla en memoria para manipular datos).

    Parámetros
    ----------
    ruta_archivo : Path
        Ruta del CSV de longitudes.

    Retorna
    -------
    pd.DataFrame
        Datos listos para limpieza.
    """
    ruta: Path = _asegurar_path(ruta_archivo)
    if not ruta.exists():
        raise FileNotFoundError(f"No se encontró el archivo de longitudes en {ruta}")

    return pd.read_csv(ruta, encoding="latin-1", dtype=str)

def limpiar_longitudes(tabla_longitudes: pd.DataFrame) -> pd.DataFrame:
    """Normaliza valores y resume las longitudes por microzona y clase de red."""
    df_longitudes = tabla_longitudes.copy()

    df_longitudes.columns = [columna.strip().lower() for columna in df_longitudes.columns]
    _validar_columnas(df_longitudes.columns)

    _normalizar_texto(df_longitudes, ["gerencia_servicios", "equipo_comercial"])
    _normalizar_texto(df_longitudes, ["departamento", "provincia", "distrito"], recortar=True)
    _sanear_ubigeo(df_longitudes)

    df_longitudes["clase"] = (
        df_longitudes["clase"].fillna("").astype(str).str.strip().str.upper()
    )
    df_longitudes.loc[~df_longitudes["clase"].isin(CLASES_VALIDAS), "clase"] = pd.NA

    _normalizar_flotantes(
        df_longitudes,
        columnas=["red_primaria", "red_secundaria"],
        minimo=0.0,
    )
    _asegurar_componentes_temporales(df_longitudes)

    return _construir_resumen(df_longitudes)

def _asegurar_path(ruta: Path | str) -> Path:
    """Convierte cualquier cadena de ruta a Path."""
    return ruta if isinstance(ruta, Path) else Path(ruta)

def _validar_columnas(columnas: Iterable[str]) -> None:
    """Verifica que estén presentes las columnas mínimas requeridas."""
    faltantes = COLUMNAS_OBLIGATORIAS_LONGITUD.difference(columnas)
    if faltantes:
        raise ValueError(f"Faltan columnas obligatorias en longitudes: {sorted(faltantes)}")

def _normalizar_texto(
    df: pd.DataFrame,
    columnas: Iterable[str],
    *,
    recortar: bool = False,
) -> None:
    """Aplica mayúsculas y limpieza de espacios en columnas de texto."""
    for columna in columnas:
        if columna not in df:
            continue
        serie = df[columna].fillna("").astype(str)
        df[columna] = (
            serie.str.upper().str.strip()
            if recortar
            else serie.str.upper().str.replace(r"\s+", " ", regex=True)
        )

def _sanear_ubigeo(df: pd.DataFrame) -> None:
    """Estandariza el UBIGEO a seis dígitos."""
    serie = (
        df["ubigeo"]
        .fillna("")
        .astype(str)
        .str.replace(r"\D", "", regex=True)
        .str.zfill(6)
    )
    mascara_valida = serie.str.fullmatch(r"\d{6}")
    df["ubigeo"] = serie.where(mascara_valida, pd.NA)

def _normalizar_flotantes(
    df: pd.DataFrame,
    *,
    columnas: Iterable[str],
    minimo: float | None = None,
) -> None:
    """Convierte columnas a float garantizando límites inferiores."""
    for columna in columnas:
        if columna not in df:
            continue
        serie = cast(pd.Series, pd.to_numeric(df[columna], errors="coerce"))
        if minimo is not None:
            serie = cast(pd.Series, serie.clip(lower=minimo))
        df[columna] = serie.fillna(0.0)

def _asegurar_componentes_temporales(df: pd.DataFrame) -> None:
    """Valida año y mes dentro de rangos esperados."""
    serie_anio = cast(pd.Series, pd.to_numeric(df["anio"], errors="coerce"))
    serie_mes = cast(pd.Series, pd.to_numeric(df["mes"], errors="coerce"))

    serie_anio = serie_anio.where(serie_anio.between(2000, 2100), other=pd.NA)
    serie_mes = serie_mes.where(serie_mes.between(1, 12), other=pd.NA)

    df["anio"] = serie_anio.fillna(pd.NA).astype("Int64")
    df["mes"] = serie_mes.fillna(pd.NA).astype("Int64")

def _construir_resumen(df: pd.DataFrame) -> pd.DataFrame:
    """Devuelve un resumen por microzona con longitudes separadas por clase."""
    agrupado = (
        df.groupby([*CLAVE_MICROZONA, "clase"], dropna=False)[["red_primaria", "red_secundaria"]]
        .sum()
        .reset_index()
    )

    tabla_pivot = agrupado.pivot_table(
        index=CLAVE_MICROZONA,
        columns="clase",
        values=["red_primaria", "red_secundaria"],
        fill_value=0.0,
    )

    tabla_pivot.columns = [
        f"{nombre_valor.lower()}_{clase.lower()}"
        for nombre_valor, clase in tabla_pivot.columns
    ]
    tabla_pivot = tabla_pivot.reset_index()

    for clase in CLASES_VALIDAS:
        clase_minuscula = clase.lower()
        columna_total = f"longitud_total_{clase_minuscula}"
        columna_primaria = f"red_primaria_{clase_minuscula}"
        columna_secundaria = f"red_secundaria_{clase_minuscula}"
        if columna_primaria in tabla_pivot and columna_secundaria in tabla_pivot:
            tabla_pivot[columna_total] = (
                tabla_pivot[columna_primaria] + tabla_pivot[columna_secundaria]
            )
        else:
            tabla_pivot[columna_total] = 0.0

    return tabla_pivot

__all__ = [
    "cargar_longitudes",
    "limpiar_longitudes",
    "CLASES_VALIDAS",
]
