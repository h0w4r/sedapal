from __future__ import annotations

from pathlib import Path
from typing import Iterable, cast

import pandas as pd


CLAVE_MICROZONA: list[str] = [
    "ubigeo",
    "distrito",
    "gerencia_servicios",
    "equipo_comercial",
    "anio",
    "mes",
]

COLUMNAS_OBLIGATORIAS: set[str] = {
    "gerencia_servicios",
    "equipo_comercial",
    "departamento",
    "provincia",
    "distrito",
    "ubigeo",
    "tarifa",
    "conexiones_agua",
    "conexiones_alcantarillado",
    "fecha_corte",
}


def cargar_conexiones(ruta_archivo: Path) -> pd.DataFrame:
    """Carga el archivo de conexiones en un DataFrame de pandas (tabla en memoria para manipular datos).

    Parámetros
    ----------
    ruta_archivo : Path
        Ruta del archivo CSV. `Path` proviene de `pathlib` y facilita el manejo seguro de rutas.

    Retorna
    -------
    pd.DataFrame
        Datos crudos listos para su limpieza.
    """
    ruta: Path = _asegurar_path(ruta_archivo)
    if not ruta.exists():
        raise FileNotFoundError(f"No se encontró el archivo de conexiones en {ruta}")

    # pandas.read_csv lee el CSV respetando la codificación indicada.
    return pd.read_csv(ruta, encoding="latin-1", dtype=str)


def limpiar_conexiones(tabla_conexiones: pd.DataFrame) -> pd.DataFrame:
    """Normaliza y valida la tabla de conexiones.

    Esta función aplica reglas de negocio básicas para garantizar tipos correctos, rangos válidos y
    claves consistentes antes de la integración con otros datasets.

    Parámetros
    ----------
    tabla_conexiones : pd.DataFrame
        Tabla cruda de conexiones.

    Retorna
    -------
    pd.DataFrame
        Tabla agregada por microzona con columnas listas para indicadores.
    """
    df_conexiones = tabla_conexiones.copy()

    df_conexiones.columns = [columna.strip().lower() for columna in df_conexiones.columns]
    _validar_columnas_obligatorias(df_conexiones.columns)

    _normalizar_cadenas(df_conexiones, columnas=["gerencia_servicios", "equipo_comercial"])
    _normalizar_cadenas(
        df_conexiones, columnas=["departamento", "provincia", "distrito"], preservar_espacios=False
    )
    _normalizar_tarifa(df_conexiones)
    _sanear_ubigeo(df_conexiones)
    _normalizar_valores_enteros(
        df_conexiones,
        columnas=["conexiones_agua", "conexiones_alcantarillado"],
        minimo=0,
    )
    _parsear_fecha_corte(df_conexiones)
    _normalizar_componentes_temporales(df_conexiones)
    df_conexiones = _agrupar_por_microzona(df_conexiones)

    return df_conexiones


def _asegurar_path(ruta: Path | str) -> Path:
    """Convierte la entrada a Path para manejar rutas de forma uniforme."""
    return ruta if isinstance(ruta, Path) else Path(ruta)


def _validar_columnas_obligatorias(columnas: Iterable[str]) -> None:
    """Verifica que el DataFrame contenga las columnas indispensables."""
    faltantes = COLUMNAS_OBLIGATORIAS.difference(columnas)
    if faltantes:
        raise ValueError(f"Faltan columnas obligatorias: {sorted(faltantes)}")


def _normalizar_cadenas(
    df: pd.DataFrame,
    columnas: Iterable[str],
    *,
    preservar_espacios: bool = False,
) -> None:
    """Lleva a mayúsculas y elimina espacios inconsistentes en columnas de texto."""
    for columna in columnas:
        if columna not in df:
            continue
        serie = (
            df[columna]
            .fillna("")
            .astype(str)
        )
        if preservar_espacios:
            df[columna] = serie.str.upper().str.replace(r"\s+", " ", regex=True)
        else:
            df[columna] = serie.str.strip().str.upper()


def _normalizar_tarifa(df: pd.DataFrame) -> None:
    """Estandariza el campo de tarifa según categorías conocidas."""
    if "tarifa" not in df:
        return
    categorias_validas = {
        "SOCIAL",
        "DOMESTICO",
        "COMERCIAL",
        "INDUSTRIAL",
        "ESTATAL",
    }
    series = df["tarifa"].fillna("").astype(str).str.strip().str.upper()
    df["tarifa"] = series.where(series.isin(categorias_validas), "OTRAS")


def _sanear_ubigeo(df: pd.DataFrame) -> None:
    """Garantiza que el UBIGEO tenga seis dígitos."""
    serie = (
        df["ubigeo"]
        .fillna("")
        .astype(str)
        .str.replace(r"\D", "", regex=True)
        .str.zfill(6)
    )
    mascara_valida = serie.str.fullmatch(r"\d{6}")
    df["ubigeo"] = serie.where(mascara_valida, pd.NA)


def _normalizar_valores_enteros(
    df: pd.DataFrame,
    *,
    columnas: Iterable[str],
    minimo: int | None = None,
) -> None:
    """Convierte columnas a enteros, aplicando límites inferiores cuando se indiquen."""
    for columna in columnas:
        if columna not in df:
            continue
        serie = cast(pd.Series, pd.to_numeric(df[columna], errors="coerce"))
        if minimo is not None:
            serie = cast(pd.Series, serie.clip(lower=minimo))
        serie = cast(pd.Series, serie.fillna(0))
        df[columna] = serie.round().astype("Int64")


def _parsear_fecha_corte(df: pd.DataFrame) -> None:
    """Transforma la cadena AAAAMMDD en un objeto datetime de pandas."""
    df["fecha_corte"] = pd.to_datetime(df["fecha_corte"].astype(str), format="%Y%m%d", errors="coerce")


def _normalizar_componentes_temporales(df: pd.DataFrame) -> None:
    """Completa y valida las columnas de año y mes con rangos seguros."""
    if "anio" not in df:
        df["anio"] = pd.NA
    if "mes" not in df:
        df["mes"] = pd.NA

    series_anio = cast(pd.Series, pd.to_numeric(df["anio"], errors="coerce"))
    series_mes = cast(pd.Series, pd.to_numeric(df["mes"], errors="coerce"))

    series_anio = series_anio.where(series_anio.between(2000, 2100), other=pd.NA)
    series_mes = series_mes.where(series_mes.between(1, 12), other=pd.NA)

    df["anio"] = series_anio.fillna(df["fecha_corte"].dt.year).round().astype("Int64")
    df["mes"] = series_mes.fillna(df["fecha_corte"].dt.month).round().astype("Int64")


def _agrupar_por_microzona(df: pd.DataFrame) -> pd.DataFrame:
    """Resume las conexiones por clave de microzona."""
    campos_texto = {
        "departamento": lambda serie: _obtener_moda(serie),
        "provincia": lambda serie: _obtener_moda(serie),
        "tarifa_predominante": ("tarifa", _obtener_moda),
    }

    agregaciones = {
        "conexiones_agua": ("conexiones_agua", "sum"),
        "conexiones_alcantarillado": ("conexiones_alcantarillado", "sum"),
        "fecha_corte": ("fecha_corte", "max"),
    }

    resumen = df.groupby(CLAVE_MICROZONA, dropna=False).agg(
        **agregaciones,
    )
    resumen = resumen.reset_index()

    for columna, funcion in campos_texto.items():
        if isinstance(funcion, tuple):
            origen, callable_func = funcion
            resumen[columna] = df.groupby(CLAVE_MICROZONA, dropna=False)[origen].agg(callable_func).reset_index(drop=True)
        else:
            resumen[columna] = df.groupby(CLAVE_MICROZONA, dropna=False)[columna.replace("_predominante", "")].agg(funcion).reset_index(drop=True)

    return resumen


def _obtener_moda(serie: pd.Series) -> object:
    """Devuelve el valor más frecuente de una serie, o `pd.NA` si está vacío."""
    serie_filtrada = serie.dropna()
    if serie_filtrada.empty:
        return pd.NA
    moda = serie_filtrada.mode()
    if moda.empty:
        return pd.NA
    return moda.iloc[0]


__all__ = [
    "CLAVE_MICROZONA",
    "COLUMNAS_OBLIGATORIAS",
    "cargar_conexiones",
    "limpiar_conexiones",
]
