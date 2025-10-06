# ETL de microzonas SEDAPAL

## Objetivo general

Unificar las fuentes oficiales de SEDAPAL para generar un dataset analítico `data/processed/microzonas_sedapal.csv` que permita priorizar microzonas críticas sin acceso adecuado al agua. El proceso ETL (Extracción, Transformación y Carga: pipeline que obtiene datos, los limpia y los guarda) integra conexiones, proyectos y longitudes de red dentro del ámbito Lima Metropolitana y para el periodo 2023-2025.

## Estructura del repositorio

```
.
├── data/
│   ├── raw/                # Descargas originales de SEDAPAL
│   └── processed/          # Resultados procesados
├── docs/
│   └── README_etl.md       # Este documento
├── src/
│   ├── __init__.py
│   ├── constantes.py       # Clave de microzona compartida
│   ├── etl_sedapal.py      # Orquestador principal del ETL
│   ├── limpieza_conexiones.py
│   ├── limpieza_longitudes.py
│   └── limpieza_proyectos.py
└── tests/
    ├── fixtures/           # Datos de prueba sintéticos (CSV de ejemplo)
    │   ├── conexiones.csv
    │   ├── longitudes.csv
    │   └── proyectos.csv
    └── test_etl_sedapal.py
```

## Dependencias

Las dependencias se listan en `requirements.txt`:

- `pandas`: manipulación de DataFrames (tablas en memoria para manipular datos).
- `requests`: descargas HTTP con cabeceras personalizadas.
- `python-dateutil`: parseo flexible de fechas.
- `chardet`: detección de codificación de archivos.
- `pytest`: ejecución de pruebas automatizadas.

Instalación sugerida (usar entorno virtual `.venv`):

```bash
python -m venv .venv
.venv\Scripts\activate  # Windows
pip install -r requirements.txt
```

## Fuentes de datos requeridas

Depositar los archivos originales en `data/raw/` con los siguientes nombres (ajustar si cambian):

- `conexiones.csv`: detalle de conexiones de agua y alcantarillado.
- `longitudes_red.csv`: longitud de redes primarias y secundarias por clase (`Agua`, `Desague`).
- `proyectos.csv`: catálogo de proyectos con estado y avance.

## Pipeline de ejecución

1. **Descarga** (`descargar_dataset`): permite replicar la descarga cuando se tienen URL públicas.
2. **Carga y limpieza**:
   - `limpieza_conexiones.cargar_conexiones` + `limpiar_conexiones`
   - `limpieza_longitudes.cargar_longitudes` + `limpiar_longitudes`
   - `limpieza_proyectos.cargar_proyectos` + `limpiar_proyectos`
3. **Integración**:
   - `enriquecer_microzonas`: merge por clave compuesta `CLAVE_MICROZONA`.
4. **Indicadores**:
   - `calcular_indicadores`: ratios, densidades y banderas de calidad.
5. **Exportación**:
   - `guardar_resultados`: crea `data/processed/microzonas_sedapal.csv`.

### Ejecución manual

```python
from pathlib import Path
from src.etl_sedapal import ejecutar_etl

ruta_salida = ejecutar_etl(
    ruta_conexiones=Path("data/raw/conexiones.csv"),
    ruta_longitudes=Path("data/raw/longitudes_red.csv"),
    ruta_proyectos=Path("data/raw/proyectos.csv"),
    ruta_salida=Path("data/processed/microzonas_sedapal.csv"),
)

print(f"Dataset generado en: {ruta_salida}")
```

## Pruebas automatizadas

Se incluyeron datasets sintéticos y un archivo de pruebas `tests/test_etl_sedapal.py` que ejercitan los módulos de limpieza, integración e indicadores.  
`pytest` (marco de pruebas unitarias en Python que permite definir casos con `assert`) ejecuta todas las verificaciones:

```bash
pytest
```

El comando anterior debe correrse en el entorno virtual con las dependencias instaladas.  
Las pruebas también generan un CSV temporal para validar `ejecutar_etl` de punta a punta.

## Servicio API de microzonas

La carpeta `app/` aloja la API construida con FastAPI (framework web asíncrono que facilita exponer servicios HTTP con validación automática). Los esquemas de entrada y salida se modelan con Pydantic (librería que valida datos mediante modelos tipados) y las dependencias se definen en `app/dependencies.py`.

### Ejecución local

```bash
uvicorn app.main:app --reload
```

`uvicorn` es un servidor ASGI (servidor web asíncrono para Python) que permite correr la aplicación en modo desarrollo. El parámetro `--reload` reinicia la aplicación al detectar cambios en el código.

### Endpoints principales

- `GET /` comprueba el estado del servicio.
- `GET /microzonas` lista microzonas con filtros, paginación y advertencias.
- `GET /microzonas/criticas` prioriza microzonas categorizadas como críticas según el índice calculado.
- `GET /microzonas/resumen` expone estadísticas globales y mensajes sobre la calidad del dataset.
- `GET /microzonas/{ubigeo}` entrega el detalle completo de una microzona identificada por su UBIGEO (código geográfico peruano de 6 dígitos).

Cada respuesta usa los modelos definidos en `app/models.py`, garantizando consistencia en nombres y tipos.

### Consideraciones de datos

- Varias microzonas tienen longitud de red igual a cero en la fuente original, por lo que los indicadores de densidad pueden ser 0 incluso tras la integración.
- Solo se encontró un registro con proyectos activos durante el análisis inicial, de modo que el endpoint de críticos puede devolver pocos resultados si se filtra estrictamente por esa condición.
- Existen registros con ratios de alcantarillado altos, pero se marcan banderas (`registros_inconsistentes`) cuando `conexiones_alcantarillado` supera `conexiones_agua`.

## Ejecución con datos reales

1. `python scripts/descargar_datasets.py`: script en Python (archivo ejecutable) que usa `descargar_dataset` para bajar los tres CSV oficiales y guardarlos en `data/raw/`.
2. `python scripts/ejecutar_pipeline.py`: script que invoca `ejecutar_etl` (función orquestadora que limpia, integra y exporta) y genera `data/processed/microzonas_sedapal.csv`.

Resumen con la versión descargada el 2024-04-18:

- Registros totales: 53 microzonas.
- Microzonas con datos de proyectos activos: 1 (los datasets de origen casi no incluyen `ETAPA` distinta de “CERRADO”).
- Microzonas con indicadores inconsistentes: 2 (casos donde `conexiones_alcantarillado` supera `conexiones_agua`; la bandera `registros_inconsistentes` queda en 1).
- Densidad promedio de red de agua: 0.0 (el archivo de longitudes publica 0 metros para la mayoría de combinaciones, por eso los indicadores quedan en cero).
- Las columnas `gerencia_servicios` y `equipo_comercial` llegan vacías en la fuente, por lo que se completan con valores `NA`.

## Formato de salida

Columnas principales (resumen):

- Clave: `ubigeo`, `distrito`, `gerencia_servicios`, `equipo_comercial`, `anio`, `mes`.
- Conexiones: `conexiones_agua`, `conexiones_alcantarillado`.
- Longitudes: `longitud_total_agua`, `longitud_total_desague`, `densidad_red_agua`, `densidad_red_desague`.
- Proyectos: `conteo_proyectos_activos`, `avance_promedio_proyectos`.
- Banderas: `faltan_datos_proyectos`, `faltan_datos_longitud`, `registros_inconsistentes`.

Todas las columnas están en UTF-8, separador por defecto (coma) y sin índice.

## Pendientes

- Ejecutar `ejecutar_etl` con los datasets reales ubicados en `data/raw/` para validar el pipeline completo.
- Revisar el CSV final `data/processed/microzonas_sedapal.csv` con los indicadores reales y documentar hallazgos adicionales.
