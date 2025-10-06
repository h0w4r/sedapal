# ETL y API de microzonas SEDAPAL

## Objetivo

Unificar fuentes oficiales de SEDAPAL para generar el dataset analítico `data/processed/microzonas_sedapal.csv`, capaz de priorizar microzonas con acceso deficiente al agua. El pipeline ETL (Extracción, Transformación y Carga: proceso que obtiene datos, los depura y los persiste) combina información de conexiones, proyectos y longitudes entre 2023 y 2025.

## Características principales

- Pipeline reproducible que limpia y enriquece datos de `data/raw/`.
- API REST construida con FastAPI (framework web asíncrono que simplifica publicar servicios HTTP con validaciones automáticas) para explorar microzonas y sus indicadores.
- Suite de pruebas con pytest (marco de pruebas unitarias en Python que usa aserciones) y datasets sintéticos en `tests/fixtures/`.
- Scripts auxiliares para descargar fuentes, ejecutar el ETL y validar el modelo de microzonas.

## Requisitos previos

- Python 3.10 o superior (intérprete del lenguaje Python).
- Git (control de versiones distribuido) para clonar el repositorio.
- Acceso a los CSV originales de SEDAPAL si se desea replicar los resultados reales.

## Instalación

1. Clonar el repositorio:

```bash
git clone https://github.com/h0w4r/sedapal.git
cd sedapal
```

2. Crear y activar un entorno virtual con `venv` (entorno aislado de dependencias Python):

```bash
python -m venv .venv
.venv\Scripts\activate
```

3. Instalar dependencias listadas en `requirements.txt`:

```bash
pip install -r requirements.txt
```

Dependencias destacadas:
- `pandas`: manipulación de DataFrames (tablas en memoria).
- `requests`: cliente HTTP para descargar archivos con cabeceras personalizadas.
- `python-dateutil`: interpretación flexible de fechas.
- `chardet`: detección automática de codificaciones.
- `fastapi`: exposición de endpoints HTTP.
- `pydantic`: modelos de datos con validación basada en tipos.
- `uvicorn`: servidor ASGI (servidor web asíncrono) usado para ejecutar FastAPI.

## Uso del pipeline ETL

### Ejecución automatizada con scripts

```bash
python scripts/descargar_datasets.py
python scripts/ejecutar_pipeline.py
```

- `scripts/descargar_datasets.py` emplea `descargar_dataset` (función que baja y guarda archivos remotos) para poblar `data/raw/`.
- `scripts/ejecutar_pipeline.py` invoca `ejecutar_etl` (función orquestadora que limpia, integra y exporta) y produce `data/processed/microzonas_sedapal.csv`.

### Ejecución manual en Python

```python
from pathlib import Path
from src.etl_sedapal import ejecutar_etl

# Definir rutas de entrada y salida con objetos Path para facilitar el manejo de archivos
ruta_salida = ejecutar_etl(
    ruta_conexiones=Path("data/raw/conexiones.csv"),
    ruta_longitudes=Path("data/raw/longitudes.csv"),
    ruta_proyectos=Path("data/raw/proyectos.csv"),
    ruta_salida=Path("data/processed/microzonas_sedapal.csv"),
)

# Mostrar la ruta final del dataset procesado
print(f"Dataset generado en: {ruta_salida}")
```

Funciones relevantes (con breve descripción en lenguaje sencillo):
- `cargar_conexiones`, `cargar_longitudes`, `cargar_proyectos`: lectura de CSV hacia DataFrames.
- `limpiar_conexiones`, `limpiar_longitudes`, `limpiar_proyectos`: depuración de columnas y tipos.
- `enriquecer_microzonas`: unión de tablas por la clave `CLAVE_MICROZONA`.
- `calcular_indicadores`: cálculo de ratios y banderas de calidad.
- `guardar_resultados`: exportación a CSV final.

## Servicio API

La carpeta `app/` contiene la API expuesta con FastAPI.

### Arranque local

```bash
uvicorn app.main:app --reload
```

- `uvicorn` (servidor ASGI) ejecuta la aplicación en modo desarrollo.
- El parámetro `--reload` reinicia el servidor cuando detecta cambios en el código.

### Endpoints destacados

- `GET /`: prueba de salud del servicio.
- `GET /microzonas`: listado con filtros y paginación.
- `GET /microzonas/criticas`: microzonas con mayor prioridad según indicadores.
- `GET /microzonas/resumen`: estadísticas agregadas del dataset.
- `GET /microzonas/{ubigeo}`: detalle completo usando el UBIGEO (código geográfico peruano de seis dígitos).

Los modelos de entrada y salida están definidos con Pydantic (librería que valida datos mediante clases tipadas), asegurando consistencia en nombres y formatos.

## Pruebas automatizadas

```bash
pytest
```

- `pytest` ejecuta los casos unitarios para módulos de limpieza, integración e indicadores.
- Los archivos de prueba usan fixtures (datos de prueba predefinidos) ubicados en `tests/fixtures/`.
- El flujo de punta a punta verifica que `ejecutar_etl` genere un CSV consistente en un directorio temporal.

## Estructura del repositorio

```
.
├── app/                     # Código de la API FastAPI y sus dependencias
├── config/                  # Parámetros de negocio (criterios de criticidad)
├── data/
│   ├── raw/                 # Descargas originales de SEDAPAL
│   └── processed/           # Resultados generados por el ETL
├── docs/
│   └── README_etl.md        # Documentación técnica detallada del pipeline
├── scripts/                 # Herramientas CLI para descarga y ejecución del ETL
├── src/                     # Fuente del ETL
│   ├── etl_sedapal.py       # Orquestador principal
│   ├── constantes.py        # Claves y parámetros comunes
│   ├── limpieza_*.py        # Módulos de limpieza y transformación
│   └── analytics/           # Indicadores y servicios analíticos
└── tests/                   # Pruebas y datos sintéticos
```

## Datos generados

El archivo `data/processed/microzonas_sedapal.csv` contiene, entre otras, las columnas:

- Identificación: `ubigeo`, `distrito`, `gerencia_servicios`, `equipo_comercial`, `anio`, `mes`.
- Métricas de conexiones: `conexiones_agua`, `conexiones_alcantarillado`.
- Métricas de red: `longitud_total_agua`, `longitud_total_desague`, `densidad_red_agua`, `densidad_red_desague`.
- Proyectos: `conteo_proyectos_activos`, `avance_promedio_proyectos`.
- Banderas de calidad: `faltan_datos_proyectos`, `faltan_datos_longitud`, `registros_inconsistentes`.

Consideraciones:
- Varias microzonas carecen de longitudes reportadas (densidades resultan 0).
- Solo existe un proyecto activo en la muestra base, lo que limita resultados en `criticos`.
- Se activa la bandera `registros_inconsistentes` cuando las conexiones de alcantarillado superan a las de agua.

## Buenas prácticas y soporte

- Mantener los CSV originales en UTF-8 para evitar problemas de codificación.
- Documentar hallazgos adicionales en `docs/` al ejecutar el ETL con nuevas fuentes.
- Abrir issues o pull requests describiendo claramente el problema o la contribución propuesta.
