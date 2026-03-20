# Radar Económico de Mérida

Dashboard geoespacial end-to-end desarrollado con Python y Streamlit
para analizar la actividad económica de Mérida, Yucatán.

## Estado actual

**Fase 2 · ETL versionado y dashboard analítico**

En esta fase ya se implementa la capa funcional principal del proyecto:

- extracción automática desde DENUE
- almacenamiento de datos raw en JSON y Parquet
- comparación por hash del último raw procesado
- carga o descarga de la capa geográfica de Mérida
- transformación geoespacial con enriquecimiento por AGEB
- generación de dataset `latest`
- generación de snapshots históricos configurables
- dashboard analítico en Streamlit con mapa, filtros, KPIs y tablas de ejecución

## Pendientes recomendados

- mejorar la cobertura de pruebas del pipeline y del dashboard
- separar dependencias de runtime y desarrollo
- endurecer validación de esquema de datos procesados
- agregar CI, linting y type checking
- modularizar más la app de Streamlit

---

## Objetivo del proyecto

Construir una solución end-to-end que permita:

- consumir fuentes públicas sobre actividad económica de Mérida
- ejecutar un ETL manual-on-demand
- detectar cambios entre ejecuciones
- almacenar históricos versionados
- alimentar un dashboard geoespacial en Streamlit

---

## Arquitectura actual

```text
Fuentes públicas
   ↓
Extracción DENUE
   ↓
raw data (JSON + Parquet)
   ↓
validación / hash
   ↓
transformación geoespacial
   ↓
latest + snapshots
   ↓
dashboard en Streamlit
```

---

## Configuración principal

La configuración se controla desde `config/settings.yaml`.

Parámetros importantes:

- `etl.enabled`: activa o desactiva el pipeline
- `etl.compare_hash`: evita reprocesar si el raw más reciente no cambió
- `etl.save_snapshots`: controla si además de `latest` se guardan snapshots históricos
- `denue.queries`: términos de búsqueda contra la API
- `merida.local_geojson_path`: capa geográfica local usada para el join espacial

---

## Componentes principales

- `etl/extract_denue.py`: extracción de datos desde DENUE
- `etl/check_updates.py`: cálculo y comparación de hash
- `etl/transform_data.py`: limpieza, georreferenciación y join espacial
- `etl/build_snapshot.py`: persistencia de `latest` y snapshots
- `etl/run_pipeline.py`: orquestación del pipeline
- `app/main.py`: dashboard principal en Streamlit

---

## Ejecución local

### Pipeline

```bash
python -m etl.run_pipeline
```

### App

```bash
streamlit run app/main.py
```
