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