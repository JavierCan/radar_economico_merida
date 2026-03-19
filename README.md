# Radar Económico de Mérida

Dashboard geoespacial end-to-end desarrollado con Python y Streamlit para analizar la actividad económica de Mérida, Yucatán.

## Estado actual

**Fase 1 · Extracción inicial**

En esta fase ya se implementa la primera capa funcional del proyecto:

- extracción automática de la fuente principal
- almacenamiento de datos raw
- carga o descarga de la capa geográfica de Mérida
- validación operativa desde Streamlit
- preparación de la base para la fase 2

Todavía no se implementan:
- comparación de cambios por hash
- snapshots históricos
- transformación analítica final
- mapa interactivo definitivo
- timeline de versiones

---

## Objetivo del proyecto

Construir una solución end-to-end que permita:

- consumir fuentes públicas sobre actividad económica de Mérida
- ejecutar un ETL manual-on-demand
- detectar cambios en futuras fases
- almacenar históricos
- alimentar un dashboard geoespacial en Streamlit

---

## Arquitectura planeada

```text
Fuentes públicas
   ↓
ETL manual-on-demand
   ↓
raw data
   ↓
transformación y versionado
   ↓
latest + snapshots
   ↓
dashboard en Streamlit