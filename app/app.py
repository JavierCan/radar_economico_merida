from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from app.charts import count_table, donut_chart, heatmap_chart, horizontal_bar_chart
from app.data import latest_file, load_geojson, load_latest_dataset
from app.filters import apply_filters, normalize_text_series
from app.map_view import build_map

st.set_page_config(page_title="Radar Económico de Mérida", layout="wide")

LATEST_PATH = Path("data/processed/radar_merida_latest.parquet")
MANIFEST_PATH = Path("data/metadata/manifest.csv")
METADATA_DIR = Path("data/metadata")
SNAPSHOTS_DIR = Path("data/snapshots")
MERIDA_LAYER_PATH = Path("data/external/merida_limites.geojson")

st.title("Radar Económico de Mérida")
st.caption("Dashboard geoespacial con filtros, KPIs y visualización analítica")

if not LATEST_PATH.exists():
    st.warning("Aún no existe el dataset procesado. Ejecuta primero el pipeline.")
    st.stop()

df = load_latest_dataset(LATEST_PATH)
geojson_data = load_geojson(MERIDA_LAYER_PATH)
latest_summary = latest_file(METADATA_DIR, "pipeline_summary_*.csv")
snapshots_count = len(list(SNAPSHOTS_DIR.glob("radar_merida_*.parquet")))

filtered_df = apply_filters(df)

total_establecimientos = len(filtered_df)
colonias_unicas = (
    normalize_text_series(filtered_df["colonia"]).replace("", pd.NA).dropna().nunique()
    if "colonia" in filtered_df.columns else 0
)
ageb_unicas = (
    normalize_text_series(filtered_df["CVE_AGEB"]).replace("", pd.NA).dropna().nunique()
    if "CVE_AGEB" in filtered_df.columns else 0
)
clases_unicas = (
    normalize_text_series(filtered_df["clase_actividad"]).replace("", pd.NA).dropna().nunique()
    if "clase_actividad" in filtered_df.columns else 0
)
terminos_unicos = (
    normalize_text_series(filtered_df["search_term"]).replace("", pd.NA).dropna().nunique()
    if "search_term" in filtered_df.columns else 0
)

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Establecimientos", f"{total_establecimientos:,}")
k2.metric("Colonias", f"{colonias_unicas:,}")
k3.metric("AGEB", f"{ageb_unicas:,}")
k4.metric("Clases actividad", f"{clases_unicas:,}")
k5.metric("Snapshots", f"{snapshots_count:,}")

st.markdown("### Panel principal")
main_left, main_right = st.columns([1.8, 1], gap="large")

with main_left:
    with st.container(border=True):
        st.subheader("Mapa interactivo")
        build_map(filtered_df, geojson_data)

with main_right:
    with st.container(border=True):
        st.subheader("Resumen general")
        st.write(f"Archivo latest: `{LATEST_PATH.name}`")
        st.write(f"Columnas: **{len(filtered_df.columns)}**")
        st.write(f"Términos activos: **{terminos_unicos}**")
        st.write(f"Registros filtrados: **{len(filtered_df):,}**")

    with st.container(border=True):
        st.subheader("Distribución por término")
        donut_chart(
            count_table(filtered_df, "search_term", limit=10),
            "search_term",
            "Participación por término de búsqueda",
        )

st.markdown("### Panel analítico")

r1c1, r1c2 = st.columns(2, gap="large")
with r1c1:
    with st.container(border=True):
        st.subheader("Top colonias")
        horizontal_bar_chart(
            count_table(filtered_df, "colonia", limit=15),
            "colonia",
            "Top 15 colonias por registros",
        )

with r1c2:
    with st.container(border=True):
        st.subheader("Distribución por estrato")
        horizontal_bar_chart(
            count_table(filtered_df, "estrato", limit=10),
            "estrato",
            "Registros por estrato",
        )

r2c1, r2c2 = st.columns(2, gap="large")
with r2c1:
    with st.container(border=True):
        st.subheader("Top AGEB")
        horizontal_bar_chart(
            count_table(filtered_df, "CVE_AGEB", limit=15),
            "CVE_AGEB",
            "Top 15 AGEB por registros",
        )

with r2c2:
    with st.container(border=True):
        st.subheader("Top clases de actividad")
        horizontal_bar_chart(
            count_table(filtered_df, "clase_actividad", limit=15),
            "clase_actividad",
            "Top 15 clases de actividad",
        )

with st.container(border=True):
    st.subheader("Cruce analítico")
    heatmap_chart(
        filtered_df,
        "search_term",
        "estrato",
        "Heatmap: término de búsqueda vs estrato",
    )

st.markdown("### Datos")

with st.container(border=True):
    st.subheader("Vista previa del dataset filtrado")
    preview_cols = [
        c
        for c in [
            "nombre_establecimiento",
            "clase_actividad",
            "estrato",
            "tipo_establecimiento",
            "colonia",
            "codigo_postal",
            "search_term",
            "CVE_AGEB",
            "latitud",
            "longitud",
        ]
        if c in filtered_df.columns
    ]

    if preview_cols:
        st.dataframe(
            filtered_df[preview_cols].head(200),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.dataframe(filtered_df.head(200), use_container_width=True, hide_index=True)

st.markdown("### Ejecuciones")
exec_left, exec_right = st.columns(2, gap="large")

with exec_left:
    with st.container(border=True):
        st.subheader("Último resumen de ejecución")
        if latest_summary is not None:
            summary_df = pd.read_csv(latest_summary)
            st.dataframe(summary_df, use_container_width=True, hide_index=True)
        else:
            st.info("No existe resumen de ejecución.")

with exec_right:
    with st.container(border=True):
        st.subheader("Historial de ejecuciones")
        if MANIFEST_PATH.exists():
            manifest_df = pd.read_csv(MANIFEST_PATH)
            if "run_timestamp" in manifest_df.columns:
                manifest_df = manifest_df.sort_values(by="run_timestamp", ascending=False)
            st.dataframe(manifest_df, use_container_width=True, hide_index=True)
        else:
            st.info("No existe manifest todavía.")
