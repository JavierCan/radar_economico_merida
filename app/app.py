from __future__ import annotations

from pathlib import Path
import pandas as pd
import streamlit as st

from etl.common import latest_file

st.set_page_config(page_title="Radar Económico de Mérida", layout="wide")

RAW_DIR = Path("data/raw")
METADATA_DIR = Path("data/metadata")

st.title("Radar Económico de Mérida")
st.caption("Fase 1 · Extracción inicial y validación de fuentes")

latest_denue = latest_file(RAW_DIR, "denue_raw_*.parquet")
latest_summary = latest_file(METADATA_DIR, "phase1_summary_*.csv")

if latest_denue is None:
    st.warning("Aún no existen extracciones raw de DENUE. Ejecuta primero el pipeline.")
    st.stop()

df = pd.read_parquet(latest_denue)

col1, col2, col3 = st.columns(3)
col1.metric("Registros DENUE", len(df))
col2.metric("Columnas", len(df.columns))
col3.metric("Archivo raw actual", latest_denue.name)

st.subheader("Vista previa de la última extracción")
st.dataframe(df.head(100), use_container_width=True)

if latest_summary is not None:
    st.subheader("Último resumen de ejecución")
    summary = pd.read_csv(latest_summary)
    st.dataframe(summary, use_container_width=True)

st.subheader("Columnas disponibles")
st.write(sorted(df.columns.tolist()))