from __future__ import annotations

from pathlib import Path
import json

import altair as alt
import pandas as pd
import geopandas as gpd
import pydeck as pdk
import streamlit as st

st.set_page_config(page_title="Radar Económico de Mérida", layout="wide")

LATEST_PATH = Path("data/processed/radar_merida_latest.parquet")
MANIFEST_PATH = Path("data/metadata/manifest.csv")
METADATA_DIR = Path("data/metadata")
SNAPSHOTS_DIR = Path("data/snapshots")
MERIDA_LAYER_PATH = Path("data/external/merida_limites.geojson")


def latest_file(directory: Path, pattern: str):
    files = sorted(directory.glob(pattern), key=lambda x: x.stat().st_mtime, reverse=True)
    return files[0] if files else None


@st.cache_data
def load_latest_dataset(path: Path) -> pd.DataFrame:
    try:
        return gpd.read_parquet(path)
    except Exception:
        return pd.read_parquet(path)


@st.cache_data
def load_geojson(path: Path):
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def normalize_text_series(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip()


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    filtered = df.copy()

    st.sidebar.header("Filtros")

    if "search_term" in filtered.columns:
        search_options = sorted(
            [x for x in normalize_text_series(filtered["search_term"]).unique().tolist() if x]
        )
        selected_search_terms = st.sidebar.multiselect(
            "Término de búsqueda",
            options=search_options,
            default=[],
        )
        if selected_search_terms:
            filtered = filtered[
                normalize_text_series(filtered["search_term"]).isin(selected_search_terms)
            ].copy()

    if "estrato" in filtered.columns:
        estrato_options = sorted(
            [x for x in normalize_text_series(filtered["estrato"]).unique().tolist() if x]
        )
        selected_estratos = st.sidebar.multiselect(
            "Estrato",
            options=estrato_options,
            default=[],
        )
        if selected_estratos:
            filtered = filtered[
                normalize_text_series(filtered["estrato"]).isin(selected_estratos)
            ].copy()

    if "tipo_establecimiento" in filtered.columns:
        tipo_options = sorted(
            [x for x in normalize_text_series(filtered["tipo_establecimiento"]).unique().tolist() if x]
        )
        selected_tipos = st.sidebar.multiselect(
            "Tipo de establecimiento",
            options=tipo_options,
            default=[],
        )
        if selected_tipos:
            filtered = filtered[
                normalize_text_series(filtered["tipo_establecimiento"]).isin(selected_tipos)
            ].copy()

    if "colonia" in filtered.columns:
        colonia_options = sorted(
            [x for x in normalize_text_series(filtered["colonia"]).unique().tolist() if x]
        )
        selected_colonias = st.sidebar.multiselect(
            "Colonia",
            options=colonia_options,
            default=[],
        )
        if selected_colonias:
            filtered = filtered[
                normalize_text_series(filtered["colonia"]).isin(selected_colonias)
            ].copy()

    if "CVE_AGEB" in filtered.columns:
        ageb_options = sorted(
            [x for x in normalize_text_series(filtered["CVE_AGEB"]).unique().tolist() if x]
        )
        selected_ageb = st.sidebar.multiselect(
            "AGEB",
            options=ageb_options,
            default=[],
        )
        if selected_ageb:
            filtered = filtered[
                normalize_text_series(filtered["CVE_AGEB"]).isin(selected_ageb)
            ].copy()

    if "clase_actividad" in filtered.columns:
        actividad_search = st.sidebar.text_input("Buscar en clase de actividad")
        if actividad_search.strip():
            filtered = filtered[
                normalize_text_series(filtered["clase_actividad"]).str.contains(
                    actividad_search.strip(),
                    case=False,
                    regex=False,
                )
            ].copy()

    if "nombre_establecimiento" in filtered.columns:
        nombre_search = st.sidebar.text_input("Buscar en nombre del establecimiento")
        if nombre_search.strip():
            filtered = filtered[
                normalize_text_series(filtered["nombre_establecimiento"]).str.contains(
                    nombre_search.strip(),
                    case=False,
                    regex=False,
                )
            ].copy()

    st.sidebar.markdown("---")
    st.sidebar.write(f"Registros filtrados: {len(filtered):,}")

    return filtered


def add_color_column(df: pd.DataFrame) -> pd.DataFrame:
    color_map = {
        "restaurante": [239, 68, 68, 180],
        "farmacia": [34, 197, 94, 180],
        "ferreteria": [245, 158, 11, 180],
        "hotel": [59, 130, 246, 180],
        "taller": [168, 85, 247, 180],
        "supermercado": [14, 165, 233, 180],
        "papeleria": [236, 72, 153, 180],
        "veterinaria": [16, 185, 129, 180],
        "abarrotes": [234, 88, 12, 180],
        "panaderia": [202, 138, 4, 180],
    }

    df = df.copy()
    if "search_term" in df.columns:
        df["point_color"] = df["search_term"].map(color_map)
    else:
        df["point_color"] = None

    df["point_color"] = df["point_color"].apply(
        lambda x: x if isinstance(x, list) else [37, 99, 235, 180]
    )
    return df


def build_map(filtered_df: pd.DataFrame, geojson_data):
    if filtered_df.empty:
        st.info("No hay registros para mostrar en el mapa con los filtros actuales.")
        return

    if "latitud" not in filtered_df.columns or "longitud" not in filtered_df.columns:
        st.warning("El dataset filtrado no contiene latitud y longitud.")
        return

    map_df = filtered_df.copy()
    map_df["latitud"] = pd.to_numeric(map_df["latitud"], errors="coerce")
    map_df["longitud"] = pd.to_numeric(map_df["longitud"], errors="coerce")
    map_df = map_df.dropna(subset=["latitud", "longitud"]).copy()

    if map_df.empty:
        st.info("No hay coordenadas válidas para mostrar en el mapa.")
        return

    map_df = add_color_column(map_df)

    center_lat = map_df["latitud"].mean()
    center_lon = map_df["longitud"].mean()

    layers = []

    if geojson_data is not None:
        layers.append(
            pdk.Layer(
                "GeoJsonLayer",
                data=geojson_data,
                stroked=True,
                filled=True,
                extruded=False,
                wireframe=False,
                get_fill_color=[15, 118, 110, 20],
                get_line_color=[15, 118, 110, 120],
                line_width_min_pixels=1,
                pickable=False,
            )
        )

    layers.append(
        pdk.Layer(
            "ScatterplotLayer",
            data=map_df,
            get_position="[longitud, latitud]",
            get_fill_color="point_color",
            get_radius=35,
            radius_min_pixels=3,
            radius_max_pixels=10,
            pickable=True,
            opacity=0.8,
        )
    )

    view_state = pdk.ViewState(
        latitude=float(center_lat),
        longitude=float(center_lon),
        zoom=11.5,
        pitch=0,
    )

    tooltip = {
        "html": """
        <b>{nombre_establecimiento}</b><br/>
        <b>Actividad:</b> {clase_actividad}<br/>
        <b>Colonia:</b> {colonia}<br/>
        <b>Estrato:</b> {estrato}<br/>
        <b>Término:</b> {search_term}<br/>
        <b>AGEB:</b> {CVE_AGEB}
        """,
        "style": {"backgroundColor": "rgba(0, 0, 0, 0.8)", "color": "white"},
    }

    deck = pdk.Deck(
        layers=layers,
        initial_view_state=view_state,
        tooltip=tooltip,
        map_style=pdk.map_styles.LIGHT,
    )

    st.pydeck_chart(deck, use_container_width=True)


def count_table(df: pd.DataFrame, column: str, limit: int | None = None) -> pd.DataFrame:
    if column not in df.columns:
        return pd.DataFrame(columns=[column, "registros"])

    out = (
        normalize_text_series(df[column])
        .replace("", "sin_valor")
        .value_counts()
        .rename_axis(column)
        .reset_index(name="registros")
    )

    if limit is not None:
        out = out.head(limit)

    return out


def donut_chart(df_counts: pd.DataFrame, category_col: str, title: str):
    if df_counts.empty:
        st.info("No hay datos para este gráfico.")
        return

    chart = (
        alt.Chart(df_counts)
        .mark_arc(innerRadius=55)
        .encode(
            theta=alt.Theta("registros:Q"),
            color=alt.Color(f"{category_col}:N", legend=alt.Legend(title=category_col)),
            tooltip=[category_col, "registros"],
        )
        .properties(height=320, title=title)
    )
    st.altair_chart(chart, use_container_width=True)


def horizontal_bar_chart(df_counts: pd.DataFrame, category_col: str, title: str):
    if df_counts.empty:
        st.info("No hay datos para este gráfico.")
        return

    chart = (
        alt.Chart(df_counts)
        .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
        .encode(
            x=alt.X("registros:Q", title="Registros"),
            y=alt.Y(f"{category_col}:N", sort="-x", title=""),
            tooltip=[category_col, "registros"],
        )
        .properties(height=340, title=title)
    )
    st.altair_chart(chart, use_container_width=True)


def heatmap_chart(df: pd.DataFrame, col_x: str, col_y: str, title: str):
    if col_x not in df.columns or col_y not in df.columns:
        st.info("No existen columnas suficientes para este gráfico.")
        return

    base = df.copy()
    base[col_x] = normalize_text_series(base[col_x]).replace("", "sin_valor")
    base[col_y] = normalize_text_series(base[col_y]).replace("", "sin_valor")

    chart = (
        alt.Chart(base)
        .mark_rect()
        .encode(
            x=alt.X(f"{col_x}:N", title=col_x),
            y=alt.Y(f"{col_y}:N", title=col_y),
            color=alt.Color("count():Q", title="Registros"),
            tooltip=[col_x, col_y, alt.Tooltip("count():Q", title="Registros")],
        )
        .properties(height=320, title=title)
    )
    st.altair_chart(chart, use_container_width=True)


st.title("Radar Económico de Mérida")
st.caption("Dashboard geoespacial con filtros, KPIs y visualización analítica")

if not LATEST_PATH.exists():
    st.warning("Aún no existe el dataset procesado. Ejecuta primero el pipeline.")
    st.stop()

df = load_latest_dataset(LATEST_PATH)
geojson_data = load_geojson(MERIDA_LAYER_PATH)
latest_summary = latest_file(METADATA_DIR, "phase2_summary_*.csv")
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

# KPIs
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