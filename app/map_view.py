from __future__ import annotations

import pandas as pd
import pydeck as pdk
import streamlit as st


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
