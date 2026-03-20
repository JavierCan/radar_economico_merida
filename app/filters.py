from __future__ import annotations

import pandas as pd
import streamlit as st


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
