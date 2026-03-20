from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from app.filters import normalize_text_series


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
