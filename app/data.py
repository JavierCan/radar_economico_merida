from __future__ import annotations

from pathlib import Path
import json

import geopandas as gpd
import pandas as pd
import streamlit as st

from etl.common import latest_file
from etl.schema import validate_processed_dataset


def _read_processed_parquet(path: Path) -> pd.DataFrame:
    geo_error: Exception | None = None

    try:
        return gpd.read_parquet(path)
    except (ValueError, OSError, TypeError, ImportError) as exc:
        geo_error = exc

    try:
        return pd.read_parquet(path)
    except Exception as exc:
        message = [f"No fue posible leer el dataset procesado en '{path}'."]
        if geo_error is not None:
            message.append(f"geopandas.read_parquet: {geo_error}")
        message.append(f"pandas.read_parquet: {exc}")
        raise ValueError(" ".join(message)) from exc


@st.cache_data
def load_latest_dataset(path: Path) -> pd.DataFrame:
    df = _read_processed_parquet(path)
    return validate_processed_dataset(df)


@st.cache_data
def load_geojson(path: Path):
    if not path.exists():
        return None

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


__all__ = ["latest_file", "load_geojson", "load_latest_dataset"]
