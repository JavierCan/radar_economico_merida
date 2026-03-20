from __future__ import annotations

from pathlib import Path
import json

import geopandas as gpd
import pandas as pd
import streamlit as st

from etl.common import latest_file
from etl.schema import validate_processed_dataset


@st.cache_data
def load_latest_dataset(path: Path) -> pd.DataFrame:
    try:
        df = gpd.read_parquet(path)
    except (ValueError, OSError, TypeError):
        df = pd.read_parquet(path)

    return validate_processed_dataset(df)


@st.cache_data
def load_geojson(path: Path):
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


__all__ = ["latest_file", "load_geojson", "load_latest_dataset"]
