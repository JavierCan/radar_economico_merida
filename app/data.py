from __future__ import annotations

from pathlib import Path
import json

import geopandas as gpd
import pandas as pd
import streamlit as st


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