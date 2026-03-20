from pathlib import Path
import os

import pandas as pd
import pytest

from app import data as app_data
from app.data import latest_file, load_geojson, load_latest_dataset


VALID_DF = pd.DataFrame(
    {
        "nombre_establecimiento": ["A"],
        "clase_actividad": ["B"],
        "colonia": ["Centro"],
        "latitud": [20.9],
        "longitud": [-89.6],
        "search_term": ["restaurante"],
        "CVE_AGEB": ["001"],
        "geometry": ["POINT (-89.6 20.9)"],
    }
)


def test_latest_file_returns_most_recent_match(tmp_path):
    older = tmp_path / "pipeline_summary_1.csv"
    newer = tmp_path / "pipeline_summary_2.csv"
    older.write_text("a", encoding="utf-8")
    newer.write_text("b", encoding="utf-8")

    os.utime(older, (1_700_000_000, 1_700_000_000))
    os.utime(newer, (1_700_000_100, 1_700_000_100))

    assert latest_file(tmp_path, "pipeline_summary_*.csv") == newer


def test_load_geojson_returns_none_when_file_does_not_exist(tmp_path):
    assert load_geojson(tmp_path / "missing.geojson") is None


def test_load_latest_dataset_validates_required_schema(tmp_path):
    path = tmp_path / "latest.parquet"
    VALID_DF.to_parquet(path, index=False)

    loaded = load_latest_dataset(Path(path))
    assert loaded.equals(VALID_DF)


def test_load_latest_dataset_falls_back_to_pandas_reader(monkeypatch, tmp_path):
    path = tmp_path / "latest.parquet"
    VALID_DF.to_parquet(path, index=False)

    monkeypatch.setattr(app_data.gpd, "read_parquet", lambda _: (_ for _ in ()).throw(ValueError("geo fail")))
    monkeypatch.setattr(app_data.pd, "read_parquet", lambda _: VALID_DF.copy())

    loaded = load_latest_dataset(Path(path))
    assert loaded.equals(VALID_DF)


def test_load_latest_dataset_surfaces_both_reader_errors(monkeypatch, tmp_path):
    path = tmp_path / "broken.parquet"
    path.write_text("not a parquet file", encoding="utf-8")

    monkeypatch.setattr(app_data.gpd, "read_parquet", lambda _: (_ for _ in ()).throw(ValueError("geo fail")))
    monkeypatch.setattr(app_data.pd, "read_parquet", lambda _: (_ for _ in ()).throw(OSError("pandas fail")))

    with pytest.raises(ValueError, match="No fue posible leer el dataset procesado") as exc_info:
        load_latest_dataset(Path(path))

    message = str(exc_info.value)
    assert "geo fail" in message
    assert "pandas fail" in message


def test_load_latest_dataset_rejects_invalid_schema(tmp_path):
    path = tmp_path / "broken.parquet"
    pd.DataFrame({"nombre_establecimiento": ["A"]}).to_parquet(path, index=False)

    with pytest.raises(ValueError, match="Faltan columnas requeridas"):
        load_latest_dataset(Path(path))
