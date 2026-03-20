import pandas as pd
import pytest

from etl.transform_data import clean_denue_data, normalize_columns, rename_denue_columns


def test_normalize_columns_standardizes_headers():
    df = pd.DataFrame(columns=[" Nombre ", "Correo.E", "Tipo Vialidad"])
    normalized = normalize_columns(df)
    assert list(normalized.columns) == ["nombre", "correo_e", "tipo_vialidad"]


def test_rename_denue_columns_maps_expected_fields():
    df = pd.DataFrame(columns=["id", "nombre", "cp", "tipo", "latitud", "longitud"])
    renamed = rename_denue_columns(df)
    assert "id_establecimiento" in renamed.columns
    assert "nombre_establecimiento" in renamed.columns
    assert "codigo_postal" in renamed.columns
    assert "tipo_establecimiento" in renamed.columns


def test_clean_denue_data_requires_geo_columns():
    df = pd.DataFrame({"nombre": ["A"]})
    with pytest.raises(ValueError):
        clean_denue_data(df)


def test_clean_denue_data_filters_invalid_coordinates_and_deduplicates():
    df = pd.DataFrame(
        {
            "CLEE": ["1", "1", "2", "3"],
            "Nombre": ["  A  ", "A", "B", "C"],
            "Latitud": [20.9, 20.9, "nope", 30.0],
            "Longitud": [-89.6, -89.6, -89.6, -89.6],
            "Colonia": [" Centro ", "Centro", "X", "Y"],
        }
    )

    cleaned = clean_denue_data(df)

    assert len(cleaned) == 1
    assert cleaned.loc[0, "clee"] == "1"
    assert cleaned.loc[0, "nombre_establecimiento"] == "A"
    assert cleaned.loc[0, "colonia"] == "Centro"
