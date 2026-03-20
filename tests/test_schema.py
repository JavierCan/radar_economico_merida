import pandas as pd
import pytest

from etl.schema import validate_processed_dataset


def test_validate_processed_dataset_accepts_required_columns():
    df = pd.DataFrame(
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

    validated = validate_processed_dataset(df)
    assert validated.equals(df)


def test_validate_processed_dataset_rejects_missing_columns():
    df = pd.DataFrame({"nombre_establecimiento": ["A"]})
    with pytest.raises(ValueError, match="Faltan columnas requeridas"):
        validate_processed_dataset(df)


def test_validate_processed_dataset_rejects_empty_dataframe():
    df = pd.DataFrame(
        columns=[
            "nombre_establecimiento",
            "clase_actividad",
            "colonia",
            "latitud",
            "longitud",
            "search_term",
            "CVE_AGEB",
            "geometry",
        ]
    )
    with pytest.raises(ValueError, match="está vacío"):
        validate_processed_dataset(df)
