from __future__ import annotations

import pandas as pd

REQUIRED_PROCESSED_COLUMNS = {
    "nombre_establecimiento",
    "clase_actividad",
    "colonia",
    "latitud",
    "longitud",
    "search_term",
    "CVE_AGEB",
    "geometry",
}

OPTIONAL_ANALYTICAL_COLUMNS = {
    "estrato",
    "tipo_establecimiento",
    "codigo_postal",
    "source_name",
    "extraction_timestamp",
}


def validate_processed_dataset(
    df: pd.DataFrame,
    required_columns: set[str] | None = None,
) -> pd.DataFrame:
    required = REQUIRED_PROCESSED_COLUMNS if required_columns is None else required_columns
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(
            "El dataset procesado no cumple el esquema esperado. "
            f"Faltan columnas requeridas: {missing}"
        )

    if df.empty:
        raise ValueError("El dataset procesado está vacío; no se puede publicar ni visualizar.")

    return df
