from __future__ import annotations

import geopandas as gpd
import pandas as pd

from etl.common import get_logger, load_settings

logger = get_logger("etl.transform_data")


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace(".", "_", regex=False)
    )
    return df


def rename_denue_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    rename_map = {
        "id": "id_establecimiento",
        "clee": "clee",
        "nombre": "nombre_establecimiento",
        "razon_social": "razon_social",
        "clase_actividad": "clase_actividad",
        "estrato": "estrato",
        "tipo_vialidad": "tipo_vialidad",
        "calle": "calle",
        "num_exterior": "num_exterior",
        "num_interior": "num_interior",
        "colonia": "colonia",
        "cp": "codigo_postal",
        "ubicacion": "ubicacion",
        "telefono": "telefono",
        "correo_e": "correo_electronico",
        "sitio_internet": "sitio_internet",
        "tipo": "tipo_establecimiento",
        "longitud": "longitud",
        "latitud": "latitud",
        "centrocomercial": "centro_comercial",
        "tipocentrocomercial": "tipo_centro_comercial",
        "numlocal": "num_local",
        "search_term": "search_term",
        "source_name": "source_name",
        "extraction_timestamp": "extraction_timestamp",
    }

    existing_renames = {k: v for k, v in rename_map.items() if k in df.columns}
    return df.rename(columns=existing_renames)


def clean_denue_data(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_columns(df)
    df = rename_denue_columns(df)

    required_geo_cols = {"latitud", "longitud"}
    missing_geo = required_geo_cols - set(df.columns)
    if missing_geo:
        raise ValueError(f"Faltan columnas geográficas requeridas en DENUE: {sorted(missing_geo)}")

    df["latitud"] = pd.to_numeric(df["latitud"], errors="coerce")
    df["longitud"] = pd.to_numeric(df["longitud"], errors="coerce")

    df = df.dropna(subset=["latitud", "longitud"]).copy()
    df = df[df["latitud"].between(20.0, 21.5) & df["longitud"].between(-90.5, -88.5)].copy()

    str_cols = df.select_dtypes(include="object").columns.tolist()
    for col in str_cols:
        df[col] = df[col].fillna("").astype(str).str.strip()

    if "clee" in df.columns:
        df = df.drop_duplicates(subset=["clee"]).copy()
    elif "id_establecimiento" in df.columns:
        df = df.drop_duplicates(subset=["id_establecimiento"]).copy()
    else:
        df = df.drop_duplicates().copy()

    return df.reset_index(drop=True)


def load_merida_layer() -> gpd.GeoDataFrame:
    settings = load_settings()
    geo_path = settings["merida"]["local_geojson_path"]

    gdf = gpd.read_file(geo_path)

    if gdf.empty:
        raise ValueError("La capa geográfica de Mérida está vacía.")

    if gdf.crs is None:
        raise ValueError("La capa geográfica de Mérida no tiene CRS definido.")

    gdf = gdf.to_crs(epsg=4326).copy()

    keep_cols = [
        col
        for col in [
            "ID",
            "EDICION",
            "VERSION",
            "TIPO_GEO",
            "CVEGEO",
            "CVE_ENT",
            "CVE_MUN",
            "CVE_LOC",
            "CVE_AGEB",
            "geometry",
        ]
        if col in gdf.columns
    ]

    gdf = gdf[keep_cols].copy()
    return gdf


def denue_to_geodataframe(df: pd.DataFrame) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        df.copy(),
        geometry=gpd.points_from_xy(df["longitud"], df["latitud"]),
        crs="EPSG:4326",
    )


def spatial_enrich_denue_with_merida(
    gdf_denue: gpd.GeoDataFrame,
    gdf_merida: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    joined = gpd.sjoin(
        gdf_denue,
        gdf_merida,
        how="inner",
        predicate="within",
    ).copy()

    if "index_right" in joined.columns:
        joined = joined.drop(columns=["index_right"])

    return joined


def finalize_schema(gdf_result: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    preferred_order = [
        "clee",
        "id_establecimiento",
        "nombre_establecimiento",
        "razon_social",
        "clase_actividad",
        "estrato",
        "tipo_establecimiento",
        "tipo_vialidad",
        "calle",
        "num_exterior",
        "num_interior",
        "colonia",
        "codigo_postal",
        "ubicacion",
        "telefono",
        "correo_electronico",
        "sitio_internet",
        "centro_comercial",
        "tipo_centro_comercial",
        "num_local",
        "longitud",
        "latitud",
        "search_term",
        "source_name",
        "extraction_timestamp",
        "ID",
        "EDICION",
        "VERSION",
        "TIPO_GEO",
        "CVEGEO",
        "CVE_ENT",
        "CVE_MUN",
        "CVE_LOC",
        "CVE_AGEB",
        "geometry",
    ]

    ordered_cols = [c for c in preferred_order if c in gdf_result.columns]
    remaining_cols = [c for c in gdf_result.columns if c not in ordered_cols]
    return gdf_result[ordered_cols + remaining_cols].copy()


def transform_data(
    df_raw: pd.DataFrame,
    gdf_merida: gpd.GeoDataFrame | None = None,
) -> gpd.GeoDataFrame:
    if df_raw.empty:
        logger.warning("El DataFrame raw está vacío.")
        return gpd.GeoDataFrame()

    logger.info("Iniciando transformación de DENUE...")
    df_clean = clean_denue_data(df_raw)
    logger.info("Registros después de limpieza: %s", len(df_clean))

    gdf_denue = denue_to_geodataframe(df_clean)
    logger.info("GeoDataFrame DENUE creado: %s registros", len(gdf_denue))

    if gdf_merida is None:
        gdf_merida = load_merida_layer()
    logger.info("Capa geográfica cargada: %s polígonos", len(gdf_merida))

    gdf_result = spatial_enrich_denue_with_merida(gdf_denue, gdf_merida)
    logger.info("Registros dentro de la capa de Mérida: %s", len(gdf_result))

    return finalize_schema(gdf_result).reset_index(drop=True)
