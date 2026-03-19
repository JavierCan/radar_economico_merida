from __future__ import annotations

from pathlib import Path
import requests
import geopandas as gpd

from etl.common import load_settings, ensure_dir


def _download_geojson(url: str, output_path: Path) -> Path:
    response = requests.get(url, timeout=120)
    response.raise_for_status()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(response.content)
    return output_path


def load_or_download_merida_layer() -> gpd.GeoDataFrame:
    settings = load_settings()
    merida_cfg = settings["merida"]

    local_path = Path(merida_cfg["local_geojson_path"])
    remote_url = merida_cfg.get("remote_geojson_url", "").strip()

    ensure_dir(local_path.parent)

    if not local_path.exists():
        if not remote_url:
            raise FileNotFoundError(
                f"No existe la capa geográfica local y no se configuró remote_geojson_url: {local_path}"
            )
        _download_geojson(remote_url, local_path)
        print(f"Capa geográfica descargada en: {local_path}")

    gdf = gpd.read_file(local_path)

    if gdf.empty:
        print("La capa geográfica de Mérida está vacía.")
    else:
        print(f"Capa geográfica cargada: {len(gdf)} registros")

    return gdf