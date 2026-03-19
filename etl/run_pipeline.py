from __future__ import annotations

from pathlib import Path
import pandas as pd

from etl.common import load_settings, ensure_dir, now_tag
from etl.extract_denue import extract_denue
from etl.extract_merida import load_or_download_merida_layer


def main():
    settings = load_settings()

    if not settings["etl"].get("enabled", False):
        print("ETL deshabilitado en settings.yaml")
        return

    raw_dir = ensure_dir(settings["paths"]["raw"])
    metadata_dir = ensure_dir(settings["paths"]["metadata"])

    print("======================================")
    print("RADAR ECONÓMICO DE MÉRIDA · FASE 1")
    print("Extracción inicial de fuentes")
    print("======================================")

    # 1) Extraer DENUE
    df_denue = extract_denue(save_raw=True)
    print(f"Registros extraídos desde DENUE: {len(df_denue)}")

    # 2) Cargar o descargar capa geográfica de Mérida
    try:
        gdf_merida = load_or_download_merida_layer()
        merida_records = len(gdf_merida)
    except Exception as e:
        print(f"No fue posible cargar la capa geográfica de Mérida: {e}")
        merida_records = 0

    # 3) Guardar un resumen de ejecución de fase 1
    summary = pd.DataFrame(
        [
            {
                "run_timestamp": pd.Timestamp.now(),
                "stage": settings["project"]["stage"],
                "denue_records": len(df_denue),
                "merida_geo_records": merida_records,
            }
        ]
    )

    summary_path = Path(metadata_dir) / f"phase1_summary_{now_tag()}.csv"
    summary.to_csv(summary_path, index=False)

    print(f"Resumen guardado en: {summary_path}")
    print("Fase 1 terminada correctamente.")


if __name__ == "__main__":
    main()