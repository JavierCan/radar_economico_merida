from __future__ import annotations

from pathlib import Path
import pandas as pd

from etl.common import load_settings, ensure_dir, now_tag
from etl.extract_denue import extract_denue
from etl.extract_merida import load_or_download_merida_layer
from etl.check_updates import has_raw_changed, save_current_hash
from etl.transform_data import transform_data
from etl.build_snapshot import build_snapshot


def append_manifest_row(row: dict) -> Path:
    settings = load_settings()
    manifest_path = Path(settings["files"]["manifest"])
    ensure_dir(manifest_path.parent)

    df_new = pd.DataFrame([row])

    if manifest_path.exists():
        df_old = pd.read_csv(manifest_path)
        df_all = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_all = df_new

    df_all.to_csv(manifest_path, index=False)
    return manifest_path


def save_run_summary(row: dict) -> Path:
    settings = load_settings()
    metadata_dir = ensure_dir(settings["paths"]["metadata"])
    summary_path = Path(metadata_dir) / f"phase2_summary_{now_tag()}.csv"
    pd.DataFrame([row]).to_csv(summary_path, index=False)
    return summary_path


def main():
    settings = load_settings()

    if not settings["etl"].get("enabled", False):
        print("ETL deshabilitado en settings.yaml")
        return

    print("======================================")
    print("RADAR ECONÓMICO DE MÉRIDA · FASE 2")
    print("Transformación, versionado y latest")
    print("======================================")

    try:
        # 1) Extraer raw de DENUE
        df_denue = extract_denue(save_raw=True)
        print(f"Registros extraídos desde DENUE: {len(df_denue)}")

        if df_denue.empty:
            row = {
                "run_timestamp": pd.Timestamp.now().isoformat(),
                "stage": settings["project"]["stage"],
                "status": "empty_raw",
                "raw_records": 0,
                "processed_records": 0,
                "merida_geo_records": 0,
                "latest_path": "",
                "snapshot_path": "",
                "raw_hash": "",
            }
            manifest_path = append_manifest_row(row)
            summary_path = save_run_summary(row)
            print(f"No se obtuvieron datos desde DENUE.")
            print(f"Manifest actualizado en: {manifest_path}")
            print(f"Resumen guardado en: {summary_path}")
            return

        # 2) Revisar si cambió el raw
        changed, current_hash, latest_raw_path = has_raw_changed()

        if not changed:
            row = {
                "run_timestamp": pd.Timestamp.now().isoformat(),
                "stage": settings["project"]["stage"],
                "status": "no_change",
                "raw_records": len(df_denue),
                "processed_records": 0,
                "merida_geo_records": 0,
                "latest_path": "",
                "snapshot_path": "",
                "raw_hash": current_hash,
                "raw_file": str(latest_raw_path),
            }
            manifest_path = append_manifest_row(row)
            summary_path = save_run_summary(row)

            print("No hubo cambios en el raw. No se ejecuta transformación.")
            print(f"Manifest actualizado en: {manifest_path}")
            print(f"Resumen guardado en: {summary_path}")
            return

        # 3) Cargar capa geo solo si sí hubo cambio
        gdf_merida = load_or_download_merida_layer()
        merida_records = len(gdf_merida)

        # 4) Transformar
        gdf_processed = transform_data(df_denue)
        processed_records = len(gdf_processed)

        if gdf_processed.empty:
            row = {
                "run_timestamp": pd.Timestamp.now().isoformat(),
                "stage": settings["project"]["stage"],
                "status": "empty_processed",
                "raw_records": len(df_denue),
                "processed_records": 0,
                "merida_geo_records": merida_records,
                "latest_path": "",
                "snapshot_path": "",
                "raw_hash": current_hash,
                "raw_file": str(latest_raw_path),
            }
            manifest_path = append_manifest_row(row)
            summary_path = save_run_summary(row)

            print("La transformación devolvió un dataset vacío.")
            print(f"Manifest actualizado en: {manifest_path}")
            print(f"Resumen guardado en: {summary_path}")
            return

        # 5) Guardar latest y snapshot
        snapshot_result = build_snapshot(
            gdf_processed,
            save_latest_file=True,
            save_snapshot_file=True,
        )

        latest_path = snapshot_result["latest_path"]
        snapshot_path = snapshot_result["snapshot_path"]

        # 6) Guardar hash nuevo
        save_current_hash(current_hash)

        # 7) Registrar ejecución
        row = {
            "run_timestamp": pd.Timestamp.now().isoformat(),
            "stage": settings["project"]["stage"],
            "status": "processed",
            "raw_records": len(df_denue),
            "processed_records": processed_records,
            "merida_geo_records": merida_records,
            "latest_path": str(latest_path) if latest_path else "",
            "snapshot_path": str(snapshot_path) if snapshot_path else "",
            "raw_hash": current_hash,
            "raw_file": str(latest_raw_path),
        }

        manifest_path = append_manifest_row(row)
        summary_path = save_run_summary(row)

        print("Proceso completado correctamente.")
        print(f"Manifest actualizado en: {manifest_path}")
        print(f"Resumen guardado en: {summary_path}")

    except Exception as e:
        row = {
            "run_timestamp": pd.Timestamp.now().isoformat(),
            "stage": settings["project"]["stage"],
            "status": "error",
            "raw_records": "",
            "processed_records": "",
            "merida_geo_records": "",
            "latest_path": "",
            "snapshot_path": "",
            "raw_hash": "",
            "raw_file": "",
            "error_message": str(e),
        }

        manifest_path = append_manifest_row(row)
        summary_path = save_run_summary(row)

        print(f"Error en el pipeline: {e}")
        print(f"Manifest actualizado en: {manifest_path}")
        print(f"Resumen guardado en: {summary_path}")
        raise


if __name__ == "__main__":
    main()