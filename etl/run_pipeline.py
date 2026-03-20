from __future__ import annotations

from pathlib import Path

import pandas as pd

from etl.build_snapshot import build_snapshot
from etl.check_updates import has_raw_changed, save_current_hash
from etl.common import ensure_dir, get_logger, load_settings, now_tag, utc_now_iso
from etl.extract_denue import extract_denue
from etl.extract_merida import load_or_download_merida_layer
from etl.schema import validate_processed_dataset
from etl.transform_data import transform_data

logger = get_logger("etl.run_pipeline")


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
    summary_path = Path(metadata_dir) / f"pipeline_summary_{now_tag()}.csv"
    pd.DataFrame([row]).to_csv(summary_path, index=False)
    return summary_path


def build_run_row(settings: dict, status: str, **kwargs) -> dict:
    row = {
        "run_timestamp": utc_now_iso(),
        "stage": settings["project"]["stage"],
        "status": status,
        "raw_records": 0,
        "processed_records": 0,
        "merida_geo_records": 0,
        "latest_path": "",
        "snapshot_path": "",
        "raw_hash": "",
        "raw_file": "",
        "error_message": "",
    }
    row.update(kwargs)
    return row


def finalize_run(row: dict) -> tuple[Path, Path]:
    manifest_path = append_manifest_row(row)
    summary_path = save_run_summary(row)
    logger.info("Manifest actualizado en %s", manifest_path)
    logger.info("Resumen guardado en %s", summary_path)
    return manifest_path, summary_path


def main():
    settings = load_settings()
    etl_settings = settings["etl"]

    if not etl_settings.get("enabled", False):
        logger.warning("ETL deshabilitado en settings.yaml")
        return

    logger.info("======================================")
    logger.info("RADAR ECONÓMICO DE MÉRIDA · %s", settings["project"]["stage"])
    logger.info("Transformación, versionado y latest")
    logger.info("======================================")

    compare_hash = etl_settings.get("compare_hash", True)
    save_snapshots = etl_settings.get("save_snapshots", True)

    try:
        df_denue = extract_denue(save_raw=True)
        logger.info("Registros extraídos desde DENUE: %s", len(df_denue))

        if df_denue.empty:
            row = build_run_row(settings, "empty_raw")
            finalize_run(row)
            logger.warning("No se obtuvieron datos desde DENUE.")
            return

        current_hash = ""
        latest_raw_path = ""

        if compare_hash:
            changed, current_hash, latest_raw_path_obj = has_raw_changed()
            latest_raw_path = str(latest_raw_path_obj)
            if not changed:
                row = build_run_row(
                    settings,
                    "no_change",
                    raw_records=len(df_denue),
                    raw_hash=current_hash,
                    raw_file=latest_raw_path,
                )
                finalize_run(row)
                logger.info("No hubo cambios en el raw. No se ejecuta transformación.")
                return

        gdf_merida = load_or_download_merida_layer()
        merida_records = len(gdf_merida)

        gdf_processed = transform_data(df_denue, gdf_merida=gdf_merida)
        validate_processed_dataset(gdf_processed)
        processed_records = len(gdf_processed)

        if gdf_processed.empty:
            row = build_run_row(
                settings,
                "empty_processed",
                raw_records=len(df_denue),
                merida_geo_records=merida_records,
                raw_hash=current_hash,
                raw_file=latest_raw_path,
            )
            finalize_run(row)
            logger.warning("La transformación devolvió un dataset vacío.")
            return

        snapshot_result = build_snapshot(
            gdf_processed,
            save_latest_file=True,
            save_snapshot_file=save_snapshots,
        )

        latest_path = snapshot_result["latest_path"]
        snapshot_path = snapshot_result["snapshot_path"]

        if compare_hash and current_hash:
            save_current_hash(current_hash)

        row = build_run_row(
            settings,
            "processed",
            raw_records=len(df_denue),
            processed_records=processed_records,
            merida_geo_records=merida_records,
            latest_path=str(latest_path) if latest_path else "",
            snapshot_path=str(snapshot_path) if snapshot_path else "",
            raw_hash=current_hash,
            raw_file=latest_raw_path,
        )
        finalize_run(row)
        logger.info("Proceso completado correctamente.")

    except Exception as exc:
        row = build_run_row(settings, "error", error_message=str(exc))
        finalize_run(row)
        logger.exception("Error en el pipeline")
        raise


if __name__ == "__main__":
    main()
