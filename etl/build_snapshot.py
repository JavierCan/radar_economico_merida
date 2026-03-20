from __future__ import annotations

from pathlib import Path
import pandas as pd

from etl.common import ensure_dir, get_logger, load_settings, now_tag
from etl.schema import validate_processed_dataset

logger = get_logger("etl.build_snapshot")


def save_latest(
    df: pd.DataFrame,
    latest_path: str | None = None,
) -> Path:
    settings = load_settings()

    if latest_path is None:
        latest_path = settings["files"]["latest_dataset"]

    out_path = Path(latest_path)
    ensure_dir(out_path.parent)

    df.to_parquet(out_path, index=False)

    logger.info("Latest guardado en %s", out_path)
    return out_path


def save_snapshot(
    df: pd.DataFrame,
    snapshots_dir: str | None = None,
    prefix: str = "radar_merida",
) -> Path:
    settings = load_settings()

    if snapshots_dir is None:
        snapshots_dir = settings["paths"]["snapshots"]

    out_dir = ensure_dir(snapshots_dir)
    version_tag = now_tag()
    out_path = Path(out_dir) / f"{prefix}_{version_tag}.parquet"

    df.to_parquet(out_path, index=False)

    logger.info("Snapshot guardado en %s", out_path)
    return out_path


def build_snapshot(
    df: pd.DataFrame,
    save_latest_file: bool = True,
    save_snapshot_file: bool = True,
) -> dict[str, Path | None]:
    validate_processed_dataset(df)

    latest_path: Path | None = None
    snapshot_path: Path | None = None

    if save_latest_file:
        latest_path = save_latest(df)

    if save_snapshot_file:
        snapshot_path = save_snapshot(df)

    return {
        "latest_path": latest_path,
        "snapshot_path": snapshot_path,
    }
