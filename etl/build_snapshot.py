from __future__ import annotations

from pathlib import Path
import pandas as pd

from etl.common import load_settings, ensure_dir, now_tag


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

    print(f"Latest guardado en: {out_path}")
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

    print(f"Snapshot guardado en: {out_path}")
    return out_path


def build_snapshot(
    df: pd.DataFrame,
    save_latest_file: bool = True,
    save_snapshot_file: bool = True,
) -> dict[str, Path | None]:
    if df.empty:
        raise ValueError("No se puede generar latest/snapshot a partir de un DataFrame vacío.")

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