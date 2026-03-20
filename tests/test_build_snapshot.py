import pandas as pd
import pytest

from etl import build_snapshot


def test_save_latest_creates_file(tmp_path, monkeypatch):
    latest_path = tmp_path / "processed" / "latest.parquet"
    monkeypatch.setattr(
        build_snapshot,
        "load_settings",
        lambda: {
            "files": {"latest_dataset": str(latest_path)},
            "paths": {"snapshots": str(tmp_path / "snapshots")},
        },
    )

    out_path = build_snapshot.save_latest(pd.DataFrame({"a": [1]}))
    assert out_path == latest_path
    assert latest_path.exists()


def test_save_snapshot_creates_versioned_file(tmp_path, monkeypatch):
    snapshots_dir = tmp_path / "snapshots"
    monkeypatch.setattr(
        build_snapshot,
        "load_settings",
        lambda: {
            "files": {"latest_dataset": str(tmp_path / "latest.parquet")},
            "paths": {"snapshots": str(snapshots_dir)},
        },
    )

    out_path = build_snapshot.save_snapshot(
        pd.DataFrame({"a": [1]}),
        prefix="radar_test",
    )
    assert out_path.exists()
    assert out_path.parent == snapshots_dir
    assert out_path.name.startswith("radar_test_")


def test_build_snapshot_raises_on_empty_dataframe():
    with pytest.raises(ValueError):
        build_snapshot.build_snapshot(pd.DataFrame())
