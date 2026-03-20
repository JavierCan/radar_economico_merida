import pandas as pd

from etl import run_pipeline


def test_run_pipeline_respects_snapshot_flag(monkeypatch):
    calls: dict[str, object] = {}
    settings = {
        "project": {"stage": "phase_2"},
        "etl": {"enabled": True, "compare_hash": False, "save_snapshots": False},
        "paths": {"metadata": "data/metadata"},
        "files": {"manifest": "data/metadata/manifest.csv"},
    }

    monkeypatch.setattr(run_pipeline, "load_settings", lambda: settings)
    monkeypatch.setattr(run_pipeline, "extract_denue", lambda save_raw=True: pd.DataFrame({"x": [1]}))
    monkeypatch.setattr(run_pipeline, "load_or_download_merida_layer", lambda: pd.DataFrame({"geometry": [1]}))
    monkeypatch.setattr(
        run_pipeline,
        "transform_data",
        lambda df_denue, gdf_merida=None: pd.DataFrame(
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
        ),
    )
    def fake_build_snapshot(df, save_latest_file=True, save_snapshot_file=True):
        calls["snapshot_args"] = {
            "save_latest_file": save_latest_file,
            "save_snapshot_file": save_snapshot_file,
        }
        return {"latest_path": "latest.parquet", "snapshot_path": None}

    monkeypatch.setattr(run_pipeline, "build_snapshot", fake_build_snapshot)
    monkeypatch.setattr(run_pipeline, "append_manifest_row", lambda row: "manifest.csv")
    monkeypatch.setattr(run_pipeline, "save_run_summary", lambda row: "summary.csv")

    run_pipeline.main()

    assert calls["snapshot_args"] == {
        "save_latest_file": True,
        "save_snapshot_file": False,
    }
