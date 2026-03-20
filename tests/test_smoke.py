from pathlib import Path


def test_structure_exists():
    assert Path("app").exists()
    assert Path("etl").exists()
    assert Path("data").exists()
    assert Path("pyproject.toml").exists()
    assert Path("app/charts.py").exists()
    assert Path("app/filters.py").exists()
    assert Path("app/map_view.py").exists()
