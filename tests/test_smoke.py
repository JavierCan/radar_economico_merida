from pathlib import Path

def test_structure_exists():
    assert Path("app").exists()
    assert Path("etl").exists()
    assert Path("data").exists()
    assert Path("pyproject.toml").exists()
