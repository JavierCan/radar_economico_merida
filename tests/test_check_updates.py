import pandas as pd

from etl import check_updates


def test_sha256_file_changes_with_content(tmp_path):
    file_path = tmp_path / "sample.txt"
    file_path.write_text("abc", encoding="utf-8")
    hash_a = check_updates.sha256_file(file_path)

    file_path.write_text("abcd", encoding="utf-8")
    hash_b = check_updates.sha256_file(file_path)

    assert hash_a != hash_b


def test_has_raw_changed_detects_same_and_different_hashes(tmp_path, monkeypatch):
    raw_dir = tmp_path / "raw"
    metadata_dir = tmp_path / "metadata"
    raw_dir.mkdir()
    metadata_dir.mkdir()

    raw_file = raw_dir / "denue_raw_test.parquet"
    pd.DataFrame({"a": [1]}).to_parquet(raw_file, index=False)

    hash_file = metadata_dir / "latest_hash.txt"

    monkeypatch.setattr(
        check_updates,
        "load_settings",
        lambda: {
            "paths": {"raw": str(raw_dir)},
            "files": {"latest_hash": str(hash_file)},
        },
    )

    changed, current_hash, latest_raw = check_updates.has_raw_changed()
    assert changed is True
    assert latest_raw == raw_file

    check_updates.save_current_hash(current_hash)
    changed_again, current_hash_again, _ = check_updates.has_raw_changed()
    assert changed_again is False
    assert current_hash_again == current_hash
