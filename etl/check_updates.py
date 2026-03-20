from __future__ import annotations

from pathlib import Path
import hashlib

from etl.common import ensure_dir, get_logger, latest_file, load_settings

logger = get_logger("etl.check_updates")


def sha256_file(path: str | Path) -> str:
    file_path = Path(path)

    if not file_path.exists():
        raise FileNotFoundError(f"No existe el archivo para calcular hash: {file_path}")

    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)

    return h.hexdigest()


def get_latest_raw_file() -> Path:
    settings = load_settings()
    raw_dir = Path(settings["paths"]["raw"])

    latest_raw = latest_file(raw_dir, "denue_raw_*.parquet")
    if latest_raw is None:
        raise FileNotFoundError("No se encontró ningún archivo raw en data/raw.")

    return latest_raw


def get_hash_file_path() -> Path:
    settings = load_settings()
    hash_path = Path(settings["files"]["latest_hash"])
    ensure_dir(hash_path.parent)
    return hash_path


def read_previous_hash() -> str | None:
    hash_file = get_hash_file_path()

    if not hash_file.exists():
        return None

    value = hash_file.read_text(encoding="utf-8").strip()
    return value or None


def save_current_hash(hash_value: str) -> Path:
    hash_file = get_hash_file_path()
    hash_file.write_text(hash_value, encoding="utf-8")
    logger.info("Hash guardado en %s", hash_file)
    return hash_file


def has_raw_changed() -> tuple[bool, str, Path]:
    latest_raw = get_latest_raw_file()
    current_hash = sha256_file(latest_raw)
    previous_hash = read_previous_hash()

    if previous_hash is None:
        logger.info("No existe hash previo. Se considera que hubo cambio.")
        return True, current_hash, latest_raw

    changed = current_hash != previous_hash

    if changed:
        logger.info("Se detectó cambio en el raw.")
    else:
        logger.info("No hubo cambios en el raw.")

    return changed, current_hash, latest_raw
