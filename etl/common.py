from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import logging
import os

import yaml


LOGGER_NAME = "radar_economico_merida"


def load_settings(path: str = "config/settings.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_env(name: str, default: str = "") -> str:
    return os.getenv(name, default)


def latest_file(directory: str | Path, pattern: str) -> Path | None:
    files = sorted(Path(directory).glob(pattern), key=lambda x: x.stat().st_mtime, reverse=True)
    return files[0] if files else None


def get_logger(name: str | None = None) -> logging.Logger:
    logger_name = LOGGER_NAME if not name else f"{LOGGER_NAME}.{name}"
    logger = logging.getLogger(logger_name)

    if not logging.getLogger(LOGGER_NAME).handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        )

    return logger
