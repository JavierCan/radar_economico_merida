from __future__ import annotations

from datetime import datetime
from pathlib import Path
import os
import yaml


def load_settings(path: str = "config/settings.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def now_tag() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


def get_env(name: str, default: str = "") -> str:
    return os.getenv(name, default)


def latest_file(directory: str | Path, pattern: str):
    files = sorted(Path(directory).glob(pattern), key=lambda x: x.stat().st_mtime, reverse=True)
    return files[0] if files else None