from __future__ import annotations

from pathlib import Path
import json
import requests
import pandas as pd
from dotenv import load_dotenv

from etl.common import load_settings, ensure_dir, now_tag, get_env

load_dotenv()


def _normalize_denue_payload(payload) -> pd.DataFrame:
    if isinstance(payload, list):
        return pd.DataFrame(payload)

    if isinstance(payload, dict):
        for key in ["results", "data", "items", "records"]:
            value = payload.get(key)
            if isinstance(value, list):
                return pd.DataFrame(value)

        return pd.DataFrame([payload])

    return pd.DataFrame()


def extract_denue(save_raw: bool = True) -> pd.DataFrame:
    settings = load_settings()
    denue_cfg = settings["denue"]
    raw_dir = ensure_dir(settings["paths"]["raw"])

    if not denue_cfg.get("enabled", False):
        raise RuntimeError("La fuente DENUE está deshabilitada en settings.yaml")

    base_url = denue_cfg.get("base_url", "").strip()
    if not base_url:
        raise ValueError(
            "Falta configurar denue.base_url en config/settings.yaml"
        )

    token_env_name = denue_cfg.get("api_token_env", "INEGI_API_TOKEN")
    api_token = get_env(token_env_name, "").strip()
    if not api_token:
        raise ValueError(
            f"No existe el token en la variable de entorno '{token_env_name}'"
        )

    query = denue_cfg.get("query", {})
    timeout_seconds = int(denue_cfg.get("timeout_seconds", 60))

    params = {
        **query,
        "token": api_token,
    }

    response = requests.get(base_url, params=params, timeout=timeout_seconds)
    response.raise_for_status()

    content_type = response.headers.get("Content-Type", "")
    if "application/json" in content_type.lower():
        payload = response.json()
    else:
        try:
            payload = response.json()
        except Exception:
            raise ValueError("La respuesta de DENUE no fue JSON interpretable.")

    df = _normalize_denue_payload(payload)

    if df.empty:
        print("DENUE devolvió una respuesta válida pero sin registros.")

    df["source_name"] = "denue_api"
    df["extraction_timestamp"] = pd.Timestamp.now()

    if save_raw:
        tag = now_tag()

        raw_json_path = Path(raw_dir) / f"denue_raw_{tag}.json"
        raw_parquet_path = Path(raw_dir) / f"denue_raw_{tag}.parquet"

        with open(raw_json_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        df.to_parquet(raw_parquet_path, index=False)

        print(f"JSON raw guardado en: {raw_json_path}")
        print(f"Parquet raw guardado en: {raw_parquet_path}")

    return df