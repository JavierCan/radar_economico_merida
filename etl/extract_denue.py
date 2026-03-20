from __future__ import annotations

from pathlib import Path
from urllib.parse import quote
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


def build_denue_url(
    base_url: str,
    palabra: str,
    latitud: float,
    longitud: float,
    radio: int,
    token: str,
) -> str:
    palabra_encoded = quote(str(palabra).strip(), safe="")
    return f"{base_url.rstrip('/')}/{palabra_encoded}/{latitud},{longitud}/{radio}/{token}"


def extract_denue(save_raw: bool = True) -> pd.DataFrame:
    settings = load_settings()
    denue_cfg = settings["denue"]
    raw_dir = ensure_dir(settings["paths"]["raw"])

    if not denue_cfg.get("enabled", False):
        raise RuntimeError("La fuente DENUE está deshabilitada en settings.yaml")

    token_env_name = denue_cfg.get("api_token_env", "INEGI_API_TOKEN")
    base_url_env_name = denue_cfg.get("base_url_env", "DENUE_BASE_URL")

    api_token = get_env(token_env_name, "").strip()
    base_url = get_env(base_url_env_name, "").strip()

    if not api_token:
        raise ValueError(
            f"No existe el token en la variable de entorno '{token_env_name}'"
        )

    if not base_url:
        raise ValueError(
            f"No existe la URL base en la variable de entorno '{base_url_env_name}'"
        )

    queries = denue_cfg.get("queries", [])
    if not queries:
        raise ValueError("No se definieron queries en denue.queries dentro de settings.yaml")

    location = denue_cfg.get("location", {})
    latitud = location.get("latitud")
    longitud = location.get("longitud")
    radio = location.get("radio", 10000)

    if latitud is None or longitud is None:
        raise ValueError("Debes definir latitud y longitud en denue.location dentro de settings.yaml")

    timeout_seconds = int(denue_cfg.get("timeout_seconds", 60))

    payloads_by_query: dict[str, object] = {}
    dfs: list[pd.DataFrame] = []

    for palabra in queries:
        url = build_denue_url(
            base_url=base_url,
            palabra=palabra,
            latitud=latitud,
            longitud=longitud,
            radio=radio,
            token=api_token,
        )

        response = requests.get(url, timeout=timeout_seconds)
        response.raise_for_status()

        try:
            payload = response.json()
        except Exception as e:
            raise ValueError(
                f"La respuesta de DENUE no fue JSON interpretable para '{palabra}': {e}"
            )

        payloads_by_query[palabra] = payload

        df_query = _normalize_denue_payload(payload)

        if not df_query.empty:
            df_query["search_term"] = palabra
            df_query["source_name"] = "denue_api"
            df_query["extraction_timestamp"] = pd.Timestamp.now()
            dfs.append(df_query)

        print(f"Consulta completada para: {palabra}")

    if not dfs:
        print("DENUE devolvió respuestas válidas pero sin registros en todas las búsquedas.")
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)

    # Deduplicación básica
    preferred_keys = [col for col in ["id", "ID", "Id", "CLEE", "clee"] if col in df.columns]
    if preferred_keys:
        df = df.drop_duplicates(subset=preferred_keys)
    else:
        df = df.drop_duplicates()

    if save_raw:
        tag = now_tag()

        raw_json_path = Path(raw_dir) / f"denue_raw_{tag}.json"
        raw_parquet_path = Path(raw_dir) / f"denue_raw_{tag}.parquet"

        with open(raw_json_path, "w", encoding="utf-8") as f:
            json.dump(payloads_by_query, f, ensure_ascii=False, indent=2)

        df.to_parquet(raw_parquet_path, index=False)

        print(f"JSON raw guardado en: {raw_json_path}")
        print(f"Parquet raw guardado en: {raw_parquet_path}")
        print(f"Registros consolidados: {len(df)}")

    return df