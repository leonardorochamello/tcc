# # app/settings.py
# from __future__ import annotations
# import os
# from pathlib import Path
# import yaml
# import pandas as pd

# # Base do projeto dentro do container (…/workspace)
# BASE_DIR = Path(__file__).resolve().parents[1]

# def _load_yaml(rel_path: str):
#     path = BASE_DIR / rel_path  # ex.: "config/config.yaml"
#     with open(path, "r", encoding="utf-8") as f:
#         return yaml.safe_load(f)

# # Carrega configs
# CFG = _load_yaml("config/config.yaml")

# # Prioriza variáveis de ambiente (substitui o que veio do YAML)
# CFG["kafka"]["bootstrap"] = os.getenv("KAFKA_BOOTSTRAP", CFG["kafka"]["bootstrap"])
# CFG["mongo"]["uri"]       = os.getenv("MONGO_URI",       CFG["mongo"]["uri"])

# # Carrega intents (espera um arquivo YAML em config/intents.yaml)
# # Estrutura esperada: { intents: [ ... ] } ou direto uma lista
# _raw_intents = _load_yaml("config/intents.yaml")
# if isinstance(_raw_intents, dict) and "intents" in _raw_intents:
#     INTENTS = _raw_intents["intents"]
# else:
#     INTENTS = _raw_intents  # caso seja lista pura

# # Utilitário para catálogo (CSV ou Mongo conforme CFG)
# def load_catalog(cfg=None):
#     cfg = cfg or CFG
#     src = cfg.get("catalogo", {}).get("source", "csv")
#     if src == "csv":
#         csv_path = BASE_DIR / cfg["catalogo"]["csv_path"]
#         return pd.read_csv(csv_path)
#     elif src == "mongo":
#         raise NotImplementedError("Leitura do catálogo via Mongo não implementada.")
#     else:
#         raise ValueError(f"Fonte de catálogo desconhecida: {src}")

# app/settings.py
from __future__ import annotations
import os, json
from pathlib import Path
import yaml
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]

def _load_yaml(rel_path: str):
    path = BASE_DIR / rel_path
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

# Carrega configs
CFG = _load_yaml("config/config.yaml")
CFG["kafka"]["bootstrap"] = os.getenv("KAFKA_BOOTSTRAP", CFG["kafka"]["bootstrap"])
CFG["mongo"]["uri"]       = os.getenv("MONGO_URI",       CFG["mongo"]["uri"])

# Intents
_raw_intents = _load_yaml("config/intents.yaml")
INTENTS = _raw_intents["intents"] if isinstance(_raw_intents, dict) and "intents" in _raw_intents else _raw_intents

def _ensure_tags_list(df: pd.DataFrame) -> pd.DataFrame:
    if "tags" not in df.columns:
        return df
    def to_list(x):
        if isinstance(x, list): return x
        if pd.isna(x): return []
        s = str(x).strip()
        # JSON list
        if s.startswith("[") and s.endswith("]"):
            try:
                v = json.loads(s)
                return v if isinstance(v, list) else [str(v)]
            except Exception:
                pass
        # separadores comuns (seu CSV está em 'semicolon_sep')
        for sep in (";", "|", ","):
            if sep in s:
                return [t.strip() for t in s.split(sep) if t.strip()]
        return [s] if s else []
    df["tags"] = df["tags"].apply(to_list)
    # tipos auxiliares
    if "preco" in df.columns:
        df["preco"] = pd.to_numeric(df["preco"], errors="coerce").fillna(0.0)
    return df

def load_catalog(cfg=None):
    cfg = cfg or CFG
    src = cfg.get("catalogo", {}).get("source", "csv")
    if src == "csv":
        csv_path = BASE_DIR / cfg["catalogo"]["csv_path"]
        df = pd.read_csv(csv_path)
        return _ensure_tags_list(df)
    elif src == "mongo":
        raise NotImplementedError("Leitura do catálogo via Mongo não implementada.")
    else:
        raise ValueError(f"Fonte de catálogo desconhecida: {src}")
