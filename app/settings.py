# app/settings.py
from __future__ import annotations
import os
from pathlib import Path
import yaml
import pandas as pd

# Base do projeto dentro do container (…/workspace)
BASE_DIR = Path(__file__).resolve().parents[1]

def _load_yaml(rel_path: str):
    path = BASE_DIR / rel_path  # ex.: "config/config.yaml"
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

# Carrega configs
CFG = _load_yaml("config/config.yaml")

# Prioriza variáveis de ambiente (substitui o que veio do YAML)
CFG["kafka"]["bootstrap"] = os.getenv("KAFKA_BOOTSTRAP", CFG["kafka"]["bootstrap"])
CFG["mongo"]["uri"]       = os.getenv("MONGO_URI",       CFG["mongo"]["uri"])

# Carrega intents (espera um arquivo YAML em config/intents.yaml)
# Estrutura esperada: { intents: [ ... ] } ou direto uma lista
_raw_intents = _load_yaml("config/intents.yaml")
if isinstance(_raw_intents, dict) and "intents" in _raw_intents:
    INTENTS = _raw_intents["intents"]
else:
    INTENTS = _raw_intents  # caso seja lista pura

# Utilitário para catálogo (CSV ou Mongo conforme CFG)
def load_catalog(cfg=None):
    cfg = cfg or CFG
    src = cfg.get("catalogo", {}).get("source", "csv")
    if src == "csv":
        csv_path = BASE_DIR / cfg["catalogo"]["csv_path"]
        return pd.read_csv(csv_path)
    elif src == "mongo":
        raise NotImplementedError("Leitura do catálogo via Mongo não implementada.")
    else:
        raise ValueError(f"Fonte de catálogo desconhecida: {src}")