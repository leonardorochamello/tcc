import yaml, pandas as pd

def _load_yaml(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

CFG = _load_yaml("config/config.yaml")
INTENTS = _load_yaml("config/intents.yaml")

def load_catalog(cfg=CFG):
    if cfg["catalogo"]["source"] == "csv":
        df = pd.read_csv(cfg["catalogo"]["csv_path"])
        df["tags"] = df["tags"].fillna("").apply(lambda s: [t.strip() for t in s.split(";") if t.strip()])
        return df
    raise NotImplementedError("Somente CSV neste passo")
