# app/retriever.py
from __future__ import annotations
import re
import pandas as pd
from typing import List

_STOP = {"a","o","os","as","de","da","do","das","dos","e","ou","pra","para","por","com","um","uma","uns","umas","no","na","nos","nas"}

def _tok(s: str) -> List[str]:
    s = (s or "").lower()
    return [t for t in re.findall(r"\w+", s, flags=re.UNICODE) if t and t not in _STOP]

class SimpleRetriever:
    """
    Retriever simples (sem dependências externas): pontua SKUs
    pelo overlap de tokens do nome/tags com a consulta.
    Score = 2*(match em tags) + 1*(match no nome)
    """
    def __init__(self, df: pd.DataFrame):
        self.rows = []
        for _, row in df.iterrows():
            sku = str(row["sku"])
            nome = str(row["nome"]) if "nome" in df.columns else sku
            tags = row.get("tags", [])
            if not isinstance(tags, list):
                # se ainda vier string, quebra por ; | ,
                s = str(tags)
                for sep in (";", "|", ","):
                    if sep in s:
                        tags = [t.strip() for t in s.split(sep) if t.strip()]
                        break
                else:
                    tags = [s] if s else []
            tag_tokens = set()
            for t in tags:
                tag_tokens.update(_tok(str(t)))
            name_tokens = set(_tok(nome))
            self.rows.append({
                "sku": sku,
                "tag_tokens": tag_tokens,
                "name_tokens": name_tokens,
            })
        self.all_skus = [r["sku"] for r in self.rows]

    def topk(self, query: str, k: int = 50) -> List[str]:
        q = set(_tok(query))
        if not q:
            return self.all_skus[:k]
        scored = []
        for i, r in enumerate(self.rows):
            tmatch = len(q & r["tag_tokens"])
            nmatch = len(q & r["name_tokens"])
            score = 2*tmatch + nmatch
            if score > 0:
                scored.append((score, i))
        if not scored:
            return self.all_skus[:k]
        scored.sort(key=lambda x: (-x[0], x[1]))  # maior score primeiro; desempate por ordem original
        return [self.rows[i]["sku"] for score, i in scored[:k]]