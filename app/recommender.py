def rec_by_tags(df_catalog, entidades, k=3):
    tags = set(t.lower() for t in entidades)
    scored = []
    for _, row in df_catalog.iterrows():
        prod_tags = set(t.lower() for t in row["tags"])
        score = len(tags & prod_tags)
        if score > 0:
            scored.append((score, row["sku"]))
    scored.sort(reverse=True, key=lambda x: x[0])
    return [sku for _, sku in scored[:k]]

import re

def match_skus_by_text(df_catalog, text: str):
    """Encontra SKUs citados pelo usuário comparando com tags do catálogo."""
    t = text.lower()
    hits = []
    for _, row in df_catalog.iterrows():
        sku = row["sku"]
        tags = [str(x).lower() for x in row["tags"]]
        if any(tag in t for tag in tags) or sku.lower() in t:
            hits.append(sku)
    # remove duplicados mantendo ordem
    return list(dict.fromkeys(hits))

def extract_qty(text: str, default=1):
    m = re.search(r"(\d+)\s*(un|unid|unidade|unidades|x)?", text.lower())
    return int(m.group(1)) if m else default

def compose_grounded_reply(intent: str, skus: list[str], df_catalog, extra=None) -> str:
    """Gera uma resposta com base no catálogo (preço/nome), sem depender do LLM."""
    def preco(sku):
        row = df_catalog.loc[df_catalog["sku"] == sku]
        return float(row["preco"].iloc[0]) if not row.empty else 0.0

    if intent == "buscar_produto" and skus:
        linhas = [f"- {sku}: R$ {preco(sku):.2f}".replace(".", ",") for sku in skus[:3]]
        return "Temos estas opções:\n" + "\n".join(linhas) + "\nPosso adicionar alguma ao carrinho?"
    if intent == "preco" and skus:
        return f"O preço de {skus[0]} é R$ {preco(skus[0]):.2f}".replace(".", ",")
    if intent == "ajuda":
        return "Claro! Trocas em até 7 dias corridos com nota e sem uso."
    if intent in {"checkout","pagar"}:
        return "Certo! Vou avançar com o processo."
    # fallback simpático
    return "Entendi. Posso sugerir canecas, planners, cadernos e papéis."