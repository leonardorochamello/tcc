import json
import streamlit as st
from uuid import uuid4

# --- imports dos módulos do projeto ---
from app.settings import CFG, INTENTS, load_catalog
from app.ai_provider import OllamaProvider, render_prompt
from app.rules import first_match_intent
from app.recommender import (
    rec_by_tags, match_skus_by_text, extract_qty, compose_grounded_reply
)
from app.event_bus import EventBus


def new_id(prefix: str) -> str:
    return f"{prefix}-{str(uuid4())[:8].upper()}"


# --- carga de config, catálogo e prompt ---
df_catalog = load_catalog(CFG)
SKUS = df_catalog["sku"].tolist()
prompt = render_prompt("config/prompt.jinja2", skus=SKUS)

# --- providers (IA e Eventos) ---
llm = OllamaProvider(model=CFG["llm"]["model"], temperature=CFG["llm"]["temperature"])
bus = EventBus(bootstrap=CFG["kafka"]["bootstrap"], topics=CFG["kafka"]["topics"])

# --- UI base ---
st.set_page_config(page_title="Papelaria (config-driven)", page_icon="🧠", layout="centered")
st.title("🧠 Chat da Papelaria (config-driven → Kafka → Mongo)")

if "sess" not in st.session_state:
    st.session_state.sess = new_id("CHAT")
if "cart" not in st.session_state:
    st.session_state.cart = None
if "hist" not in st.session_state:
    st.session_state.hist = []  # [(role, content)]

with st.sidebar:
    st.subheader("Sessão")
    st.text_input("Kafka bootstrap", value=CFG["kafka"]["bootstrap"], disabled=True)
    st.text_input("id_sessao_chat", value=st.session_state.sess, disabled=True)
    if st.button("Nova sessão"):
        st.session_state.sess = new_id("CHAT")
        st.session_state.cart = None
        st.session_state.hist = []
        st.rerun()

# histórico renderizado
for role, content in st.session_state.hist:
    with st.chat_message("user" if role == "user" else "assistant"):
        st.markdown(content)

# ---------------- Chat Loop ----------------
msg = st.chat_input("Digite sua mensagem…")
if msg:
    # 0) registra mensagem do usuário (UI + evento)
    st.session_state.hist.append(("user", msg))
    bus.send_chat(st.session_state.sess, "usuario", msg, "outro", [])

    # 1) heurística rápida + SKUs mencionados
    rule_intent = first_match_intent(msg, INTENTS)
    matched_skus = match_skus_by_text(df_catalog, msg)

    # ================== Construção da resposta (prioridade) ==================
    # 1) Perguntas de preço / busca com SKU identificado -> resposta grounded (determinística)
    if rule_intent in {"preco", "buscar_produto"} and matched_skus:
        resposta = compose_grounded_reply(rule_intent, matched_skus, df_catalog)
        out = {
            "resposta": resposta,
            "intencao": rule_intent,
            "entidades": matched_skus,
            "recomendacoes": matched_skus[:3],
            "acao_carrinho": None,
        }

    # 2) Pedido explícito para adicionar item ao carrinho -> ação determinística
    elif rule_intent == "adicionar_carrinho" and matched_skus:
        qtd = extract_qty(msg, default=1)
        sku = matched_skus[0]
        row = df_catalog.loc[df_catalog["sku"] == sku]
        preco_unit = float(row["preco"].iloc[0]) if not row.empty else 0.0

        resposta = f"Adicionando {qtd}x {sku}."
        out = {
            "resposta": resposta,
            "intencao": "adicionar_carrinho",
            "entidades": [sku, str(qtd)],
            "recomendacoes": [],
            "acao_carrinho": {
                "acao": "adicionar",
                "sku": sku,
                "quantidade": int(qtd),
                "desconto": 0,
            },
        }

    # 3) Caso geral -> IA (JSON) + fallback por tags
    else:
        raw = llm.chat_json(system_prompt=prompt, user=msg, history=[])
        try:
            out = json.loads(raw)
        except Exception:
            # Fallback mínimo quando o modelo não retorna JSON válido
            out = {
                "resposta": "Pode reformular?",
                "intencao": rule_intent or "outro",
                "entidades": matched_skus,
                "recomendacoes": matched_skus[:3] if matched_skus else [],
                "acao_carrinho": None,
            }

        # Se o LLM não recomendou nada, recomende por tags do catálogo
        if not out.get("recomendacoes"):
            out["recomendacoes"] = rec_by_tags(
                df_catalog,
                out.get("entidades", matched_skus),
                k=CFG["recs"]["max_itens"],
            )

        # Se intenção veio vazia/“outro” e a regra detectou algo, use a intenção da regra
        if not out.get("intencao") or out.get("intencao") in {"", "outro"}:
            if rule_intent:
                out["intencao"] = rule_intent

        # Garantia de resposta legível
        if not out.get("resposta"):
            out["resposta"] = "OK"
    # ================== /construção da resposta ==================

    # 4) UI: mostrar resposta do bot e publicar eventos Kafka
    resp = out.get("resposta", "OK")
    st.session_state.hist.append(("assistant", resp))
    bus.send_chat(
        st.session_state.sess,
        "bot",
        resp,
        out.get("intencao", "outro"),
        out.get("entidades", []),
    )

    recs = out.get("recomendacoes", [])
    if recs:
        bus.send_rec(st.session_state.sess, recs, origem="ia_local")

    ac = out.get("acao_carrinho")
    if ac:
        if st.session_state.cart is None:
            st.session_state.cart = new_id("CARR")
            bus.send_cart(st.session_state.cart, st.session_state.sess, "criar", [], 0.0)

        if ac.get("acao") == "adicionar" and ac.get("sku") in SKUS:
            sku = ac["sku"]
            qtd = int(ac.get("quantidade", 1))
            desc = int(ac.get("desconto", 0))
            preco = float(df_catalog.loc[df_catalog["sku"] == sku, "preco"].iloc[0])
            item = [{
                "sku": sku,
                "quantidade": qtd,
                "preco_unitario": preco,
                "desconto": desc
            }]
            total = sum(i["quantidade"] * (i["preco_unitario"] - i["desconto"]) for i in item)
            bus.send_cart(st.session_state.cart, st.session_state.sess, "adicionar", item, total)
        elif ac.get("acao") in {"checkout", "pagar"}:
            bus.send_cart(st.session_state.cart, st.session_state.sess, ac["acao"], [], 0.0)

    st.rerun()