import re
import requests
import time

GREET_RE = re.compile(r'^\s*(oi|olá|ola|bom dia|boa tarde|boa noite)\b', re.IGNORECASE)
def is_greeting(text: str) -> bool:
    return bool(GREET_RE.match(text or ""))

CHECKOUT_RE = re.compile(r'(?i)(finalizar|concluir|fechar)\s+(compra|pedido|carrinho)|\bcheckout\b|ir\s+para\s+o\s+pagamento')
PAGAR_RE = re.compile(r'(?i)\b(pagar|pagamento|quero pagar)\b')

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

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
from app.retriever import SimpleRetriever   # <<< usa o retriever simples (sem sklearn)


def is_add_intent(text: str) -> bool:
    t = text.lower()
    return bool(re.search(r'\b(adiciona|adicione|coloca|p(o|ô)e|por no carrinho|coloca no carrinho)\b', t))

def new_id(prefix: str) -> str:
    return f"{prefix}-{str(uuid4())[:8].upper()}"


# --- carga de config, catálogo e retriever ---
df_catalog = load_catalog(CFG)
SKUS = df_catalog["sku"].tolist()
retr = SimpleRetriever(df_catalog)  # RAG "lite" para filtrar SKUs por mensagem

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
if "last_candidates" not in st.session_state:
    st.session_state.last_candidates = []  # SKUs sugeridos mais recentes (p/ "adicione")

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
    # 0) Saudações / msg curtíssima -> sem IA (curto-circuito)
    if is_greeting(msg) or len(msg.strip().split()) <= 1:
        out = {
            "resposta": "Oi! 👋 Posso ajudar com planners, canetas, cadernos e presentes. Exemplos: 'preço da SKU-PLANNER-A5', 'caneta preta', 'adiciona 2x SKU-...'",
            "intencao": "ajuda",
            "entidades": [],
            "recomendacoes": [],
            "acao_carrinho": None,
        }

    # 1) Preço / busca com SKU detectado -> determinístico
    elif rule_intent in {"preco", "buscar_produto"} and matched_skus:
        resposta = compose_grounded_reply(rule_intent, matched_skus, df_catalog)
        out = {
            "resposta": resposta,
            "intencao": rule_intent,
            "entidades": matched_skus,
            "recomendacoes": matched_skus[:3],
            "acao_carrinho": None,
        }

    # 2) Adicionar sem SKU explícito -> usa últimos candidatos (sem IA)
    elif rule_intent == "adicionar_carrinho" and (matched_skus or st.session_state.last_candidates):
        qtd = extract_qty(msg, default=1)
        sku = (matched_skus or st.session_state.last_candidates)[0]
        out = {
            "resposta": f"Adicionando {qtd}x {sku}.",
            "intencao": "adicionar_carrinho",
            "entidades": [sku, str(qtd)],
            "recomendacoes": [],
            "acao_carrinho": {"acao":"adicionar","sku":sku,"quantidade":int(qtd),"desconto":0},
        }

    # 2.x) Finalização/checkout sem IA
    elif rule_intent in {"checkout", "pagar"}:
        texto = "Certo! Vou avançar com o processo." if rule_intent == "checkout" else "Certo! Vou avançar com o pagamento."
        out = {
            "resposta": texto,
            "intencao": rule_intent,
            "entidades": [],
            "recomendacoes": [],
            "acao_carrinho": {"acao": rule_intent},  # dispara evento no Kafka
        }
    elif rule_intent in {"checkout","pagar"} or CHECKOUT_RE.search(msg) or PAGAR_RE.search(msg):
        intent = "pagar" if PAGAR_RE.search(msg) else "checkout"
        texto = "Certo! Vou avançar com o processo." if intent == "checkout" else "Certo! Vou avançar com o pagamento."
        out = {
            "resposta": texto,
            "intencao": intent,
            "entidades": [],
            "recomendacoes": [],
            "acao_carrinho": {"acao": intent},
        }

    # 3) Caso geral -> IA (blindado com timeout/erros) + fallback por tags
    else:
        dynamic_skus = retr.topk(msg, k=50)
        prompt_dyn = render_prompt("config/prompt.jinja2", skus=dynamic_skus)

        t0 = time.perf_counter()
        try:
            raw = llm.chat_json(system_prompt=prompt_dyn, user=msg, history=[])
        except requests.exceptions.RequestException:
            # Falha de rede/timeout/etc -> resposta simpática sem IA
            out = {
                "resposta": "Tive um problema temporário com a IA. Diga um SKU ou produto (ex.: 'preço da SKU-PLANNER-A5' ou 'caneta preta') que já te ajudo.",
                "intencao": rule_intent or "ajuda",
                "entidades": matched_skus,
                "recomendacoes": matched_skus[:3] if matched_skus else [],
                "acao_carrinho": None,
            }
        else:
            try:
                out = json.loads(raw)
            except Exception:
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