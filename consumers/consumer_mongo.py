from kafka import KafkaConsumer
from pymongo import MongoClient
from datetime import datetime, timezone
import json, os
from urllib.parse import urlparse

# --------- Desserialização segura ---------
def safe_deserialize(m: bytes):
    if m is None:
        return None
    s = m.decode("utf-8", errors="replace").strip()
    if not s:
        return None
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        print(f"⚠️ Mensagem não-JSON ignorada: {s[:120]}")
        return None

# --------- Configurações (via env) ---------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")

# Tópicos que iremos consumir
TOPICOS = [
    "paper_vendas",
    "paper_estoque",
    "paper_atendimento",
    "paper_chat",
    "paper_recomendacao",
    "paper_carrinho",
    "paper_prospeccao",
]

MONGO_URI = "mongodb+srv://aluno:unifor@unifor.az8irj3.mongodb.net"

# Nome do DB Bronze (configurável)
DB_BRONZE_NAME = os.getenv("DB_BRONZE_NAME", "papelaria_bronze")

# Mapeamento tópico -> coleção (no DB Bronze)
TOPIC_TO_COLL = {
    "paper_chat":          "conversas_chat_raw",
    "paper_recomendacao":  "recomendacoes_raw",
    "paper_carrinho":      "carrinhos_raw",
    "paper_vendas":        "pedidos_vendas_raw",
    "paper_estoque":       "mov_estoque_raw",
    "paper_atendimento":   "tickets_atend_raw",
    "paper_prospeccao":    "leads_prosp_raw",
}

# --------- Helper para mascarar credenciais no log ---------
def _mask_uri(u: str) -> str:
    try:
        parsed = urlparse(u)
        # Monta uma versão sem senha
        if parsed.username:
            user = parsed.username
            netloc = parsed.hostname or ""
            if parsed.port:
                netloc += f":{parsed.port}"
            return f"{parsed.scheme}://{user}:***@{netloc}{parsed.path or ''}"
        return f"{parsed.scheme}://{parsed.hostname or ''}{parsed.path or ''}"
    except Exception:
        return "<uri>"

# --------- Conexões ---------
consumer = KafkaConsumer(
    *TOPICOS,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_deserializer=safe_deserialize,
    enable_auto_commit=False,
    auto_offset_reset="latest",
    group_id="consumer_papelaria_ptbr_v1"
)

mongo = MongoClient(MONGO_URI)
db_bronze = mongo[DB_BRONZE_NAME]

# --------- Persistência BRONZE (append-only) ---------
def gravar_bronze(topico: str, ev: dict):
    if ev is None:
        return
    coll_name = TOPIC_TO_COLL.get(topico)
    if not coll_name:
        print(f"⚠️ Tópico não mapeado para Bronze: {topico}")
        return

    # Enriquecimento mínimo de ingestão (carimbo técnico)
    doc = dict(ev)
    doc["_ingested_at"] = datetime.now(timezone.utc)
    doc["_topic"] = topico

    db_bronze[coll_name].insert_one(doc)

    # monta um id amigável para log
    friendly_id = (
        doc.get("id_evento")
        or doc.get("id_pedido")
        or doc.get("id_carrinho")
        or doc.get("id_ticket")
        or doc.get("id_lead")
        or "<sem_id>"
    )
    print(f"✅ Bronze <- {coll_name}: inserido {friendly_id}")

# --------- Loop principal ---------
try:
    print("────────────────────────────────────────────────────────")
    print(f"Kafka bootstrap ........: {KAFKA_BOOTSTRAP}")
    print(f"Mongo URI (mascarada) ..: {_mask_uri(MONGO_URI)}")
    print(f"Mongo DB (Bronze) ......: {DB_BRONZE_NAME}")
    print(f"Tópicos ................: {', '.join(TOPICOS)}")
    print("Modo Bronze ............: append-only")
    print("────────────────────────────────────────────────────────")
    print("▶️  Iniciando consumo ...")
    for msg in consumer:
        gravar_bronze(msg.topic, msg.value)
        consumer.commit()
except KeyboardInterrupt:
    print("⏹️ Interrompido por CTRL+C.")
finally:
    consumer.close()
    mongo.close()
    print("👋 Consumer finalizado.")