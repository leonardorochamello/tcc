import json
from kafka import KafkaProducer
from uuid import uuid4
from datetime import datetime, timezone

def _id(prefix): return f"{prefix}-{str(uuid4())[:8].upper()}"
def _now(): return datetime.now(timezone.utc).isoformat()

class EventBus:
    def __init__(self, bootstrap, topics):
        self.topics = topics
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"))

    def send_chat(self, sess, remetente, msg, intencao, entidades):
        self.producer.send(self.topics["chat"], {
            "id_evento": _id("EVT-CHAT"), "data_evento": _now(),
            "id_sessao_chat": sess, "remetente": remetente,
            "mensagem": msg, "intencao": intencao, "entidades": entidades, "id_cliente": None
        })

    def send_rec(self, sess, skus, origem="ia_local"):
        if not skus: return
        self.producer.send(self.topics["recomendacao"], {
            "id_evento": _id("EVT-REC"), "data_evento": _now(),
            "id_sessao_chat": sess, "origem": origem,
            "produtos": [{"sku": s, "motivo_sugestao": origem} for s in skus]
        })

    def send_cart(self, carr, sess, acao, itens, total):
        self.producer.send(self.topics["carrinho"], {
            "id_evento": _id("EVT-CARR"), "data_evento": _now(),
            "id_carrinho": carr, "id_sessao_chat": sess, "id_cliente": None,
            "acao": acao, "itens": itens, "valor_total": round(total, 2)
        })