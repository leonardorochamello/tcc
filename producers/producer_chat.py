import json, argparse, random, time
from uuid import uuid4
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

parser = argparse.ArgumentParser()
parser.add_argument("--bootstrap", default="localhost:29092")
parser.add_argument("--n", type=int, default=1000, help="quantidade de mensagens")
parser.add_argument("--dias", type=int, default=30, help="distribuir nos últimos D dias")
parser.add_argument("--sessoes", type=int, default=200, help="quantidade de sessões distintas")
parser.add_argument("--sleep-ms", type=int, default=0)
args = parser.parse_args()

producer = KafkaProducer(
    bootstrap_servers=args.bootstrap,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
)

INTENTS = [
    ("buscar_produto", ["caneca", "presente"]),
    ("buscar_produto", ["planner", "A5"]),
    ("ajuda", ["troca", "prazo"]),
    ("saudacao", []),
    ("finalizar_compra", [])
]

def ts_aleatorio_ultimos_dias(dias: int) -> datetime:
    agora = datetime.now(timezone.utc)
    inicio = agora - timedelta(days=dias)
    delta = agora - inicio
    base = inicio + timedelta(seconds=random.randint(0, int(delta.total_seconds())))
    hora = random.choices(range(24),
        weights=[1,1,1,1,1,1,3,5,7,9,10,10,10,9,8,7,6,4,3,2,1,1,1,1], k=1)[0]
    return base.replace(hour=hora, minute=random.randint(0,59), second=random.randint(0,59))

SESSOES = [f"CHAT-{str(uuid4())[:8].upper()}" for _ in range(args.sessoes)]

def mensagem_usuario(intent, entities):
    if "caneca" in entities:
        return "Quero uma caneca para presente"
    if "planner" in entities:
        return "Tem planner A5?"
    if intent == "ajuda":
        return "Como funciona a troca? Qual o prazo?"
    if intent == "finalizar_compra":
        return "Quero finalizar a compra"
    return "Oi"

def mensagem_bot(intent):
    if intent == "buscar_produto":
        return "Temos opções disponíveis. Quer ver recomendações?"
    if intent == "ajuda":
        return "Claro! Posso explicar como funciona a troca."
    if intent == "finalizar_compra":
        return "Posso te ajudar a concluir o pedido."
    return "Olá! Como posso ajudar?"

for i in range(args.n):
    sess = random.choice(SESSOES)
    intent, entities = random.choice(INTENTS)
    base_ts = ts_aleatorio_ultimos_dias(args.dias)

    ev_user = {
        "id_evento": f"EVT-CHAT-{str(uuid4())[:8].upper()}",
        "data_evento": base_ts.isoformat(),
        "id_sessao_chat": sess,
        "remetente": "usuario",
        "mensagem": mensagem_usuario(intent, entities),
        "intencao": intent,
        "entidades": entities,
        "id_cliente": None
    }
    producer.send("paper_chat", ev_user)

    if random.random() < 0.85:
        ev_bot = {
            "id_evento": f"EVT-CHAT-{str(uuid4())[:8].upper()}",
            "data_evento": (base_ts + timedelta(seconds=random.randint(1, 8))).isoformat(),
            "id_sessao_chat": sess,
            "remetente": "bot",
            "mensagem": mensagem_bot(intent),
            "intencao": intent,
            "entidades": entities,
            "id_cliente": None
        }
        producer.send("paper_chat", ev_bot)

    if args.sleep_ms > 0:
        time.sleep(args.sleep_ms/1000)

producer.flush()
print(f"✅ Enviadas {args.n} mensagens (com respostas do bot quando aplicável).")