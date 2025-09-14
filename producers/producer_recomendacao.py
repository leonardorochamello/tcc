import json, argparse, random, time
from uuid import uuid4
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

parser = argparse.ArgumentParser()
parser.add_argument("--bootstrap", default="localhost:29092")
parser.add_argument("--n", type=int, default=500, help="quantidade de eventos de recomendação")
parser.add_argument("--dias", type=int, default=30, help="distribuir nos últimos D dias")
parser.add_argument("--sessoes", type=int, default=200, help="pool de sessões")
parser.add_argument("--sleep-ms", type=int, default=0)
args = parser.parse_args()

producer = KafkaProducer(
    bootstrap_servers=args.bootstrap,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
)

CATALOGO = [
    {"sku": "SKU-CANECA-BR", "tags": ["caneca", "presente", "personalizado"]},
    {"sku": "SKU-PLANNER-A5", "tags": ["planner", "A5", "organizacao"]},
    {"sku": "SKU-CADERNO-ESP", "tags": ["caderno", "espiral"]},
    {"sku": "SKU-PLANNER-A4", "tags": ["planner", "A4"]}
]

ORIGENS = ["top_vendas", "similaridade", "promo"]

def ts_aleatorio_ultimos_dias(dias: int) -> datetime:
    agora = datetime.now(timezone.utc)
    inicio = agora - timedelta(days=dias)
    delta = agora - inicio
    base = inicio + timedelta(seconds=random.randint(0, int(delta.total_seconds())))
    hora = random.choices(range(24),
        weights=[1,1,1,1,1,1,3,5,7,9,10,10,10,9,8,7,6,4,3,2,1,1,1,1], k=1)[0]
    return base.replace(hour=hora, minute=random.randint(0,59), second=random.randint(0,59))

SESSOES = [f"CHAT-{str(uuid4())[:8].upper()}" for _ in range(args.sessoes)]

for i in range(args.n):
    sess = random.choice(SESSOES)
    base_ts = ts_aleatorio_ultimos_dias(args.dias)
    n_prods = random.choices([2,3,4],[0.6,0.3,0.1])[0]
    prods = random.sample(CATALOGO, k=min(n_prods, len(CATALOGO)))
    itens = []
    for p in prods:
        itens.append({
            "sku": p["sku"],
            "motivo_sugestao": random.choice(["similaridade_por_tags", "top_vendas", "promo_em_andamento"])
        })

    event = {
        "id_evento": f"EVT-REC-{str(uuid4())[:8].upper()}",
        "data_evento": base_ts.isoformat(),
        "id_sessao_chat": sess,
        "origem": random.choice(ORIGENS),
        "produtos": itens
    }

    producer.send("paper_recomendacao", event)
    if args.sleep_ms > 0:
        time.sleep(args.sleep_ms/1000)

producer.flush()
print(f"✅ Enviados {args.n} eventos de recomendação.")