import json, argparse, random, time
from uuid import uuid4
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

parser = argparse.ArgumentParser()
parser.add_argument("--bootstrap", default="localhost:29092")
parser.add_argument("--n-carrinhos", type=int, default=200, help="quantidade de carrinhos (fluxos)")
parser.add_argument("--dias", type=int, default=30, help="distribuir nos últimos D dias")
parser.add_argument("--sleep-ms", type=int, default=0)
args = parser.parse_args()

producer = KafkaProducer(
    bootstrap_servers=args.bootstrap,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
)

CATALOGO = [
    {"sku":"SKU-CANECA-BR","preco":39.9},
    {"sku":"SKU-PLANNER-A5","preco":49.9},
    {"sku":"SKU-CADERNO-ESP","preco":34.9},
    {"sku":"SKU-BLOCO-NOTAS","preco":19.9}
]

def ts_aleatorio_ultimos_dias(dias: int) -> datetime:
    agora = datetime.now(timezone.utc)
    inicio = agora - timedelta(days=dias)
    delta = agora - inicio
    base = inicio + timedelta(seconds=random.randint(0, int(delta.total_seconds())))
    hora = random.choices(range(24),
        weights=[1,1,1,1,1,1,3,5,7,9,10,10,10,9,8,7,6,4,3,2,1,1,1,1], k=1)[0]
    return base.replace(hour=hora, minute=random.randint(0,59), second=random.randint(0,59))

def itens_aleatorios():
    n = random.choices([1,2,3],[0.7,0.25,0.05])[0]
    picks = random.sample(CATALOGO, k=n)
    itens = []
    for p in picks:
        itens.append({
            "sku": p["sku"],
            "quantidade": random.choices([1,2,3],[0.8,0.18,0.02])[0],
            "preco_unitario": p["preco"],
            "desconto": random.choice([0,0,0,5,10])
        })
    return itens

def valor_total(itens):
    total = 0.0
    for i in itens:
        total += i["quantidade"] * (i["preco_unitario"] - i["desconto"])
    return round(total, 2)

for _ in range(args["n_carrinhos"] if isinstance(args, dict) else args.n_carrinhos):
    id_sessao = f"CHAT-{str(uuid4())[:8].upper()}"
    id_carrinho = f"CARR-{str(uuid4())[:8].upper()}"
    base_ts = ts_aleatorio_ultimos_dias(args.dias)

    ev_criar = {
        "id_evento": f"EVT-CARR-{str(uuid4())[:8].upper()}",
        "data_evento": base_ts.isoformat(),
        "id_carrinho": id_carrinho,
        "id_sessao_chat": id_sessao,
        "id_cliente": None,
        "acao": "criar",
        "itens": [],
        "valor_total": 0.0
    }
    producer.send("paper_carrinho", ev_criar)

    itens = itens_aleatorios()
    ev_add = {
        "id_evento": f"EVT-CARR-{str(uuid4())[:8].upper()}",
        "data_evento": (base_ts + timedelta(minutes=random.randint(1, 15))).isoformat(),
        "id_carrinho": id_carrinho,
        "id_sessao_chat": id_sessao,
        "id_cliente": None,
        "acao": "adicionar",
        "itens": itens,
        "valor_total": valor_total(itens)
    }
    producer.send("paper_carrinho", ev_add)

    if random.random() < 0.15:
        ev_rem = {
            "id_evento": f"EVT-CARR-{str(uuid4())[:8].upper()}",
            "data_evento": (base_ts + timedelta(minutes=random.randint(2, 25))).isoformat(),
            "id_carrinho": id_carrinho,
            "id_sessao_chat": id_sessao,
            "id_cliente": None,
            "acao": "remover",
            "itens": itens[:1],
            "valor_total": valor_total(itens[1:]) if len(itens) > 1 else 0.0
        }
        itens = itens[1:] if len(itens) > 1 else []
        producer.send("paper_carrinho", ev_rem)

    if random.random() < 0.85 and itens:
        ev_checkout = {
            "id_evento": f"EVT-CARR-{str(uuid4())[:8].upper()}",
            "data_evento": (base_ts + timedelta(minutes=random.randint(5, 40))).isoformat(),
            "id_carrinho": id_carrinho,
            "id_sessao_chat": id_sessao,
            "id_cliente": None,
            "acao": "checkout",
            "itens": [],
            "valor_total": valor_total(itens)
        }
        producer.send("paper_carrinho", ev_checkout)

        if random.random() < 0.8:
            ev_pagar = {
                "id_evento": f"EVT-CARR-{str(uuid4())[:8].upper()}",
                "data_evento": (base_ts + timedelta(minutes=random.randint(10, 60))).isoformat(),
                "id_carrinho": id_carrinho,
                "id_sessao_chat": id_sessao,
                "id_cliente": None,
                "acao": "pagar",
                "itens": [],
                "valor_total": valor_total(itens)
            }
            producer.send("paper_carrinho", ev_pagar)
        else:
            ev_aband = {
                "id_evento": f"EVT-CARR-{str(uuid4())[:8].upper()}",
                "data_evento": (base_ts + timedelta(minutes=random.randint(20, 90))).isoformat(),
                "id_carrinho": id_carrinho,
                "id_sessao_chat": id_sessao,
                "id_cliente": None,
                "acao": "abandonar",
                "itens": [],
                "valor_total": valor_total(itens)
            }
            producer.send("paper_carrinho", ev_aband)

    if args.sleep_ms > 0:
        time.sleep(args.sleep_ms/1000)

producer.flush()
print(f"✅ Simulados {args.n_carrinhos} fluxos de carrinho.")