import json, random, argparse, time
from uuid import uuid4
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

parser = argparse.ArgumentParser()
parser.add_argument("--bootstrap", default="localhost:29092")
parser.add_argument("--n", type=int, default=2000, help="quantidade de movimentações")
parser.add_argument("--dias", type=int, default=60, help="últimos D dias")
parser.add_argument("--sleep-ms", type=int, default=0)
args = parser.parse_args()

CATALOGO = ["SKU-CANECA-BR","SKU-PLANNER-A5","SKU-CADERNO-ESP","SKU-BLOCO-NOTAS"]
WAREHOUSES = ["central","loja_fisica"]
MOVS = ["entrada","saida","devolucao","ajuste"]
MOV_WEIGHTS = [0.35,0.45,0.12,0.08]

REASONS = {
    "entrada": ["reposicao","compra_fornecedor"],
    "saida": ["venda","consumo_interno"],
    "devolucao": ["devolucao_cliente"],
    "ajuste": ["acerto_inventario","perda_danificado"]
}

def ts_aleatorio_ultimos_dias(dias: int) -> datetime:
    agora = datetime.now(timezone.utc)
    inicio = agora - timedelta(days=dias)
    delta = agora - inicio
    base = inicio + timedelta(seconds=random.randint(0, int(delta.total_seconds())))
    hora = random.choices(range(24),
                          weights=[1,1,1,1,1,1,3,5,7,9,10,10,10,9,8,7,6,4,3,2,1,1,1,1], k=1)[0]
    return base.replace(hour=hora, minute=random.randint(0,59), second=random.randint(0,59))

producer = KafkaProducer(
    bootstrap_servers=args.bootstrap,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
)

def enviar_mov():
    movimento = random.choices(MOVS, weights=MOV_WEIGHTS, k=1)[0]
    motivo = random.choice(REASONS[movimento])
    if movimento == "entrada":
        quantidade = random.choice([5,10,20,30])
    elif movimento == "saida":
        quantidade = random.choice([1,2,3,4,5])
    elif movimento == "devolucao":
        quantidade = random.choice([1,2,3])
    else:
        quantidade = random.choice([1,2,5,10])

    evento = {
        "id_evento": f"EVT-EST-{str(uuid4())[:8].upper()}",
        "data_evento": ts_aleatorio_ultimos_dias(args.dias).isoformat(),
        "id_produto": random.choice(CATALOGO),
        "movimento": movimento,
        "quantidade": int(quantidade),
        "motivo": motivo,
        "local_estoque": random.choice(WAREHOUSES)
    }
    producer.send("paper_estoque", evento)

for i in range(args.n):
    enviar_mov()
    if args.sleep_ms > 0:
        time.sleep(args.sleep_ms/1000)

producer.flush()
print(f"✅ Enviadas {args.n} movimentações de estoque em {args.dias} dias.")