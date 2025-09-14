import json, random, argparse, time
from uuid import uuid4
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

# -------- parâmetros por linha de comando --------
parser = argparse.ArgumentParser()
parser.add_argument("--bootstrap", default="localhost:29092")
parser.add_argument("--n", type=int, default=500, help="quantidade de pedidos")
parser.add_argument("--dias", type=int, default=30, help="últimos D dias")
parser.add_argument("--sleep-ms", type=int, default=0, help="intervalo entre envios (ms)")
args = parser.parse_args()

# -------- catálogo --------
CATALOGO = [
    {"sku":"SKU-CANECA-BR", "preco_unitario":39.90, "categoria":"caneca"},
    {"sku":"SKU-PLANNER-A5", "preco_unitario":49.90, "categoria":"planner"},
    {"sku":"SKU-CADERNO-ESP", "preco_unitario":34.90, "categoria":"caderno"},
    {"sku":"SKU-BLOCO-NOTAS", "preco_unitario":19.90, "categoria":"papelaria"}
]

CIDADES = [
    ("São Paulo","SP"), ("Rio de Janeiro","RJ"), ("Belo Horizonte","MG"),
    ("Fortaleza","CE"), ("Curitiba","PR"), ("Salvador","BA")
]

CANAIS          = [("online", 0.65), ("loja", 0.35)]
PAGAMENTOS      = [("pix", 0.50), ("cartao", 0.40), ("boleto", 0.10)]
STATUS_FINAL    = [("pago", 0.87), ("cancelado", 0.13)]
STATUS_PAGO_ENV = [("enviado", 0.92), ("pago", 0.08)]
DESCONTOS       = [0,0,0,5,10]

def escolha_ponderada(opcoes):
    nomes, pesos = zip(*opcoes)
    return random.choices(nomes, weights=pesos, k=1)[0]

def ts_aleatorio_ultimos_dias(dias: int) -> datetime:
    agora = datetime.now(timezone.utc)
    inicio = agora - timedelta(days=dias)
    delta = agora - inicio
    base = inicio + timedelta(seconds=random.randint(0, int(delta.total_seconds())))
    hora = random.choices(range(24),
                          weights=[1,1,1,1,1,1,3,5,7,9,10,10,10,9,8,7,6,4,3,2,1,1,1,1], k=1)[0]
    return base.replace(hour=hora, minute=random.randint(0,59), second=random.randint(0,59))

def gerar_itens():
    n_itens = random.choices([1,2,3],[0.65,0.25,0.10])[0]
    itens = []
    usados = set()
    for _ in range(n_itens):
        prod = random.choice(CATALOGO)
        while prod["sku"] in usados:
            prod = random.choice(CATALOGO)
        usados.add(prod["sku"])
        desconto = random.choice(DESCONTOS)
        itens.append({
            "sku": prod["sku"],
            "quantidade": random.choices([1,2,3],[0.7,0.25,0.05])[0],
            "preco_unitario": prod["preco_unitario"],
            "desconto": desconto
        })
    return itens

def status_realista():
    s1 = "criado"
    fim = escolha_ponderada(STATUS_FINAL)
    if fim == "cancelado":
        return [s1, "cancelado"]
    else:
        if escolha_ponderada(STATUS_PAGO_ENV) == "enviado":
            return [s1, "pago", "enviado"]
        else:
            return [s1, "pago"]

producer = KafkaProducer(
    bootstrap_servers=args.bootstrap,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
)

def enviar_pedido():
    id_pedido = f"PED-{str(uuid4())[:8].upper()}"
    id_cliente = f"CLI-{str(uuid4())[:6].upper()}"
    canal = escolha_ponderada(CANAIS)
    pagamento = escolha_ponderada(PAGAMENTOS)
    cidade, estado = random.choice(CIDADES)
    itens = gerar_itens()
    timeline_status = status_realista()
    base_ts = ts_aleatorio_ultimos_dias(args.dias)
    for idx, st in enumerate(timeline_status):
        evento = {
            "id_evento": f"EVT-{uuid4()}",
            "data_evento": (base_ts + timedelta(minutes=idx*random.randint(5,60))).isoformat(),
            "id_pedido": id_pedido,
            "id_cliente": id_cliente,
            "canal": canal,
            "itens": itens,
            "forma_pagamento": pagamento,
            "status": st,
            "cidade": cidade,
            "estado": estado
        }
        producer.send("paper_vendas", evento)

for i in range(args.n):
    enviar_pedido()
    if args.sleep_ms > 0:
        time.sleep(args.sleep_ms/1000)

producer.flush()
print(f"✅ Enviados {args.n} pedidos distribuídos em {args.dias} dias.")