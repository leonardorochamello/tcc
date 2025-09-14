import json, random, argparse, time
from uuid import uuid4
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

parser = argparse.ArgumentParser()
parser.add_argument("--bootstrap", default="localhost:29092")
parser.add_argument("--n", type=int, default=800, help="quantidade de tickets")
parser.add_argument("--dias", type=int, default=60, help="últimos D dias")
parser.add_argument("--sleep-ms", type=int, default=0)
args = parser.parse_args()

CANAIS = ["whatsapp", "email", "telefone", "loja"]
STATUS_FINAL = [("resolvido", 0.86), ("cancelado", 0.14)]

def ts_aleatorio_ultimos_dias(dias: int) -> datetime:
    agora = datetime.now(timezone.utc)
    inicio = agora - timedelta(days=dias)
    delta = agora - inicio
    base = inicio + timedelta(seconds=random.randint(0, int(delta.total_seconds())))
    # favorece horário comercial
    hora = random.choices(range(24),
        weights=[1,1,1,1,1,1,3,5,7,9,10,10,10,9,8,7,6,4,3,2,1,1,1,1], k=1)[0]
    return base.replace(hour=hora, minute=random.randint(0,59), second=random.randint(0,59))

producer = KafkaProducer(
    bootstrap_servers=args.bootstrap,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
)

def enviar_ticket():
    id_ticket = f"TCK-{str(uuid4())[:8].upper()}"
    id_cliente = f"CLI-{str(uuid4())[:6].upper()}"
    canal = random.choice(CANAIS)

    # tempos (segundos)
    tpr = random.randint(30, 15*60)                 # 0,5 a 15 min
    final = random.choices([s for s,_ in STATUS_FINAL], weights=[w for _,w in STATUS_FINAL], k=1)[0]
    tmr  = 0 if final != "resolvido" else random.randint(10*60, 6*60*60)  # 10min a 6h

    base_ts = ts_aleatorio_ultimos_dias(args.dias)

    eventos = [
        {
            "id_evento": f"EVT-SUP-{str(uuid4())[:8].upper()}",
            "data_evento": base_ts.isoformat(),
            "id_ticket": id_ticket,
            "id_cliente": id_cliente,
            "canal": canal,
            "status": "aberto",
            "tempo_primeira_resposta": None,
            "tempo_resolucao": 0,
            "satisfacao_cliente": None,
            "interacoes": 1
        },
        {
            "id_evento": f"EVT-SUP-{str(uuid4())[:8].upper()}",
            "data_evento": (base_ts + timedelta(minutes=random.randint(5,30))).isoformat(),
            "id_ticket": id_ticket,
            "id_cliente": id_cliente,
            "canal": canal,
            "status": "em_atendimento",
            "tempo_primeira_resposta": tpr,
            "tempo_resolucao": 0,
            "satisfacao_cliente": None,
            "interacoes": random.randint(1,3)
        }
    ]

    # evento final
    eventos.append({
        "id_evento": f"EVT-SUP-{str(uuid4())[:8].upper()}",
        "data_evento": (base_ts + timedelta(minutes=random.randint(30, 300))).isoformat(),
        "id_ticket": id_ticket,
        "id_cliente": id_cliente,
        "canal": canal,
        "status": final,
        "tempo_primeira_resposta": tpr,
        "tempo_resolucao": tmr,
        "satisfacao_cliente": None if final != "resolvido" else random.choice([3,4,5]),
        "interacoes": random.randint(1,5)
    })

    for e in eventos:
        producer.send("paper_atendimento", e)

for i in range(args.n):
    enviar_ticket()
    if args.sleep_ms > 0: time.sleep(args.sleep_ms/1000)

producer.flush()
print(f"✅ Enviados {args.n} tickets distribuídos em {args.dias} dias.")