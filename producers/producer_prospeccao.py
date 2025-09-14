import json, random, argparse, time
from uuid import uuid4
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

parser = argparse.ArgumentParser()
parser.add_argument("--bootstrap", default="localhost:29092")
parser.add_argument("--n", type=int, default=600, help="quantidade de leads")
parser.add_argument("--dias", type=int, default=90, help="últimos D dias")
parser.add_argument("--sleep-ms", type=int, default=0)
args = parser.parse_args()

ORIGENS = [("organico",0.25),("indicacao",0.15),("redes_sociais",0.25),("anuncio",0.25),("feiras",0.10)]
ETAPAS  = ["novo","contatado","qualificado","proposta","ganho","perdido"]

def escolha_ponderada(pares):
    nomes, pesos = zip(*pares)
    return random.choices(nomes, weights=pesos, k=1)[0]

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

def enviar_lead():
    id_lead = f"LEAD-{str(uuid4())[:8].upper()}"
    origem = escolha_ponderada(ORIGENS)
    # simulamos um “estado atual” do lead (não event-sourcing completo aqui)
    etapa = random.choices(ETAPAS, weights=[0.15,0.2,0.25,0.2,0.1,0.1], k=1)[0]
    contatos = {
        "novo": (0,20), "contatado": (1,40), "qualificado": (2,60),
        "proposta": (3,75), "ganho": (4,90), "perdido": (2,40)
    }[etapa]
    n_contatos = random.randint(0, contatos[0]) if etapa=="novo" else random.randint(1, contatos[0])
    pontuacao = random.randint(max(0,contatos[1]-15), min(100,contatos[1]+15))

    evento = {
        "id_evento": f"EVT-LEAD-{str(uuid4())[:8].upper()}",
        "data_evento": ts_aleatorio_ultimos_dias(args.dias).isoformat(),
        "id_lead": id_lead,
        "origem": origem,
        "etapa": etapa,
        "contatos": n_contatos,
        "pontuacao": pontuacao
    }
    producer.send("paper_prospeccao", evento)

for i in range(args.n):
    enviar_lead()
    if args.sleep_ms > 0: time.sleep(args.sleep_ms/1000)

producer.flush()
print(f"✅ Enviados {args.n} leads distribuídos em {args.dias} dias.")