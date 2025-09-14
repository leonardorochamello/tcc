# 📊 Pipelines de Transformação — TCC Papelaria (Kafka → MongoDB)

Use estes pipelines no **MongoDB Atlas → Data Explorer → Collections → Aggregations → JSON**.  
Clique **Run** para materializar coleções (o último stage é sempre um **$merge**).

> **Dica:** se o Atlas pedir índice único para o campo de `on`, crie o índice com:
> `db.<colecao>.createIndex({ <campo>: 1 }, { unique: true })` no **mongosh**.

---

## 🥉 Bronze → Silver (8 pipelines)

> Fontes na camada `papelaria_bronze` e destinos na `papelaria_silver`.  
> O #3 usa a saída do #2 (consolidação de carrinhos).

### 1) Chat — `papelaria_bronze.conversas_chat_raw` → `papelaria_silver.chat_msgs`
```json
[
  {
    "$project": {
      "_id": 0,
      "id_evento": 1,
      "id_sessao_chat": 1,
      "remetente": 1,
      "mensagem": 1,
      "intencao": { "$ifNull": ["$intencao", "outro"] },
      "entidades": { "$ifNull": ["$entidades", []] },
      "data_evento": { "$toDate": "$data_evento" }
    }
  },
  {
    "$merge": {
      "into": { "db": "papelaria_silver", "coll": "chat_msgs" },
      "on": ["id_evento"],
      "whenMatched": "replace",
      "whenNotMatched": "insert"
    }
  }
]
```

### 2) Carrinhos (eventos) — `papelaria_bronze.carrinhos_raw` → `papelaria_silver.carrinhos_eventos`
```json
[
  {
    "$project": {
      "_id": 0,
      "id_evento": 1,
      "id_carrinho": 1,
      "id_sessao_chat": 1,
      "acao": 1,
      "itens": { "$ifNull": ["$itens", []] },
      "valor_total": { "$toDouble": { "$ifNull": ["$valor_total", 0] } },
      "data_evento": { "$toDate": "$data_evento" }
    }
  },
  {
    "$merge": {
      "into": { "db": "papelaria_silver", "coll": "carrinhos_eventos" },
      "on": ["id_evento"],
      "whenMatched": "replace",
      "whenNotMatched": "insert"
    }
  }
]
```

### 3) Carrinhos (estado final) — `papelaria_silver.carrinhos_eventos` → `papelaria_silver.carrinhos_estado`
> Execute este pipeline com a **coleção de origem** `papelaria_silver.carrinhos_eventos`.
```json
[
  { "$sort": { "id_carrinho": 1, "data_evento": 1 } },
  {
    "$group": {
      "_id": "$id_carrinho",
      "id_sessao_chat": { "$last": "$id_sessao_chat" },
      "status_final": { "$last": "$acao" },
      "valor_total": { "$last": "$valor_total" },
      "data_ult_evento": { "$last": "$data_evento" }
    }
  },
  {
    "$project": {
      "_id": 0,
      "id_carrinho": "$_id",
      "id_sessao_chat": 1,
      "status_final": 1,
      "valor_total": 1,
      "data_ult_evento": 1
    }
  },
  {
    "$merge": {
      "into": { "db": "papelaria_silver", "coll": "carrinhos_estado" },
      "on": ["id_carrinho"],
      "whenMatched": "replace",
      "whenNotMatched": "insert"
    }
  }
]
```

### 4) Vendas — `papelaria_bronze.pedidos_vendas_raw` → `papelaria_silver.pedidos_vendas`
```json
[
  {
    "$project": {
      "_id": 0,
      "id_pedido": 1,
      "cliente": 1,
      "canal": 1,
      "itens": {
        "$map": {
          "input": { "$ifNull": ["$itens", []] },
          "as": "i",
          "in": {
            "sku": "$$i.sku",
            "quantidade": { "$toInt": "$$i.quantidade" },
            "preco_unitario": { "$toDouble": "$$i.preco_unitario" },
            "desconto": { "$toDouble": { "$ifNull": ["$$i.desconto", 0] } }
          }
        }
      },
      "valor_total": { "$toDouble": { "$ifNull": ["$valor_total", 0] } },
      "data_evento": { "$toDate": "$data_evento" }
    }
  },
  {
    "$merge": {
      "into": { "db": "papelaria_silver", "coll": "pedidos_vendas" },
      "on": ["id_pedido"],
      "whenMatched": "replace",
      "whenNotMatched": "insert"
    }
  }
]
```

### 5) Estoque — `papelaria_bronze.mov_estoque_raw` → `papelaria_silver.estoque_movs`
```json
[
  {
    "$project": {
      "_id": 0,
      "id_evento": 1,
      "sku": { "$ifNull": ["$sku", "$id_produto"] },
      "tipo": {
        "$cond": [
          { "$in": [ { "$toLower": "$movimento" }, ["saida", "venda", "baixa"] ] },
          "saida",
          "entrada"
        ]
      },
      "quantidade": { "$toInt": "$quantidade" },
      "motivo": 1,
      "data_evento": { "$toDate": "$data_evento" }
    }
  },
  { "$match": { "sku": { "$ne": null, "$ne": "" } } },
  {
    "$merge": {
      "into": { "db": "papelaria_silver", "coll": "estoque_movs" },
      "on": ["id_evento"],
      "whenMatched": "replace",
      "whenNotMatched": "insert"
    }
  }
]
```

### 6) Atendimento — `papelaria_bronze.tickets_atend_raw` → `papelaria_silver.atendimento_tickets`
```json
[
  {
    "$project": {
      "_id": 0,
      "id_ticket": 1,
      "cliente": 1,
      "canal": 1,
      "status": { "$ifNull": ["$status", "aberto"] },
      "sla_min": { "$toDouble": { "$ifNull": ["$sla_min", 0] } },
      "csat": {
        "$cond": [
          { "$ne": ["$csat", null] },
          { "$toDouble": "$csat" },
          null
        ]
      },
      "data_evento": { "$toDate": "$data_evento" }
    }
  },
  {
    "$merge": {
      "into": { "db": "papelaria_silver", "coll": "atendimento_tickets" },
      "on": ["id_ticket"],
      "whenMatched": "replace",
      "whenNotMatched": "insert"
    }
  }
]
```

### 7) Prospecção — `papelaria_bronze.leads_prosp_raw` → `papelaria_silver.prospeccao_leads`
```json
[
  {
    "$project": {
      "_id": 0,
      "id_lead": 1,
      "origem": { "$toLower": { "$ifNull": ["$origem", "desconhecida"] } },
      "status": { "$toLower": { "$ifNull": ["$status", "novo"] } },
      "data_evento": { "$toDate": "$data_evento" }
    }
  },
  {
    "$merge": {
      "into": { "db": "papelaria_silver", "coll": "prospeccao_leads" },
      "on": ["id_lead"],
      "whenMatched": "replace",
      "whenNotMatched": "insert"
    }
  }
]
```

### 8) Recomendações — `papelaria_bronze.recomendacoes_raw` → `papelaria_silver.recomendacoes`
```json
[
  {
    "$project": {
      "_id": 0,
      "id_evento": 1,
      "id_sessao_chat": 1,
      "origem": { "$ifNull": ["$origem", "desconhecida"] },
      "lista": {
        "$cond": [
          { "$ne": ["$recomendacoes", null] },
          "$recomendacoes",
          { "$ifNull": ["$itens", []] }
        ]
      },
      "data_evento": { "$toDate": "$data_evento" }
    }
  },
  {
    "$merge": {
      "into": { "db": "papelaria_silver", "coll": "recomendacoes" },
      "on": ["id_evento"],
      "whenMatched": "replace",
      "whenNotMatched": "insert"
    }
  }
]
```

---

## 🥇 Silver → Gold (10 pipelines de KPIs)

> Fontes na camada `papelaria_silver` e destinos na `papelaria_gold`.

### 9) Vendas — KPIs por mês — `papelaria_silver.pedidos_vendas` → `papelaria_gold.vendas_kpis_mes`
```json
[
  {
    "$group": {
      "_id": { "ano": { "$year": "$data_evento" }, "mes": { "$month": "$data_evento" } },
      "pedidos": { "$sum": 1 },
      "receita": { "$sum": "$valor_total" }
    }
  },
  {
    "$project": {
      "_id": 0,
      "ano": "$_id.ano",
      "mes": "$_id.mes",
      "pedidos": 1,
      "receita": 1,
      "ticket_medio": {
        "$cond": [ { "$gt": ["$pedidos", 0] }, { "$divide": ["$receita", "$pedidos"] }, 0 ]
      }
    }
  },
  { "$sort": { "ano": 1, "mes": 1 } },
  {
    "$merge": {
      "into": { "db": "papelaria_gold", "coll": "vendas_kpis_mes" },
      "on": ["ano","mes"],
      "whenMatched": "replace",
      "whenNotMatched": "insert"
    }
  }
]
```

### 10) Vendas — Top SKUs (acumulado) — `papelaria_silver.pedidos_vendas` → `papelaria_gold.vendas_top_skus`
```json
[
  { "$unwind": "$itens" },
  {
    "$group": {
      "_id": "$itens.sku",
      "qtd": { "$sum": "$itens.quantidade" },
      "receita": {
        "$sum": {
          "$multiply": [
            "$itens.quantidade",
            { "$subtract": ["$itens.preco_unitario", { "$ifNull": ["$itens.desconto", 0] }] }
          ]
        }
      }
    }
  },
  { "$project": { "_id": 0, "sku": "$_id", "qtd": 1, "receita": 1 } },
  { "$sort": { "receita": -1 } },
  {
    "$merge": {
      "into": { "db": "papelaria_gold", "coll": "vendas_top_skus" },
      "on": ["sku"],
      "whenMatched": "replace",
      "whenNotMatched": "insert"
    }
  }
]
```

### 11) Vendas — Participação por canal/mês — `papelaria_silver.pedidos_vendas` → `papelaria_gold.vendas_participacao_canal`
```json
[
  {
    "$group": {
      "_id": { "ano": { "$year": "$data_evento" }, "mes": { "$month": "$data_evento" }, "canal": "$canal" },
      "receita": { "$sum": "$valor_total" }
    }
  },
  {
    "$group": {
      "_id": { "ano": "$_id.ano", "mes": "$_id.mes" },
      "total_mes": { "$sum": "$receita" },
      "por_canal": { "$push": { "canal": "$_id.canal", "receita": "$receita" } }
    }
  },
  { "$unwind": "$por_canal" },
  {
    "$project": {
      "_id": 0,
      "ano": "$_id.ano",
      "mes": "$_id.mes",
      "canal": "$por_canal.canal",
      "receita": "$por_canal.receita",
      "participacao": {
        "$cond": [
          { "$gt": ["$total_mes", 0] },
          { "$divide": ["$por_canal.receita", "$total_mes"] },
          0
        ]
      }
    }
  },
  { "$sort": { "ano": 1, "mes": 1, "canal": 1 } },
  {
    "$merge": {
      "into": { "db": "papelaria_gold", "coll": "vendas_participacao_canal" },
      "on": ["ano","mes","canal"],
      "whenMatched": "replace",
      "whenNotMatched": "insert"
    }
  }
]
```

### 12) Estoque — Giro por SKU/mês — `papelaria_silver.estoque_movs` → `papelaria_gold.estoque_giro`
```json
[
  {
    "$group": {
      "_id": { "sku": "$sku", "ano": { "$year": "$data_evento" }, "mes": { "$month": "$data_evento" } },
      "entradas": { "$sum": { "$cond": [ { "$eq": ["$tipo", "entrada"] }, "$quantidade", 0 ] } },
      "saidas": { "$sum": { "$cond": [ { "$eq": ["$tipo", "saida"] }, "$quantidade", 0 ] } }
    }
  },
  {
    "$project": {
      "_id": 0,
      "sku": "$_id.sku",
      "ano": "$_id.ano",
      "mes": "$_id.mes",
      "entradas": 1,
      "saidas": 1,
      "giro": "$saidas"
    }
  },
  { "$sort": { "sku": 1, "ano": 1, "mes": 1 } },
  {
    "$merge": {
      "into": { "db": "papelaria_gold", "coll": "estoque_giro" },
      "on": ["sku","ano","mes"],
      "whenMatched": "replace",
      "whenNotMatched": "insert"
    }
  }
]
```

### 13) Estoque — Risco de ruptura — `papelaria_silver.estoque_movs` → `papelaria_gold.estoque_risco_ruptura`
```json
[
  {
    "$facet": {
      "saldo_total": [
        {
          "$group": {
            "_id": "$sku",
            "entradas": { "$sum": { "$cond": [ { "$eq": ["$tipo", "entrada"] }, "$quantidade", 0 ] } },
            "saidas":   { "$sum": { "$cond": [ { "$eq": ["$tipo", "saida"] }, "$quantidade", 0 ] } }
          }
        },
        { "$project": { "_id": 0, "sku": "$_id", "saldo_atual": { "$subtract": ["$entradas", "$saidas"] } } }
      ],
      "saidas_30d": [
        {
          "$match": {
            "data_evento": { "$gte": { "$dateSubtract": { "startDate": "$$NOW", "unit": "day", "amount": 30 } } },
            "tipo": "saida"
          }
        },
        { "$group": { "_id": "$sku", "saidas_30d": { "$sum": "$quantidade" } } },
        { "$project": { "_id": 0, "sku": "$_id", "saidas_30d": 1 } }
      ]
    }
  },
  {
    "$project": {
      "joined": {
        "$map": {
          "input": "$saldo_total",
          "as": "s",
          "in": {
            "sku": "$$s.sku",
            "saldo_atual": "$$s.saldo_atual",
            "saidas_30d": {
              "$let": {
                "vars": {
                  "m": {
                    "$first": {
                      "$filter": {
                        "input": "$saidas_30d",
                        "as": "x",
                        "cond": { "$eq": ["$$x.sku", "$$s.sku"] }
                      }
                    }
                  }
                },
                "in": { "$ifNull": ["$$m.saidas_30d", 0] }
              }
            }
          }
        }
      }
    }
  },
  { "$unwind": "$joined" },
  {
    "$project": {
      "_id": 0,
      "sku": "$joined.sku",
      "saldo_atual": "$joined.saldo_atual",
      "consumo_medio_30d": { "$divide": ["$joined.saidas_30d", 30] },
      "dias_cobertura": {
        "$cond": [
          { "$gt": ["$joined.saidas_30d", 0] },
          {
            "$divide": [
              "$joined.saldo_atual",
              { "$max": [ { "$divide": ["$joined.saidas_30d", 30] }, 0.0001 ] }
            ]
          },
          null
        ]
      },
      "risco_ruptura": {
        "$and": [
          { "$ne": ["$joined.saidas_30d", 0] },
          { "$lt": [
              {
                "$divide": [
                  "$joined.saldo_atual",
                  { "$max": [ { "$divide": ["$joined.saidas_30d", 30] }, 0.0001 ] }
                ]
              }, 7
          ] }
        ]
      }
    }
  },
  {
    "$merge": {
      "into": { "db": "papelaria_gold", "coll": "estoque_risco_ruptura" },
      "on": ["sku"],
      "whenMatched": "replace",
      "whenNotMatched": "insert"
    }
  }
]
```

### 14) Atendimento — KPIs por mês — `papelaria_silver.atendimento_tickets` → `papelaria_gold.atendimento_kpis_mes`
```json
[
  {
    "$group": {
      "_id": { "ano": { "$year": "$data_evento" }, "mes": { "$month": "$data_evento" } },
      "tickets": { "$sum": 1 },
      "resolvidos": { "$sum": { "$cond": [ { "$eq": ["$status", "resolvido"] }, 1, 0 ] } },
      "media_tpr_min": { "$avg": "$sla_min" },
      "csat_soma": { "$sum": { "$cond": [ { "$ne": ["$csat", null] }, "$csat", 0 ] } },
      "csat_cnt":  { "$sum": { "$cond": [ { "$ne": ["$csat", null] }, 1, 0 ] } }
    }
  },
  {
    "$project": {
      "_id": 0,
      "ano": "$_id.ano",
      "mes": "$_id.mes",
      "tickets": 1,
      "resolvidos": 1,
      "taxa_resolucao": {
        "$cond": [ { "$gt": ["$tickets", 0] }, { "$divide": ["$resolvidos", "$tickets"] }, 0 ]
      },
      "media_tpr_min": 1,
      "csat_medio": {
        "$cond": [ { "$gt": ["$csat_cnt", 0] }, { "$divide": ["$csat_soma", "$csat_cnt"] }, null ]
      }
    }
  },
  { "$sort": { "ano": 1, "mes": 1 } },
  {
    "$merge": {
      "into": { "db": "papelaria_gold", "coll": "atendimento_kpis_mes" },
      "on": ["ano","mes"],
      "whenMatched": "replace",
      "whenNotMatched": "insert"
    }
  }
]
```

### 15) Prospecção — KPIs mês/origem — `papelaria_silver.prospeccao_leads` → `papelaria_gold.prospeccao_kpis_mes_origem`
```json
[
  {
    "$group": {
      "_id": {
        "ano": { "$year": "$data_evento" },
        "mes": { "$month": "$data_evento" },
        "origem": "$origem"
      },
      "leads": { "$sum": 1 },
      "convertidos": { "$sum": { "$cond": [ { "$eq": ["$status", "convertido"] }, 1, 0 ] } }
    }
  },
  {
    "$project": {
      "_id": 0,
      "ano": "$_id.ano",
      "mes": "$_id.mes",
      "origem": "$_id.origem",
      "leads": 1,
      "convertidos": 1,
      "taxa_conversao": {
        "$cond": [ { "$gt": ["$leads", 0] }, { "$divide": ["$convertidos", "$leads"] }, 0 ]
      }
    }
  },
  { "$sort": { "ano": 1, "mes": 1, "origem": 1 } },
  {
    "$merge": {
      "into": { "db": "papelaria_gold", "coll": "prospeccao_kpis_mes_origem" },
      "on": ["ano","mes","origem"],
      "whenMatched": "replace",
      "whenNotMatched": "insert"
    }
  }
]
```

### 16) Chat — Funil Sessão→Carrinho→Compra — `papelaria_silver.chat_msgs` + `carrinhos_estado` → `papelaria_gold.chat_funil`
```json
[
  { "$group": { "_id": "$id_sessao_chat", "mensagens": { "$sum": 1 } } },
  { "$project": { "_id": 0, "id_sessao_chat": "$_id", "mensagens": 1 } },
  {
    "$lookup": {
      "from": "carrinhos_estado",
      "localField": "id_sessao_chat",
      "foreignField": "id_sessao_chat",
      "as": "estado"
    }
  },
  {
    "$addFields": {
      "teve_carrinho": { "$gt": [ { "$size": "$estado" }, 0 ] },
      "status_final": {
        "$cond": [
          { "$gt": [ { "$size": "$estado" }, 0 ] },
          { "$arrayElemAt": [ "$estado.status_final", 0 ] },
          null
        ]
      },
      "converteu": { "$in": [ { "$ifNull": ["$status_final", ""] }, ["pagar", "checkout"] ] }
    }
  },
  { "$project": { "id_sessao_chat": 1, "mensagens": 1, "teve_carrinho": 1, "status_final": 1, "converteu": 1 } },
  {
    "$merge": {
      "into": { "db": "papelaria_gold", "coll": "chat_funil" },
      "on": ["id_sessao_chat"],
      "whenMatched": "replace",
      "whenNotMatched": "insert"
    }
  }
]
```

### 17) Chat — Intenções por mês — `papelaria_silver.chat_msgs` → `papelaria_gold.chat_intencoes_mes`
```json
[
  {
    "$group": {
      "_id": { "ano": { "$year": "$data_evento" }, "mes": { "$month": "$data_evento" }, "intencao": "$intencao" },
      "mensagens": { "$sum": 1 }
    }
  },
  { "$project": { "_id": 0, "ano": "$_id.ano", "mes": "$_id.mes", "intencao": "$_id.intencao", "mensagens": 1 } },
  {
    "$merge": {
      "into": { "db": "papelaria_gold", "coll": "chat_intencoes_mes" },
      "on": ["ano","mes","intencao"],
      "whenMatched": "replace",
      "whenNotMatched": "insert"
    }
  }
]
```

### 18) Recomendações — KPIs mês/origem — `papelaria_silver.recomendacoes` → `papelaria_gold.recomendacoes_kpis_mes_origem`
```json
[
  {
    "$group": {
      "_id": { "ano": { "$year": "$data_evento" }, "mes": { "$month": "$data_evento" }, "origem": "$origem" },
      "total_recs": { "$sum": 1 }
    }
  },
  { "$project": { "_id": 0, "ano": "$_id.ano", "mes": "$_id.mes", "origem": "$_id.origem", "total_recs": 1 } },
  { "$sort": { "ano": 1, "mes": 1, "origem": 1 } },
  {
    "$merge": {
      "into": { "db": "papelaria_gold", "coll": "recomendacoes_kpis_mes_origem" },
      "on": ["ano","mes","origem"],
      "whenMatched": "replace",
      "whenNotMatched": "insert"
    }
  }
]
```

---

### ✅ Ordem sugerida de execução
1. **Bronze → Silver:** 1 → 8 (rodar #2 antes do #3).
2. **Silver → Gold:** 9 → 18.