# Checklist  — Arquitetura Kafka + MongoDB (Papelaria)

Este documento apresenta o checklist completo das três camadas da arquitetura (**Bronze, Silver e Gold**),
detalhando coleções, transformações, ingestão (append/overwrite) e perguntas de negócio que cada KPI responde.

---

## 1) Camada Bronze
**Características:**  
- Ingestão: **append-only** (sempre adiciona, nunca altera).  
- Função: manter dados crus vindos do Kafka, inclusive inválidos.  
- Garantia: preserva o histórico original de cada tópico.

### Coleções Bronze

- `pedidos_vendas_raw`: pedidos completos (id_pedido, cliente, canal, itens, valor_total, data_evento)  
- `mov_estoque_raw`: movimentações de estoque (id_produto, movimento, quantidade, motivo, data_evento)  
- `tickets_atend_raw`: atendimentos (id_ticket, cliente, canal, status, SLA, CSAT, data_evento)  
- `conversas_chat_raw`: mensagens de chat (id_evento, id_sessao_chat, remetente, mensagem, intenção, entidades, data_evento)  
- `recomendacoes_raw`: recomendações feitas (id_evento, id_sessao_chat, origem, itens sugeridos, data_evento)  
- `carrinhos_raw`: eventos de carrinho (id_evento, id_carrinho, ação, itens, valor_total, data_evento)  
- `leads_prosp_raw`: leads/prospecções (id_lead, origem, status, data_evento)  

---

## 2) Camada Silver
**Características:**  
- Ingestão: **upsert/overwrite por chave natural** (id_evento, id_pedido, etc.).  
- Função: curar dados, garantir idempotência, padronizar tipos (datas ISODate, números double/int).  

### Coleções Silver

- `chat_msgs`: mensagens limpas do chat, intencao default "outro"  
- `carrinhos_eventos`: histórico de eventos do carrinho  
- `carrinhos_estado`: último status e valor_total do carrinho  
- `pedidos_vendas`: pedidos normalizados (itens, valores, data_evento)  
- `estoque_movs`: movimentações normalizadas (sku, tipo, quantidade, motivo, data_evento)  
- `atendimento_tickets`: tickets de atendimento normalizados (status, SLA, CSAT, data_evento)  
- `prospeccao_leads`: leads de prospecção normalizados (origem, status, data_evento)  
- `recomendacoes`: recomendações normalizadas (origem, lista, data_evento)  

---

## 3) Camada Gold
**Características:**  
- Ingestão: **overwrite por chave de agregação** (ano/mes, sku, origem…).  
- Função: responder perguntas de negócio (KPIs), já agregados e prontos para dashboards.  

### Coleções Gold (KPIs)

- `vendas_kpis_mes`: receita, pedidos e ticket médio por mês  
- `vendas_top_skus`: top SKUs vendidos (quantidade e receita)  
- `vendas_participacao_canal`: participação de receita por canal/mês  
- `estoque_giro`: entradas, saídas e giro mensal por SKU  
- `estoque_risco_ruptura`: saldo atual, consumo médio, dias de cobertura, risco  
- `atendimento_kpis_mes`: tickets, resolvidos, taxa de resolução, TPR, CSAT por mês  
- `prospeccao_kpis_mes_origem`: leads e taxa de conversão por origem/mês  
- `chat_funil`: Sessão → Carrinho → Compra (funil de conversão)  
- `chat_intencoes_mes`: contagem de intenções por mês  
- `recomendacoes_kpis_mes_origem`: recomendações por mês/origem  

---

# 📊 Visão Comparativa — Domínios

| Domínio        | Bronze (raw)              | Silver (curado)                     | Gold (KPI)                                      | O que responde? |
|----------------|---------------------------|-------------------------------------|-------------------------------------------------|-----------------|
| **Vendas**     | `pedidos_vendas_raw`      | `pedidos_vendas`                    | `vendas_kpis_mes`, `vendas_top_skus`, `vendas_participacao_canal` | Receita por mês, ticket médio, top SKUs, participação por canal |
| **Estoque**    | `mov_estoque_raw`         | `estoque_movs`                      | `estoque_giro`, `estoque_risco_ruptura`         | Giro mensal, risco de ruptura |
| **Atendimento**| `tickets_atend_raw`       | `atendimento_tickets`               | `atendimento_kpis_mes`                          | TMR, TPR, CSAT, taxa de resolução |
| **Chat**       | `conversas_chat_raw`      | `chat_msgs`                         | `chat_funil`, `chat_intencoes_mes`              | Intenções mais comuns, funil Sessão→Compra |
| **Carrinho**   | `carrinhos_raw`           | `carrinhos_eventos`, `carrinhos_estado` | (usado no funil)                              | Evolução do carrinho, estado final, conversão |
| **Prospecção** | `leads_prosp_raw`         | `prospeccao_leads`                  | `prospeccao_kpis_mes_origem`                    | Origem de leads, taxa de conversão |
| **Recomendações** | `recomendacoes_raw`    | `recomendacoes`                     | `recomendacoes_kpis_mes_origem`                 | Quantidade e origem das recomendações |

---
