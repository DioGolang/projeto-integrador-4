# Domínio: Pagamento (Payment)

Cobre o modelo financeiro, a idempotência do webhook de pagamento, e todas as regras de estorno e disputa. Referencia ADR-002 (Transactional Outbox), ADR-005 (idempotência), ADR-007 (checkout hospedado com split).

---

## Modelo Financeiro

- **SaaS B2B:** o lojista paga uma assinatura mensal para uso da plataforma.
- **Pagamento antecipado (pré-despacho):** o cliente final paga via Checkout Hospedado (Pix ou Cartão) antes de o pedido ser despachado a qualquer entregador (ADR-007). Um pedido não pago nunca é visível ao motor de despacho.
- **100% do valor do frete pertence ao entregador**, creditado automaticamente via split de pagamento do PSP — sem reconciliação manual.
- O backend nunca processa dado de cartão diretamente (PCI compliance é responsabilidade do PSP, via checkout hospedado) e nunca intermedeia dinheiro fisicamente através do entregador. O valor do frete é calculado pela plataforma (fórmula em `reputation-pricing.md`), não digitado por ninguém.

## Fluxo de Cobrança

1. Pedido criado → transição automática para `AWAITING_PAYMENT` (ver `order-lifecycle.md`).
2. Backend cria cobrança no PSP com split pré-configurado (fração ao lojista, frete integral ao entregador).
3. Cliente recebe QR Code (pedido de balcão) ou link via WhatsApp Business API / SMS fallback (pedido remoto) — sem exigir instalação de app.
4. PSP notifica `payment.approved` via webhook.

## Idempotência do Webhook de Pagamento

- `INSERT` do identificador único da transação do PSP com `ON CONFLICT DO NOTHING` — múltiplas entregas do mesmo webhook (Pix ou Cartão) nunca geram dupla confirmação.
- `OrderPaid`/`payment.approved` publicado no RabbitMQ via Outbox; a transição `AWAITING_PAYMENT → PAID` ocorre uma única vez, independentemente de reentregas.

## Estorno e Disputa

### 6.1 Cancelamento Antes do Despacho (Estorno Automático)
- **Regra:** se o pedido é cancelado pelo lojista em `AWAITING_PAYMENT` (raro) ou `PAID`/antes de `DISPATCHED`, o backend aciona a API de estorno do PSP automaticamente de forma integral.
- Novo evento de domínio: `RefundIssued`, publicado via Outbox, consumido pelo mesmo adaptador SSE.

### 6.2 Compensação de Deslocamento (Late Cancellation)
- **O Risco:** Entregador aceita (`ACCEPTED`), gasta tempo/gasolina até a loja (`AT_STORE`), e o lojista cancela (ex: pedido caiu no chão). Se o estorno for integral, o motoboy arca com o prejuízo da viagem.
- **A Regra:** Cancelamentos originados pelo lojista após o entregador estar no estado `ACCEPTED` ou `AT_STORE` geram uma **Taxa de Deslocamento** (ex: 30% do valor do frete). Esse valor é retido do estorno feito ao lojista/cliente e creditado imediatamente na carteira do entregador, protegendo a frota.

### 6.3 Corrida de Eventos no Timeout de Pagamento
- Webhook `payment.approved` recebido para um pedido já em `PAYMENT_EXPIRED` **não é descartado**: aciona estorno automático imediato (o cliente não perde o valor por uma corrida de eventos do sistema) e notifica o lojista do motivo.

### Nenhum Entregador Disponível (Fan-out esgotado)
- Após o número máximo de ciclos de redespacho (mecânica em `dispatch-fleet.md`), o pedido vai para `CANCELLED_NO_DELIVERER` com estorno automático total. Lojista notificado com motivo explícito, distinto de cancelamento por decisão própria.

### Ajuste por Indisponibilidade Parcial
- Remoção de item de um pedido já `PAID` (regra completa em `order-lifecycle.md`) aciona estorno parcial automático via PSP pela diferença de valor.

### Disputa Pós-Entrega
- Janela de **24 horas** após `COMPLETED` para o cliente abrir contestação (link no mesmo canal do pagamento — WhatsApp/SMS), reportando item errado, faltante ou não entregue.
- Move o pedido para `UNDER_DISPUTE` (estado terminal de auditoria, não reversível para estados operacionais anteriores) e gera tarefa na fila de revisão manual (mesma estrutura operacional de `SUSPECTED_THEFT`, ver `security-zero-trust.md`).
- Arbitragem inicial cabe ao lojista (controla o conteúdo do pedido); casos envolvendo possível falha do entregador escalam para revisão da plataforma, usando como evidência o histórico de geofencing/PIN (ver `security-zero-trust.md`).
