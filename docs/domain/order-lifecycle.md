# Domínio: Ciclo de Vida do Pedido (Order Lifecycle)

Cobre a máquina de estados do pedido, os requisitos funcionais RF01–RF05, a resolução de concorrência no aceite e as regras de pedidos com múltiplos itens. Referencia ADR-001 (comunicação assíncrona), ADR-003 (concorrência), ADR-005 (idempotência), ADR-007 (pagamento).

---

## Máquina de Estados

```text
CREATED → AWAITING_PAYMENT → PAID → DISPATCHED → ACCEPTED → AT_STORE → IN_TRANSIT → COMPLETED
                  ↓ (timeout)                                              ↓ (disputa em até 24h)
          PAYMENT_EXPIRED / CANCELLED                                 UNDER_DISPUTE

DISPATCHED → (Fan-out esgotado) → CANCELLED_NO_DELIVERER
```

- `CREATED → AWAITING_PAYMENT`: automático, ao gerar a cobrança no PSP (ADR-007, ver `payment.md`).
- `AWAITING_PAYMENT → PAID`: via webhook `payment.approved`, idempotente. Detalhe em `payment.md`.
- `PAID → DISPATCHED`: automático, publicação de `OrderReadyForDispatch` via Outbox (ADR-002).
- `AWAITING_PAYMENT` sem confirmação em 15 minutos → `PAYMENT_EXPIRED` (estorno tratado em `payment.md`).
- `DISPATCHED` sem aceite após esgotar as tentativas de Fan-out → `CANCELLED_NO_DELIVERER` (mecânica de redespacho em `dispatch-fleet.md`; estorno em `payment.md`).
- `COMPLETED` com contestação em até 24h → `UNDER_DISPUTE` (detalhe em `payment.md`).

### Produção Vinculada ao Aceite (Just-in-Time Prep)
- **O Risco:** Se o restaurante iniciar a produção imediatamente após `PAID`, um eventual esgotamento de Fan-out (`CANCELLED_NO_DELIVERER`) gerará estorno integral ao cliente e desperdício físico para o lojista.
- **A Regra:** O sistema instrui ativamente o lojista (via UI) a **não preparar** o pedido durante a fase `DISPATCHED` ("Procurando Entregador..."). O preparo do pedido só deve ser iniciado quando a transição para `ACCEPTED` ocorrer ("Entregador João a caminho"). Isso garante que não haverá perda de insumos caso a malha logística não consiga absorver a demanda.

## RF01 — Ingestão de Pedidos (revisado: múltiplos itens)

- O lojista registra uma solicitação de entrega informando: dados do cliente (incluindo telefone, obrigatório para o checkout — ver `payment.md`), endereço de destino, e uma **lista de itens** (`nome`, `quantidade`, `valor_unitário`) — não mais um valor único digitado.
- O valor do frete **não é digitado pelo lojista**: é calculado pela plataforma (fórmula em `reputation-pricing.md`) e retornado como parte da resposta de criação do pedido, para confirmação — nunca edição livre.
- **Validações na criação:** distância loja→cliente dentro do raio máximo permitido, e origem dentro da área de cobertura ativa (regras completas em `dispatch-fleet.md`) — pedidos fora desses limites são rejeitados na criação, não silenciosamente perdidos depois.

### Indisponibilidade Parcial Pós-Pagamento
- Se o lojista, com o pedido já `PAID`, precisa remover um item (ex: esgotado), o ajuste permitido é **apenas de remoção**, nunca adição — adicionar item pós-pagamento abriria brecha para cobrar valor além do consentido pelo cliente no checkout.
- Remoção de item aciona estorno parcial automático via PSP pela diferença de valor (mecânica em `payment.md`); o pedido segue seu fluxo normal com o valor ajustado.
- Se a remoção zerar o pedido inteiro, ele é cancelado e segue a regra de estorno total.

## RF02 — Roteamento e Notificação (Fan-Out)

- Ao entrar em `DISPATCHED`, o pedido aciona a busca de entregadores candidatos e o Fan-out. Mecânica completa (raio de busca, FSM do entregador, redespacho, Dead Miles) em `dispatch-fleet.md`.

## RF03 — Aceite Concorrente

- **Modelo:** FCFS (First-Come, First-Served). Concorrência resolvida via **Atomic State Update** (`UPDATE ... WHERE status = 'DISPATCHED'`), alinhado ao ADR-003 — sem coluna `version`, sem lógica de retry adicional.
- O primeiro `UPDATE` bem-sucedido move o pedido para `ACCEPTED`. Os demais recebem `409 Conflict` com mensagem amigável ("Outro entregador já pegou esta corrida").
- Priorização por score de reputação (delay artificial para entregadores com score baixo) é regra de `dispatch-fleet.md`/`reputation-pricing.md`, não altera o mecanismo de concorrência aqui descrito.

### Idempotência do Aceite (RNF, alinhado ao ADR-005)
- `/orders/{id}/accept`: mesmo entregador reenviando o request (retry por instabilidade de rede/3G) → `200 OK`, sem erro. Outro entregador já aceitou → `409 Conflict` (conflito real, não confundir com retry).
- `/orders/{id}/complete`: se já `COMPLETED`, retorna `200 OK` sem duplicar efeitos colaterais (eventos de conclusão, split de pagamento).

## RF04 — Gestão de Estado

- Status exposto em tempo real ao lojista via SSE (ADR-004). Todas as transições descritas na máquina de estados acima são refletidas nesse canal.

## RF05 — Deadlines e Expiração

- Pedido `DISPATCHED` sem aceite dentro do tempo limite (ex: 10 minutos): cancela a busca atual e reinicia um novo ciclo de Fan-out (mecânica completa em `dispatch-fleet.md`).
- Após esgotar o número máximo de ciclos de redespacho, transiciona para `CANCELLED_NO_DELIVERER`, com estorno automático tratado em `payment.md`.
