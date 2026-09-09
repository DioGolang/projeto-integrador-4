# ADR-001: Padrão de Comunicação para o Despacho de Pedidos

## Status
Aceito

## Contexto

O motor de despacho precisa notificar múltiplos entregadores ativos em uma região assim que um pedido é criado (RF02). Duas abordagens foram consideradas:

1. **REST síncrono**: a API do lojista, ao receber o pedido, chamaria diretamente um serviço de despacho, que por sua vez chamaria (via HTTP) o serviço/app de cada entregador candidato.
2. **Mensageria assíncrona (Fan-out)**: a API do lojista publica um evento `OrderPlaced`; um serviço consumidor (`dispatcher`) reage a esse evento, resolve os candidatos e distribui a notificação via um exchange fan-out (RabbitMQ) ou tópico (SNS/SQS).

O cenário de negócio exige resiliência a picos (ex: horário de almoço, chuva) e isolamento de falhas entre o cadastro do pedido e a lógica de roteamento (RNF01).

## Decisão

Adotamos **mensageria assíncrona** com um exchange do tipo *fan-out* (RabbitMQ) ou tópico SNS com fila SQS por consumidor, para o fluxo de despacho.

- A API do lojista (`cmd/api`) apenas grava o `Order` e publica `OrderPlaced` via Transactional Outbox (ver ADR-002). Ela não conhece o `dispatcher`.
- O serviço `dispatcher` (`cmd/dispatcher`) consome `OrderPlaced`, resolve os entregadores candidatos (RF06 — raio de geolocalização) e publica notificações individuais para os apps dos entregadores.
- Comunicação síncrona (REST) é usada apenas para operações que exigem resposta imediata ao usuário: cadastro de pedido (RF01), consulta de status (RF04, complementado por SSE), e o endpoint de aceite de corrida (RF03), que grava no banco de forma síncrona mas publica o resultado de forma assíncrona.

## Consequências

**Positivas:**
- Se o `dispatcher` cair, o lojista continua registrando pedidos normalmente (RNF01) — eles ficam bufferizados na fila até o serviço voltar.
- Picos de demanda são absorvidos pela fila em vez de sobrecarregar o serviço de roteamento diretamente (RNF03).
- Fan-out desacopla "quantos entregadores existem" de "quantas chamadas HTTP a API do lojista precisa fazer".

**Negativas / trade-offs:**
- Consistência eventual: o lojista não sabe instantaneamente se algum entregador foi notificado — precisa de RF04/RNF06 (SSE) para refletir isso.
- Complexidade operacional adicional: broker de mensagens é mais uma peça de infraestrutura a operar e monitorar (mitigado por Docker Compose em dev e DLQ/retry em produção — RNF04).
- Depuração mais difícil que uma chamada síncrona — mitigado pelo Correlation-ID e tracing distribuído (RNF02).
