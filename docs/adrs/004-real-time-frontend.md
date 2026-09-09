# ADR-004: Padrão de Atualização em Tempo Real para o Frontend

## Status
Substituído pelo ADR-010 (Server Streaming via Connect RPC).

## Contexto

RF04 exige que o status do pedido (`CREATED`, `DISPATCHED`, `ACCEPTED`, `IN_TRANSIT`, `COMPLETED`, `CANCELLED`) seja exposto em tempo real para o lojista no painel Next.js. Três abordagens foram avaliadas:

1. **Polling**: o frontend consulta `GET /orders/:id` periodicamente.
2. **WebSocket**: canal bidirecional persistente entre frontend e backend.
3. **Server-Sent Events (SSE)**: canal unidirecional (servidor → cliente) sobre HTTP simples, nativo do browser via `EventSource`.

O fluxo de dados aqui é unidirecional — o lojista apenas observa mudanças de status; ele não precisa enviar dados em tempo real de volta pelo mesmo canal (ações como aceitar/cancelar continuam sendo requests HTTP normais).

## Decisão

Adotamos **Server-Sent Events (SSE)** para a atualização de status em tempo real no painel do lojista.

- O backend Go expõe um endpoint (`GET /orders/stream`) que mantém a conexão HTTP aberta e envia eventos conforme o status dos pedidos do lojista muda (alimentado pelos mesmos domain events publicados via Outbox/broker, consumidos por um adaptador que os retransmite via SSE).
- No Next.js, um hook (`useOrderStream`) encapsula o `EventSource` nativo do browser e atualiza o cache local (React Query/SWR) quando um evento chega, evitando a necessidade de um estado global (Redux) só para isso.

Descartamos WebSocket por adicionar complexidade de infraestrutura (protocolo próprio, necessidade de heartbeat/reconexão manual) sem necessidade real, já que o fluxo é unidirecional. Descartamos Polling puro por gerar carga desnecessária no backend e uma percepção de latência maior para o lojista, especialmente durante os picos que o RNF03 já assume como cenário esperado.

## Consequências

**Positivas:**
- SSE roda sobre HTTP/1.1 padrão, reconecta automaticamente via `EventSource` sem código extra, e é suficiente para o caso de uso unidirecional.
- Menor superfície de infraestrutura que WebSocket (sem necessidade de sticky sessions especiais além do que HTTP keep-alive já exige).
- Integra-se naturalmente ao modelo de eventos de domínio já existente (Outbox → broker → adaptador SSE).

**Negativas / trade-offs:**
- Não serve para comunicação bidirecional — se no futuro o frontend precisar enviar dados em tempo real pelo mesmo canal (ex: chat com o entregador), será necessário reavaliar para WebSocket.
- Proxies/load balancers precisam ser configurados para não interromper conexões HTTP de longa duração (timeout de idle connection).
- Cada conexão SSE aberta consome uma goroutine/conexão no backend — escalabilidade horizontal deve ser considerada se o número de lojistas simultâneos crescer muito (fora do escopo do MVP).
