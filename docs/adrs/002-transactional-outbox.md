# ADR-002: Consistência Distribuída via Transactional Outbox

## Status
Aceito

## Contexto

Ao criar um pedido (RF01), o sistema precisa (a) persistir o `Order` no PostgreSQL e (b) publicar o evento `OrderPlaced` no broker de mensagens (RabbitMQ/SQS) para acionar o despacho (ADR-001). Essas são duas operações contra dois sistemas diferentes.

Se implementadas como duas escritas independentes ("dual write"), existe uma janela de falha real:

- O `Order` é salvo no Postgres, mas a aplicação cai/a rede falha antes de publicar no broker → o pedido nunca é despachado, e o lojista não é avisado (viola RF02 e RNF03).
- O evento é publicado no broker, mas a transação do Postgres falha e sofre rollback → o `dispatcher` tenta processar um pedido que não existe no banco.

## Decisão

Adotamos o padrão **Transactional Outbox**:

1. Toda escrita que gera um evento de domínio grava, na **mesma transação SQL**, tanto a entidade (`orders`) quanto o evento na tabela `outbox_events` (mesmo banco, mesma transação — atomicidade garantida pelo próprio Postgres).
2. Um processo separado, o `outbox-relay` (`cmd/outbox-relay`), faz polling periódico (ou usa CDC/logical replication, como evolução futura) na tabela `outbox_events` com `status = 'PENDING'`, publica cada evento no broker e marca como `PUBLISHED` somente após confirmação de entrega (ack do broker).
3. Os use cases da camada de aplicação (`CreateOrderUseCase`, `AcceptDeliveryUseCase`, `ExpireOrderUseCase`) nunca publicam diretamente no broker — eles dependem apenas da interface `OutboxWriter`, que grava no Postgres.

## Consequências

**Positivas:**
- Elimina a janela de inconsistência do dual-write: se a transação falha, nem o `Order` nem o evento existem; se ela é confirmada, os dois existem juntos, garantido.
- O domínio permanece desacoplado de infraestrutura de mensageria (reforça o DIP descrito no ADR-006).
- O `outbox-relay` pode reprocessar falhas de publicação (retry) sem re-executar a lógica de negócio.

**Negativas / trade-offs:**
- Introduz *at-least-once delivery*: o relay pode publicar o mesmo evento mais de uma vez em caso de falha entre publicar e marcar `PUBLISHED` — por isso o ADR-005 (idempotência no consumo) é obrigatório, não opcional.
- Latência adicional: o evento só é publicado no próximo ciclo de polling do relay, não instantaneamente (aceitável dado RNF03 — consistência eventual já é uma premissa do sistema).
- Mais uma tabela e um processo a manter/monitorar (mitigado por ser um `cmd/` simples e stateless).
