# ADR-006: Mecânica de Expiração de Pedidos (Timeouts)

## Status
Proposto

## Contexto
O requisito **RF05** determina que se um pedido despachado não for aceito por nenhum entregador dentro de um tempo limite (ex: 10 minutos), o sistema deve cancelar a busca e notificar o lojista.

Precisamos de um mecanismo que mude o status do pedido de `DISPATCHED` para `CANCELLED` (ou `EXPIRED`) garantindo precisão aceitável, sem sobrecarregar a infraestrutura. Avaliamos duas opções principais:

1. **Delayed Message (Mensageria via broker com TTL):** Ao criar o pedido, publica-se um evento `OrderTimeoutScheduled` em uma fila específica (RabbitMQ com DLX + TTL, ou SQS Delay Queue) configurada para segurar a mensagem por 10 minutos. Quando o tempo expira, o consumidor pega a mensagem, verifica o banco; se o status ainda for `DISPATCHED`, altera para `CANCELLED`.
2. **Polling / Cronjob (Banco de Dados):** Um worker em Go (Goroutine com `time.Ticker` ou lib de cron) roda a cada 1 minuto executando uma query atômica no banco: `UPDATE orders SET status = 'CANCELLED' WHERE status = 'DISPATCHED' AND created_at < NOW() - INTERVAL '10 minutes'`.

## Decisão
Adotamos o **Polling / Cronjob no Banco de Dados** (Opção 2) para o MVP.

Um novo processo (ou goroutine dedicada) executará periodicamente a verificação e atualização em lote. Após a atualização, a rotina publica o evento `OrderExpiredEvent` via o mesmo padrão de Transactional Outbox (ADR-002) para que o frontend seja notificado via SSE (ADR-004).

## Consequências

**Positivas:**
- **Simplicidade Operacional:** Não dependemos de plugins específicos do RabbitMQ (Delayed Message Exchange) nem limitamos a portabilidade para outros brokers, mantendo o banco como a fonte da verdade do tempo.
- **Eficiência sob Sucesso:** Na abordagem de filas com TTL, 100% dos pedidos geram uma mensagem "fantasma" que precisará ser consumida e ignorada 10 minutos depois, mesmo que o pedido tenha sido aceito no primeiro minuto. No polling, o banco de dados só atualiza o que de fato expirou.
- **Recuperação a Falhas:** Se o sistema inteiro cair por 20 minutos, ao voltar, o cronjob varre o passado de forma retroativa e expira todos os pedidos atrasados de uma vez. O broker exigiria acumular mensagens mortas.

**Negativas / trade-offs:**
- **Falta de precisão de milissegundos:** A expiração não é exata em 10m:00s. Dependendo do intervalo do polling (ex: 1 em 1 minuto), a expiração pode ocorrer aos 10m:59s. Dado o cenário de logística local, essa variação de segundos é perfeitamente tolerável pelo negócio.
- Overhead de leitura recorrente no banco: mitigado por um índice cobrindo `(status, created_at)` para tornar a query ultrarrápida.
