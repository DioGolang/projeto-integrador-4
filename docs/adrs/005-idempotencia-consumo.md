# ADR-005: Idempotência no Consumo de Eventos

## Status
Aceito

## Contexto

O Transactional Outbox (ADR-002) combinado com brokers como RabbitMQ/SQS garante semântica de entrega **at-least-once** — nunca *exactly-once*. Isso significa que, em cenários de falha (ex: consumidor processa a mensagem mas cai antes de confirmar o ack), o mesmo evento pode ser entregue e processado mais de uma vez pelo mesmo consumidor.

Sem tratamento, isso pode gerar efeitos colaterais duplicados: notificar o mesmo entregador duas vezes para a mesma corrida, ou (em casos mais graves) reprocessar uma transição de estado já aplicada.

## Decisão

Todo consumidor de eventos (`dispatcher`, adaptador SSE, e demais workers) verifica idempotência antes de processar, usando a tabela `processed_events`:

```sql
CREATE TABLE processed_events (
    event_id UUID NOT NULL,
    consumer_name VARCHAR(255) NOT NULL,
    processed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, consumer_name)
);
```

Fluxo do consumidor:
1. Recebe a mensagem, extrai `event_id`.
2. Tenta inserir `(event_id, consumer_name)` em `processed_events`.
3. Se a inserção falhar por violação de chave primária (evento já processado por este consumidor), a mensagem é descartada (ack sem reprocessamento) — é um duplicado seguro de ignorar.
4. Se a inserção for bem-sucedida, o processamento de negócio prossegue normalmente.

Cada consumidor lógico (`dispatcher`, `sse-adapter`, etc.) tem seu próprio `consumer_name`, permitindo que o mesmo evento seja processado uma vez por cada consumidor distinto, mas nunca duas vezes pelo mesmo.

## Consequências

**Positivas:**
- Protege contra efeitos colaterais duplicados (RF07) sem exigir *exactly-once delivery* do broker, que é mais caro/complexo de garantir na infraestrutura.
- É uma solução simples, testável e centrada no próprio Postgres — não depende de recursos específicos do broker (RabbitMQ ou SQS, indiferente).

**Negativas / trade-offs:**
- Adiciona uma escrita extra (e uma tabela) por evento processado — overhead aceitável dado o volume esperado do MVP.
- A tabela `processed_events` cresce indefinidamente; requer uma rotina de limpeza/particionamento por data como trabalho futuro (fora do escopo inicial do MVP).
- Não protege contra duplicação de efeitos colaterais **externos** ao banco (ex: uma notificação push enviada ao entregador antes da checagem de idempotência) — o desenho dos workers deve checar idempotência **antes** de qualquer efeito colateral externo, não depois.
