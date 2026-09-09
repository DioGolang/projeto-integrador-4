# ADR-009: Tratamento de Falhas e Dead Letter Queues (DLQ)

## Status
Aceito

## Contexto
O requisito **RNF04** (Tratamento de Erros e Retries) estipula a necessidade de uma arquitetura resiliente. No cenário assíncrono (mensageria), quando o serviço consumidor (`dispatcher` ou `sse-adapter`) falha ao processar uma mensagem (ex: erro de banco de dados temporário, indisponibilidade de rede ou falha de parsing), não podemos simplesmente descartar a mensagem, nem processá-la infinitamente travando a fila (*head-of-line blocking*).

## Decisão
Adotamos a seguinte política de tratamento de falhas baseada em **Exponential Backoff e Dead Letter Queues (DLQ)**:

1. **Classificação do Erro:**
   - **Erros Transientes (Recuperáveis):** Ex: timeout de banco, deadlock, API externa fora do ar. A mensagem é devolvida (NACK) para sofrer *retry*.
   - **Poison Pills (Fatais/Não Recuperáveis):** Ex: erro de validação de payload (JSON malformado), ID não existente. A mensagem é enviada **imediatamente** para a DLQ, pois nenhum número de retentativas fará o código funcionar sem um *hotfix*.

2. **Mecânica de Retry (Exponential Backoff):**
   - Para erros transientes, o consumidor fará NACK e a mensagem será roteada para uma fila de *Retry* com um TTL (Time-To-Live). Após o TTL expirar (ex: 5s, 15s, 60s), ela retorna para a fila principal.
   - O número máximo de retentativas é definido como **3 vezes**.

3. **Fluxo de DLQ (Dead Letter Queue):**
   - Após esgotadas as 3 tentativas, ou em caso de *Poison Pill*, a mensagem é movida para uma fila de DLQ (`order_events_dlq`).
   - Mensagens na DLQ ficam armazenadas passivamente. **A DLQ não tem consumidor automático.**
   - O painel de observabilidade (Grafana/Prometheus) emitirá um alerta caso o tamanho da DLQ seja `> 0`.

4. **Reprocessamento (Shovel):**
   - A equipe técnica analisará a mensagem na DLQ, fará o *hotfix* no código (se for bug) ou normalizará a infraestrutura.
   - Um script administrativo (ou o plugin *Shovel* do RabbitMQ) será usado para mover as mensagens da DLQ de volta para a fila original para reprocessamento manual.

## Consequências

**Positivas:**
- **Zero Perda de Dados:** Nenhuma requisição se perde; o pior cenário é um atraso até intervenção humana.
- **Fila Principal Livre:** Erros sistemáticos não causam engarrafamento (*head-of-line blocking*), pois as mensagens problemáticas saem do caminho rapidamente.
- **Isolamento de Bugs:** Permite que desenvolvedores depurem eventos exatos que causaram pânico ou erro.

**Negativas / trade-offs:**
- Requer instrumentação adicional na infraestrutura (configuração das regras de *x-dead-letter-exchange* e TTL no RabbitMQ/SQS).
- Exige monitoramento ativo. Uma DLQ sem alerta é apenas um "cemitério silencioso de dados" onde o lojista fica sem resposta e ninguém percebe.
