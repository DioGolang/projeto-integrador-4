# ADR-013: Observabilidade e Rastreamento Distribuído (OpenTelemetry)

## Status

Aceito

## Contexto

O RNF02 exige a rastreabilidade ponta a ponta dos pedidos. Em nossa arquitetura orientada a eventos, uma requisição nasce na API (Go), é gravada no banco (PostgreSQL), capturada pelo *Outbox Relay*, enviada ao *Broker* (RabbitMQ) e finalmente processada por um *Worker* (Dispatcher). Sem um mecanismo padronizado de propagação de contexto, debugar uma falha silenciosa em produção (ex: por que a corrida X não tocou no celular do entregador Y?) seria impossível.

## Decisão

Adotaremos o **OpenTelemetry (OTel)** como padrão oficial para instrumentação de Tracing, Metrics e Logs no backend em Go.

1. **Propagação de Contexto (W3C Trace Context):** O identificador único da requisição (`Trace-ID`) será extraído e injetado nativamente no `context.Context` do Go.
2. **Fronteiras de Rede:**
   - Ao publicar no RabbitMQ, o *Outbox Relay* extrairá o `Trace-ID` do evento no banco e o injetará nos *Headers* da mensagem AMQP.
   - O *Worker* consumidor fará o caminho inverso: lerá o *Header* do RabbitMQ e criará um novo `context.Context` filho, mantendo a linhagem do rastro.
3. **Coleta e Visualização (A Trindade da Observabilidade):** Utilizaremos o *OTel Collector* rodando no `docker-compose` para receber os dados gerados pelo Go e exportá-los para a stack do **Grafana**:
   - **Traces:** Exportados para o Grafana Tempo (rastreabilidade distribuída ponto a ponto).
   - **Métricas:** Exportadas para o Prometheus (uso de CPU, número de goroutines em uso, tamanho da fila do RabbitMQ).
   - **Logs:** Os logs estruturados (`slog` no Go) serão correlacionados com o `Trace-ID` e exportados via OTel para o Grafana Loki, permitindo cruzar falhas nos rastros diretamente com as linhas de log.

## Consequências

**Positivas:**
- Visibilidade absoluta: conseguiremos visualizar em um gráfico de Gantt o tempo exato gasto na query do banco, no enfileiramento e na execução matemática do PostGIS.
- Padronização de mercado (CNCF), evitando *vendor lock-in* com ferramentas proprietárias como Datadog ou New Relic (podemos trocar o destino exportando apenas uma variável de ambiente).

**Negativas / trade-offs:**
- Leve degradação de performance (*overhead* de instrumentação). Em produção, será necessário configurar o *Sampling* (ex: gravar apenas 10% dos rastros em cenários de sucesso, mas 100% em cenários de erro).
- Maior verbosidade no código Go, pois toda chamada de função, banco ou rede exigirá a passagem explícita do `ctx context.Context`.
