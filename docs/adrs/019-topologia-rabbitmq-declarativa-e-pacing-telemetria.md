# ADR 019: Topologia Declarativa do Broker, Ciclo de Vida dos Eventos e Pacing da Telemetria

## Status
Aprovado / Em Implementação Controlada

## Contexto
Durante o Gate 1.2 da Fase 6, a criação de 1.000 pedidos foi concluída com sucesso (100% de checks HTTP válidos e latência p95 de 5,73 ms). No entanto, a ativação imediata de 1.000 entregadores pelo simulador disparou 1.000 goroutines que emitiam requisições de telemetria GPS a cada 1 segundo (`TELEMETRY_INTERVAL_MS=1000`).

Esse fluxo concorrente evidenciou três gargalos arquiteturais:
1. **Contenção no Pool de Conexões SQL (`DB_MAX_OPEN_CONNS = 50`):** As 50 conexões configuradas foram disputadas simultaneamente por requisições HTTP da API (`UpdateLocation`), pelo worker do Outbox (`SELECT ... FOR UPDATE SKIP LOCKED`) e pelo consumidor da projeção (`UPDATE delivery_read_model`). Isso gerou 137.195 eventos de espera com tempo médio de retenção de 21,66 ms.
2. **Buffer na Fila AMQP (`api.delivery_projection`):** O consumidor serial da projeção opera com `prefetch = 1` e canal único, processando ~250 a 330 msgs/s devido à transação de escrita no PostgreSQL. Como o simulador gerava 1.000 msgs/s, a fila acumulou um buffer de até 114k mensagens antes de drenar completamente.
3. **Persistência de Políticas RabbitMQ e Hostname Dinâmico:** A ausência de um `hostname` fixo no `docker-compose.yml` fazia com que reinicializações do daemon Docker gerassem diretórios Mnesia baseados no ID efêmero do container (`rabbit@<container_id>`), desvinculando políticas dinâmicas aplicadas via CLI (`rabbitmqctl set_policy DLX`).

## Decisões de Arquitetura

### 1. Semântica Estrita dos Eventos e Separação de Fases
Fica formalmente estabelecida a distinção entre os quatro estágios do ciclo de vida dos eventos:
* **Fase 1 — Persistência Transacional de Comando (Negócio):** A RPC grava a entidade de domínio e o evento correspondente em `outbox_events` com status `PENDING` na mesma transação atômica do PostgreSQL.
* **Fase 2 — Publicação no Broker (Worker):** O Outbox Worker seleciona lotes com `FOR UPDATE SKIP LOCKED`, publica os eventos na exchange `logistics.events` e, após confirmação de entrega do canal AMQP, atualiza o status para `PROCESSED`.
  * **Definição Normativa:** O status `PROCESSED` no Outbox denota **estritamente** que o evento foi publicado com sucesso no broker AMQP. Não indica que a mensagem foi entregue a um consumidor, nem que a projeção no banco foi atualizada.
* **Fase 3 — Consumo e Projeção (Consumer):** O consumidor recebe a mensagem da fila `api.delivery_projection`, valida a ordenação causal através do `SequenceValidator`, abre transação no PostgreSQL, atualiza `delivery_read_model` (protegido por `WHERE sequence < new_seq`) e atualiza o checkpoint global em `projection_checkpoints`.
* **Fase 4 — Confirmação do Broker (ACK):** O comando `d.Ack(false)` é enviado ao RabbitMQ **estritamente após** o commit com sucesso da transação SQL da projeção. Se a transação falhar, o ACK não é enviado (gera NACK com requeue para erros transitórios ou rejeição para DLQ em erros irrecuperáveis).

### 2. Semântica de Checkpoint Global vs. Sequência Causal por Agregado
* **`projection_checkpoints.last_sequence`:** Armazena o `stream_position` monótono global do Outbox. Garante recuperação determinística após reinicialização do consumidor.
* **`delivery_read_model.sequence`:** Armazena a versão causal interna do agregado individual (`Seq 1` = Criação, `Seq 2` = Atribuição de Rota, `Seq 3..N` = Posições da Trajetória). Garante que mensagens entregues fora de ordem global não regridam o estado do agregado.

### 3. Pacing da Telemetria no Simulador
Para viabilizar testes de saturação focados no ciclo de pedidos sem sobrecarregar artificialmente o pool com coordenadas intermediárias, o simulador passa a suportar modos explícitos de execução:
* **Padrão Imutável do Domínio:** `ENABLE_TELEMETRY=true`, `TELEMETRY_INTERVAL_MS=1000`, `SIM_SPEED_FACTOR=1.0`. A simulação física em tempo real permanece como comportamento padrão do sistema.
* **Perfil "GPS Desativado":** Configurado via `ENABLE_TELEMETRY=false`. O entregador aceita a corrida, calcula a rota, aguarda o tempo de trânsito físico estimado e conclui a corrida, **sem emitir chamadas intermediárias de `UpdateLocation`**.
* **Perfil "Telemetria com Intervalo Controlado":** Configurado via `TELEMETRY_INTERVAL_MS=5000` ou `SIM_SPEED_FACTOR=5.0`. Permite desacelerar a taxa de emissão de telemetria sem alterar as regras de transição nem a distância física.

### 4. Preservação de Dados do RabbitMQ e Topologia Declarativa
* O volume persistente `projeto-integrador-4_rabbitmqdata` deve ser preservado.
* Nenhuma fila existente deve ser excluída para contornar incompatibilidades de argumentos.
* A configuração da Dead Letter Exchange (`logistics.events.dlx`) e Dead Letter Queue (`api.delivery_projection.dlq`) deve ser gerida via **RabbitMQ Policies** (ou provisionamento declarativo via `definitions.json`), preservando argumentos existentes da fila.
* A migração para um nó de hostname estável (`rabbit@rabbitmq`) só poderá ser efetivada após validação de backup e procedimento de migração/leitura dos diretórios Mnesia históricos.

## Consequências

### Positivas
* Elimina a confusão entre mensagens publicadas no Outbox e mensagens projetadas no modelo de leitura.
* Permite isolar os limites de capacidade da API de pedidos (`CreateOrder` / `AcceptDelivery`) separando-os da carga de telemetria de fundo.
* Evita loops de NACK infinito preservando mensagens venenosas na DLQ para auditoria.

### Negativas / Mitigações
* A validação de integridade requer consultas em duas tabelas (`outbox_events` e `delivery_read_model`), já que o modelo de leitura mutável armazena apenas o estado final da entidade.
* O consumidor serial com `QoS=1` acumula mensagens na fila durante rajadas com GPS ativo em 1s, demandando tempo de drenagem proporcional ao tamanho do lote gerado.
