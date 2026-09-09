# Análise Crítica e Operações de Produção (Day-2 Operations)

Este documento apresenta a análise crítica da arquitetura do Motor Logístico, validando as escolhas dos Architecture Decision Records (ADRs) e mapeando os desafios operacionais (Day-2) para a fase de implementação em Go. O desenho escapa das armadilhas clássicas de microsserviços (como o *dual-write problem*) e adota soluções pragmáticas que equilibram rigor com viabilidade de entrega para o MVP.

---

## 1. Consistência e Mensageria (ADR-001, ADR-002, ADR-005)

A trindade estrutural do sistema distribuído está perfeitamente alinhada com as melhores práticas de Event-Driven Architecture (EDA).

- **Transactional Outbox (ADR-002):** É a escolha definitiva para garantir que o pedido salvo no Postgres seja invariavelmente publicado no broker. A decisão de gravar a entidade e o evento na mesma transação SQL garante atomicidade.
- **Idempotência Obrigatória (ADR-005):** Como o Outbox garante *at-least-once delivery*, o uso da tabela `processed_events` cruzando `event_id` e `consumer_name` evita que o mesmo consumidor processe o evento duas vezes.

> [!WARNING]
> **Desafio Técnico (Advogado do Diabo):**
> O *polling* periódico na tabela `outbox_events` (ADR-002) pode gerar *table scans* pesados se não houver um índice parcial explícito (ex: `CREATE INDEX ON outbox_events (created_at) WHERE status = 'PENDING'`). Além disso, a tabela `processed_events` (ADR-005) crescerá infinitamente. Para o futuro, deve-se planejar um cronjob de expurgo de eventos processados há mais de 7 dias, mantendo o banco leve.

## 2. Resolução de Concorrência e Estado (ADR-003, ADR-006)

A simplificação das lógicas de trava evita gargalos no banco de dados.

- **Atomic State Update (ADR-003):** Utilizar `UPDATE ... WHERE status = 'DISPATCHED'` é a abordagem mais idiomática e performática para resolver a corrida do FCFS. Ao validar o `RowsAffected() == 1`, a aplicação delega o isolamento para o motor transacional do Postgres sem manter conexões abertas com `SELECT FOR UPDATE`.
- **Polling para Timeouts (ADR-006):** Delegar a expiração de pedidos atrasados para uma Goroutine que executa uma query atômica no banco simplifica absurdamente a infraestrutura, eliminando a necessidade de Delayed Message Exchanges no RabbitMQ.

> [!WARNING]
> **Desafio Técnico (Advogado do Diabo):**
> Para a query de timeout do ADR-006 não derrubar a performance do Postgres a cada minuto, será obrigatoriamente necessário um índice composto cobrindo `(status, created_at)` na tabela de pedidos.

## 3. Escalabilidade e Separação de Dados (Redis)

- **Segregação Espacial:** A decisão de mover a ingestão de coordenadas (*write-heavy*) para o Redis utilizando `GEOADD` e `GEOSEARCH` protege o PostgreSQL de um volume esmagador de atualizações efêmeras de localização.

> [!WARNING]
> **Desafio Técnico (Advogado do Diabo):**
> O Redis não possui rollback automático em caso de falha no meio de uma requisição. É vital configurar um tempo de expiração (`EXPIRE`) agressivo para as chaves de localização, garantindo que entregadores que desligaram a internet subitamente não continuem aparecendo na busca do Fan-out como se estivessem ativos.

## 4. Conexões Longas e Segurança (ADR-004, JWT)

- **Server-Sent Events (ADR-004):** Adotar SSE para o painel do lojista é uma escolha elegante que evita o peso bidirecional e os heartbeats pesados do WebSocket.
- **Stateless JWT:** Injetar as Claims do token diretamente no `context.Context` no middleware em Go blinda as rotas contra ataques onde o entregador tenta adulterar IDs no payload.

> [!WARNING]
> **Desafio Técnico (Advogado do Diabo):**
> Em Go, cada requisição HTTP rodando um SSE prenderá uma Goroutine dedicada enquanto o cliente estiver conectado. Será preciso ajustar os timeouts do Reverse Proxy (NGINX/Traefik) ou Load Balancer; caso contrário, a infraestrutura cortará as conexões inativas silenciosamente, deixando o lojista com uma tela congelada.

## 5. Resiliência e Falhas (ADR-009)

- **Categorização de Erros:** A distinção entre erros transientes (que sofrem *Exponential Backoff*) e **Poison Pills** (que vão direto para a Dead Letter Queue) demonstra visão operacional de produção. Isso evita o *head-of-line blocking*, onde uma única mensagem corrompida trava o processamento de todo o sistema.

## 6. Contratos, Connect RPC e Streaming (ADR-010)

- **Comunicação Nativa:** A escolha do Connect RPC elimina a necessidade de proxies reversos complexos como o Envoy, operando de forma nativa sobre a biblioteca padrão `net/http` do Go.
- **Server Streaming vs SSE:** O uso de Server Streaming (`rpc StreamOrderStatus`) substitui com elegância o SSE (Server-Sent Events) desenhado anteriormente, gerenciando a comunicação em tempo real de forma fortemente tipada.
- **Aviso de Segurança (Interceptors):** A decisão de remover os IDs de usuário do corpo (payload) das requisições e passá-los via Headers (`Authorization: Bearer`) para serem lidos pelo interceptor do Go consolida o modelo Zero Trust.

> [!WARNING]
> **Desafio Técnico (Advogado do Diabo - Falha de Conexão Longa):**
> Conexões de streaming baseadas em HTTP sofrem cortes automáticos por Load Balancers (como NGINX ou AWS ALB) se não houver tráfego após um tempo ocioso (ex: 60 segundos). O handler em Go precisará implementar "Pings" (keep-alives) regulares injetados no stream, caso contrário, o painel do lojista ficará com a tela travada silenciosamente sem saber que a conexão caiu.

## 7. Arquitetura Go e Monolito Modular (ADR-011, DDD)

- A organização do código em pacotes de domínio autônomos (`/internal/order`, `/internal/fleet`) e o respeito à regra *accept interfaces, return structs* refletem o estado da arte do Go idiomático. A utilização do padrão de Injeção de Dependência via pacotes raiz isola totalmente a infraestrutura (`/postgres`, `/redis`) da lógica de negócios.

> [!TIP]
> **Análise de Internals (Escape Analysis e GC):**
> Ao desenhar as entidades e os DTOs transitórios recebidos do Connect RPC, avalie com cuidado a alocação de memória. Estruturas de dados pequenas e de vida curta devem ser passadas por valor (cópias na Stack). O excesso de ponteiros forçará os objetos a escaparem para a Heap (Escape Analysis), aumentando a frequência e a latência de pausas do Garbage Collector (GC) durante picos de entrega.

## 8. Infraestrutura State of the Art (ADR-012)

- Migrar para o RabbitMQ 4.x.x e utilizar Quorum Queues com suporte nativo a delayed retries atende perfeitamente à necessidade de resiliência e retentativas das filas. A adoção do Khepri (baseado no algoritmo Raft para consenso) garante consistência robusta de estado no cluster.

> [!CAUTION]
> **Cenário de Catastrophic Failure (Gargalo de I/O):**
> As Quorum Queues do RabbitMQ exigem gravação síncrona em disco (Write-Ahead Logging - WAL) em múltiplos nós do cluster antes de confirmar o recebimento (ACK) ao publicador. Se os discos tiverem IOPS baixos, o RabbitMQ aplicará *backpressure* fatal na API Go, causando contenção e esgotamento das Goroutines responsáveis pelo Transactional Outbox Relay. É imperativo utilizar discos SSD/NVMe de alta performance.

## 9. Postgres em Alta Escala (Degradação por Volume)

O Modelo Entidade-Relacionamento com vínculo 1:1 rigoroso com a Foreign Key `UNIQUE` entre `ORDER` e `DELIVERY` blinda o sistema contra atribuições múltiplas, e a presença da `urban_canyon_flag` assegura a auditoria de Graceful Degradation no geofencing.

> [!WARNING]
> **Desafio Técnico (Advogado do Diabo - Partitioning):**
> As tabelas `outbox_events` e `processed_events` são padrões do tipo *append-only* que crescem em progressão geométrica. Em semanas, essas tabelas terão milhões de linhas. Sem uma política agressiva de retenção, isso destruirá a performance de polling do Outbox. Deve-se planejar a implementação de particionamento nativo do PostgreSQL (`PARTITION BY RANGE` por data) ou um cronjob de hard delete implacável, mantendo o *shared buffers* do Postgres focado apenas nos pedidos recentes.

## 10. Design de Código em Go (Internals e Produção)

Para garantir que a base de código reflita os princípios de simplicidade e observabilidade de uma arquitetura Cloud-Native, o *scaffolding* em Go obedecerá aos seguintes dogmas de infraestrutura e runtime:

- **Graceful Shutdown e Controle de Contexto:** Um contêiner pode ser derrubado via `SIGKILL` por falta de RAM ou esteira de CI/CD, quebrando streams e perdendo mensagens (mesmo com Outbox). A implementação do `main.go` **deve** atrelar o ciclo de vida da aplicação a um `ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)`. O Go deve parar de aceitar novos requests HTTP, terminar as mensagens RabbitMQ atuais, enviar o ACK e desligar com `os.Exit(0)`.
- **Abstração de Banco de Dados (sqlc + pgx):** ORMs mágicos baseados em *Reflection* (como GORM) jogam alocações transitórias para a Heap, punindo o Garbage Collector em horários de pico. Utilizaremos **SQL puro** otimizado com o driver `pgx/v5` gerenciado pelo **sqlc**, que gera structs e interfaces estaticamente tipadas antes da compilação.
- **Gerenciamento de Schema (Migrations):** O Go não fará "auto-sync" de banco. Utilizaremos ferramentas nativas de CLI (`golang-migrate` ou `goose`) para gerenciar as versões do esquema em arquivos `.sql` (`up`/`down`), acionados estritamente via esteira de Deploy e CI/CD.
- **Error Handling Moderno (Domínio para RPC):** A camada de transporte não deve conhecer banco de dados. Erros de domínio serão definidos como *Sentinel Errors* (`var ErrDelivererNotFound = errors.New(...)`). Na borda do RPC, utilizaremos `errors.Is`/`errors.As` nativos do Go 1.13+ para mapear para códigos do Connect (ex: `connect.CodeInternal`). Erros originais devem sempre ser envelopados via `fmt.Errorf("...: %w", err)` para preservar o stack trace no logger, sem vazar detalhes de infra para o frontend.
- **Functional Options Pattern (Extensibilidade):** Evitaremos construtores gigantes ou variáveis `nil` (um forte *code smell*). A injeção de dependências múltiplas (Logger, Tracer, Broker) utilizará o padrão de parâmetros variádicos funcionais (ex: `NewServer(db, WithLogger(logger), WithTracer(tracer))`), garantindo flexibilidade para instanciar versões enxutas nos testes unitários e complexas em produção.
- **Worker Pools e Prefetch (Evitando Thundering Herd):** A leitura de filas no RabbitMQ não instanciará uma *Goroutine* por mensagem solta (o que esgotaria as conexões do banco). Utilizaremos *Prefetch Count* (`QoS`) no RabbitMQ alinhado com um *Worker Pool* estrito no Go (ex: 50 *Goroutines* lendo de um *channel*). Isso cria um teto rígido de processamento, protegendo o banco contra picos absurdos de tráfego.
- **Blindagem do Connection Pool (TCP Exhaustion):** Bancos de dados em nuvem sofrem cortes silenciosos de conexões TCP inativas por parte de Firewalls. O `main.go` **deve** configurar explicitamente a tríade de estabilidade do pacote `database/sql`: `SetMaxOpenConns` (limite máximo para não estrangular o DB), `SetMaxIdleConns` (igual ao limite máximo para evitar abre/fecha constante) e `SetConnMaxLifetime` (matando a conexão proativamente antes que o firewall a derrube).
- **Inversão de Dependência (Interfaces Minúsculas):** Em Go, as interfaces pertencem a quem usa (Consumer), não a quem implementa. Em vez de interfaces gigantes (`OrderRepository`), os casos de uso declararão interfaces minúsculas estritamente para o que precisam (ex: `type OutboxWriter interface { SaveEvent(...) }`). A mesma *struct* concreta do PostgreSQL satisfará as interfaces menores, preservando o Princípio de Segregação de Interface (ISP) e facilitando testes unitários sem *Mock Hell*.

## 11. Resiliência de Interface (Regra do Duplo Clique)

Para blindar o sistema contra falhas de rede no lado do lojista (ex: apertar o botão "Criar Pedido" 3 vezes porque a tela travou), a API exigirá o cabeçalho `Idempotency-Key`.
- O cliente Next.js gera um UUID (v4) único no momento da renderização da tela de checkout e o envia no *Header* (`Idempotency-Key: <uuid>`).
- O backend interceptará essa chave e a utilizará como trava (`ON CONFLICT DO NOTHING` no Redis ou Postgres) durante a ingestão (RF01).
- Se a inserção falhar por chave duplicada, o backend devolve o mesmo pedido já gerado com HTTP 200 OK, eliminando a chance de cobranças múltiplas no cartão do cliente.

---
*Este documento atua como o manual de restrições ("Guardrails") para a fase de codificação e infraestrutura do projeto em Golang.*
