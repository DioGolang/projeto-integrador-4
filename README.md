<div align="center">
  <img src="docs/logo/logo.png" alt="Motor Logístico Logo" width="300" />
</div>

# Motor Logístico: Despacho Assíncrono para o Comércio Local

![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8?style=for-the-badge&logo=go)
![Next.js](https://img.shields.io/badge/Next.js-16+-000000?style=for-the-badge&logo=next.js)
![React](https://img.shields.io/badge/React-19-61DAFB?style=for-the-badge&logo=react)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18_(PostGIS)-336791?style=for-the-badge&logo=postgresql)
![Redis](https://img.shields.io/badge/Redis-8-DC382D?style=for-the-badge&logo=redis)
![RabbitMQ](https://img.shields.io/badge/RabbitMQ-4.3.5_(Management)-FF6600?style=for-the-badge&logo=rabbitmq)
![Protobuf](https://img.shields.io/badge/Protobuf-Connect_RPC-blue?style=for-the-badge)
![OpenTelemetry](https://img.shields.io/badge/OpenTelemetry-Tracing_%26_Metrics-F5A800?style=for-the-badge&logo=opentelemetry)
![Grafana](https://img.shields.io/badge/Grafana-13.2.1-F46800?style=for-the-badge&logo=grafana)

---

## 📑 Sumário

- [Visão Geral](#-visão-geral)
- [Glossário para Iniciantes](#-glossário-para-iniciantes)
- [Arquitetura e Estrutura do Monorepo](#-arquitetura-e-estrutura-do-monorepo)
- [Fluxo de Dados e Ciclo de Vida dos Eventos](#-fluxo-de-dados-e-ciclo-de-vida-dos-eventos)
- [Stack Tecnológica](#-stack-tecnológica)
- [Pré-requisitos e Instalação de Ferramentas](#-pré-requisitos-e-instalação-de-ferramentas)
- [Guia Passo a Passo: Do Zero ao Primeiro Pedido](#-guia-passo-a-passo-do-zero-ao-primeiro-pedido)
- [Catálogo Completo de Comandos (Makefile & CLI)](#-catálogo-completo-de-comandos-makefile--cli)
- [Portas, Painéis e Acessos Rápidos](#-portas-painéis-e-acessos-rápidos)
- [Fluxo de Trabalho Git da Equipe](#-fluxo-de-trabalho-git-da-equipe)
- [Resolução de Problemas Frequentes (Troubleshooting / FAQ)](#-resolução-de-problemas-frequentes-troubleshooting--faq)
- [Documentações Complementares](#-documentações-complementares)

---

## 📖 Visão Geral

O **Motor Logístico** é uma plataforma inteligente e descentralizada de gestão logística, despacho assíncrono e rastreamento em tempo real. Foi concebido para conectar diretamente pequenos comércios locais (como pizzarias, padarias e farmácias) a entregadores parceiros da própria região, eliminando taxas abusivas de intermediários centralizados.

### 💡 Para quem está começando: Como o sistema funciona na prática?
1. **O Lojista cria um pedido** no painel web (informando endereço de entrega e itens).
2. **O sistema gera um PIN de segurança de 4 dígitos** exclusivo para a entrega.
3. **O Motor de Despacho (Fan-out)** localiza entregadores disponíveis em um raio geográfico e notifica-os de forma assíncrona.
4. **O Entregador aceita a corrida** e se desloca até o estabelecimento para retirar o pacote.
5. **Durante o trajeto**, a posição do entregador é transmitida em tempo real via streaming para o mapa do lojista.
6. **Ao chegar ao destino**, a entrega só pode ser finalizada se o entregador estiver fisicamente perto do cliente (**Geofencing** de até 50 metros) e digitar o **PIN correto**, prevenindo fraudes.

---

## 📚 Glossário para Iniciantes

Se você é iniciante na equipe ou está tendo o primeiro contato com microsserviços e mensageria, estes conceitos são fundamentais:

| Termo | O que significa de forma simples? | Onde usamos no projeto? |
|---|---|---|
| **Monorepo** | Um único repositório Git que guarda todo o código: backend, frontend, simulador e infraestrutura. Facilita o versionamento integrado. | Na raiz do projeto (`api/`, `web/`, `simulator/`, etc.). |
| **Docker & Docker Compose** | Ferramenta que cria "mini computadores isolados" (contêineres) para rodar o banco de dados, filas e serviços sem sujar o seu sistema operacional. | Arquivo `docker-compose.yml`. |
| **Connect RPC & Protobuf** | Forma moderna e super rápida de comunicação entre backend e frontend usando contratos estritos tipados (`.proto`), substituindo REST tradicional. | Pasta `proto/` e camada de rede da API. |
| **PostgreSQL + PostGIS** | Banco de dados relacional equipado com uma extensão especial (`PostGIS`) para cálculos de coordenadas geográficas (latitude/longitude e distâncias). | Banco principal (`localhost:5432`). |
| **RabbitMQ & Mensageria** | Um correio inteligente de mensagens (broker). Quando um pedido é criado, ele avisa outros serviços sem travar a requisição do usuário. | Fila de eventos (`localhost:5672`). |
| **Transactional Outbox** | Padrão arquitetural que salva o pedido e a mensagem na mesma transação no banco, garantindo que nenhuma mensagem seja perdida se o servidor cair. | Tabela `outbox_events` e worker Go. |
| **OSRM (Open Source Routing Machine)** | Motor de cálculo de rotas reais de trânsito em malha viária, calculando caminhos percorridos por motos. | Utilizado pelo simulador de entregas. |
| **Migrations** | Arquivos SQL numerados (`000001_...up.sql`) que constroem as tabelas do banco de forma versionada e controlada. | Pasta `migrations/`. |
| **Seed** | Script que popula o banco com dados iniciais fictícios (lojista de teste, entregador e admin) para podermos testar sem cadastrar tudo na mão. | Comando `make seed`. |

---

## 🏛️ Arquitetura e Estrutura do Monorepo

O repositório é organizado de maneira modular:

```text
projeto-integrador-6/
├── api/          # Backend Go (Connect RPC, Domain Services, Transactional Outbox, OTel, LiveHub)
│   ├── cmd/      # Pontos de entrada executáveis (server, seed, loadtest, operational_test)
│   ├── internal/ # Código de negócio, regras de domínio, banco de dados e mensageria
│   └── gen/      # Código Go gerado automaticamente a partir dos arquivos .proto
├── web/          # Frontend Next.js 16 / React 19 (Portal do Lojista, Mapa MapLibre GL, Autenticação)
│   ├── src/      # Páginas, componentes de UI, hooks e clientes RPC
│   └── public/   # Ativos estáticos e web workers do mapa
├── simulator/    # Motor autônomo em Go que simula motos se movendo e enviando telemetria
├── loadtest/     # Scripts de testes de estresse k6 e monitoramento de saturação
├── proto/        # Contratos de comunicação entre serviços (Protocol Buffers)
├── migrations/   # Scripts SQL versionados do banco de dados (000001 a 000008)
├── infra/        # Configurações de observabilidade (Prometheus, OTel, Tempo, Loki, Grafana, RabbitMQ)
├── docs/         # Decisões de Arquitetura (ADRs 001 a 019), diagramas C4 e runbooks
├── Makefile      # Atalhos para todos os comandos úteis do dia a dia
└── docker-compose.yml # Definição dos contêineres de infraestrutura
```

---

## 🔄 Fluxo de Dados e Ciclo de Vida dos Eventos

### 1. Sequência Transacional (Outbox & Fan-out)
O pedido é gravado no banco de forma atômica e publicado no RabbitMQ com confirmação diferida:

```mermaid
sequenceDiagram
    autonumber
    participant Merchant as Lojista / Cliente
    participant API as 🚢 API Service (Go)
    participant DB as 🐘 PostgreSQL 18
    participant Relay as ⚙️ Outbox Relay
    participant RMQ as 🐇 RabbitMQ Broker
    participant Consumer as 📥 Projection Consumer
    participant Hub as ⚡ LiveHub (SSE/Stream)
    participant Web as 🖥️ Web Tracker (Next.js)

    Merchant->>API: Connect RPC: CreateOrder (HMAC PIN)
    activate API
    API->>DB: TX: INSERT order + INSERT outbox_events (PENDING)
    API-->>Merchant: Order ID + PIN de Entrega
    deactivate API

    loop Polling com SKIP LOCKED
        Relay->>DB: SELECT PENDING outbox_events FOR UPDATE SKIP LOCKED
        Relay->>RMQ: PublishWithDeferredConfirm (logistics.events)
        RMQ-->>Relay: Publisher Confirm (ACK)
        Relay->>DB: UPDATE outbox_events SET status = 'PROCESSED'
    end

    RMQ->>Consumer: Delivery (QoS=1, api.delivery_projection)
    activate Consumer
    Consumer->>Consumer: Valida Sequence Causal & Deduplica
    Consumer->>DB: TX: INSERT processed_events + UPDATE delivery_read_model + UPDATE checkpoint
    DB-->>Consumer: Commit OK
    Consumer->>RMQ: Basic.Ack
    Consumer->>Hub: Publish(Event)
    Hub-->>Web: StreamOrderStatus (Server-Streaming RPC)
    deactivate Consumer
```

### 2. Máquina de Estados do Pedido
Cada pedido segue uma ordem rigorosa de transições:

```mermaid
stateDiagram-v2
    direction LR
    [*] --> CREATED: Criação do Pedido
    CREATED --> DISPATCHED: Despacho para a Rede
    CREATED --> CANCELLED: Cancelamento pelo Lojista
    
    DISPATCHED --> ACCEPTED: Entregador Aceita Corrida
    DISPATCHED --> CANCELLED: Cancelamento

    ACCEPTED --> IN_TRANSIT: Entregador Retira o Pedido
    IN_TRANSIT --> DELIVERED: Conclusão (PIN correto + Geofence < 50m)
    IN_TRANSIT --> FROZEN: Tentativas Inválidas de PIN / Fora do Raio
    FROZEN --> ACCEPTED: Descongelamento Manual (Admin)
    
    DELIVERED --> [*]
    CANCELLED --> [*]
```

---

## 🛠️ Stack Tecnológica

| Componente | Tecnologia | Versão | Função Principal |
|---|---|---|---|
| **Backend API** | Go | `1.22+` | Monolito modular, endpoints RPC, workers de Outbox e regras de negócio |
| **Frontend Web** | Next.js / React | `16 / 19` | Painel do lojista, acompanhamento com MapLibre GL e stream de pedidos |
| **Simulador** | Go / OSRM | `1.22+` | Simula trajetos reais de entregadores e telemetria GPS |
| **Banco Relacional** | PostgreSQL + PostGIS | `18` | Persistência transacional e operações geoespaciais |
| **Cache & Geo** | Redis | `8 Alpine` | Cache de estado e buscas espaciais ultrarrápidas |
| **Broker de Eventos** | RabbitMQ | `4.3.5` | Mensageria, filas Quorum e Dead Letter Exchanges |
| **Métricas** | Prometheus | `v3.13.2` | Coleta métricas de latência, erro e throughput |
| **Traces** | Grafana Tempo | `2.4.0` | Rastreamento distribuído de requisições de ponta a ponta |
| **Logs** | Grafana Loki | `3.7.7` | Centralização e consulta de logs estruturados |
| **Dashboards** | Grafana | `13.2.1` | Visualização unificada de métricas, traces e logs |

---

## 💻 Pré-requisitos e Instalação de Ferramentas

Antes de iniciar, certifique-se de ter as ferramentas instaladas no seu computador:

### 1. Docker e Docker Compose (Obrigatório)
- **Windows / Mac:** Baixe e instale o [Docker Desktop](https://www.docker.com/products/docker-desktop/). No Windows, utilize o backend WSL2.
- **Linux (Ubuntu/Debian):**
  ```bash
  sudo apt update && sudo apt install -y docker.io docker-compose-plugin
  sudo usermod -aG docker $USER
  # Faça logout e login novamente para ativar as permissões
  ```

### 2. Go (Golang 1.22+) (Obrigatório para Backend e Simulador)
- Baixe no site oficial: [go.dev/dl](https://go.dev/dl/)
- Confirme a instalação no terminal:
  ```bash
  go version
  ```

### 3. Node.js 20+ e npm (Obrigatório para o Frontend)
- Baixe no site oficial: [nodejs.org](https://nodejs.org/) (versão LTS recomendada).
- Confirme a instalação:
  ```bash
  node -v
  npm -v
  ```

### 4. Make (Gerenciador de Tarefas)
- **Linux (Ubuntu/Debian):** `sudo apt install make`
- **Linux (Fedora):** `sudo dnf install make`
- **macOS:** `xcode-select --install` ou `brew install make`
- **Windows:** Utilize dentro do terminal Ubuntu (WSL2), ou instale via Chocolatey (`choco install make`) ou Winget (`winget install GnuWin32.Make`).

### 5. Ferramentas Auxiliares (Opcionais, necessárias apenas para migrações e contratos)
- **golang-migrate** (para rodar migrações fora do Docker):
  ```bash
  go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest
  ```
- **Buf CLI** (para compilar arquivos `.proto`):
  ```bash
  go install github.com/bufbuild/buf/cmd/buf@latest
  ```

---

## 🚀 Guia Passo a Passo: Do Zero ao Primeiro Pedido

Siga este tutorial passo a passo para rodar o projeto do zero:

### Passo 1: Clonar o Repositório e Entrar na Pasta
Abra o seu terminal e execute:
```bash
git clone https://github.com/DioGolang/projeto-integrador-6.git
cd projeto-integrador-6
```

---

### Passo 2: Configurar o Arquivo de Variáveis de Ambiente (`.env`)
O arquivo `.env` guarda senhas, portas e segredos de criptografia. Ele nunca deve ser comitado no Git.
Crie o seu `.env` copiando o modelo de exemplo:

```bash
cp .env.example .env
```

> ⚠️ **Atenção para Iniciantes:** O arquivo `.env.example` já vem configurado com valores padrão perfeitos para o ambiente de desenvolvimento local. Em produção, altere as variáveis `JWT_SECRET`, `PIN_HMAC_SECRET` e `GRAFANA_ADMIN_PASSWORD` para chaves aleatórias seguras.

---

### Passo 3: Subir a Infraestrutura (Banco de Dados, Filas e Métricas)
Execute o comando abaixo para iniciar todos os contêineres em segundo plano:

```bash
make up
```
*(Caso não tenha o comando `make` instalado, execute: `docker compose up -d`)*

Para checar se todos os contêineres subiram com sucesso e estão saudáveis (`healthy`):
```bash
make ps
```
*(Você verá contêineres como `logistics-postgres`, `logistics-rabbitmq`, `logistics-redis`, `logistics-prometheus`, etc., com status `Up`).*

---

### Passo 4: Aplicar as Migrações do Banco de Dados
As migrações criam as tabelas e extensões geográficas (`PostGIS`) no PostgreSQL:

```bash
make migrate-up
```

> 💡 **Não tem a ferramenta `migrate` instalada na máquina?** Sem problemas! Execute este comando Docker alternativo que roda tudo em um contêiner descartável:
> ```bash
> docker run --rm -v $(pwd)/migrations:/migrations --network host migrate/migrate -path=/migrations/ -database "postgres://root:password@localhost:5432/logistics?sslmode=disable" up
> ```

---

### Passo 5: Criar os Usuários e Perfis Iniciais (Seed)
Para você não precisar cadastrar lojas e entregadores manualmente, execute o script de semente (seed):

```bash
make seed
```
*(Caso não use Make: `go run ./api/cmd/seed`)*

Esse comando criará automaticamente três perfis com senhas protegidas com bcrypt:

| Papel (Role) | E-mail de Login | Senha Padrão | O que ele faz? |
|---|---|---|---|
| **Lojista (Merchant)** | `loja@elo.com` | `Merchant@123!` | Dono do estabelecimento ("Pizzaria Elo"). Cria e despacha pedidos. |
| **Entregador (Deliverer)** | `entregador@elo.com` | `Entregador@123!` | Entregador parceiro ("Carlos Entregador"). Aceita e conclui corridas. |
| **Administrador (Admin)** | `admin@elo.com` | `Admin@123!` | Gestão da plataforma e desbloqueio de pedidos congelados. |

---

### Passo 6: Executar a Aplicação

Você tem duas formas de rodar a aplicação:

#### 🔹 Opção A: Execução Local para Desenvolvimento (Recomendada)
Esta opção permite alterar o código com recarregamento rápido (Hot Reload) e ver logs em tempo real. Abra **três abas no seu terminal**:

1. **Terminal 1 - Iniciar a API Backend (Go):**
   ```bash
   make run-api
   ```
   *(Ou: `cd api && go run cmd/server/main.go`)*  
   *A API iniciará na porta `8080`.*

2. **Terminal 2 - Iniciar o Portal Web do Lojista (Next.js):**
   ```bash
   cd web
   npm install   # (necessário apenas na primeira vez)
   npm run dev
   ```
   *O portal estará disponível em: [http://localhost:3001](http://localhost:3001)*

3. **Terminal 3 - Iniciar o Simulador de Entregas (Go):**
   ```bash
   make run-simulator
   ```
   *(Ou: `cd simulator && go run main.go`)*  
   *O simulador faz login como `entregador@elo.com`, escuta novas corridas, aceita entregas automaticamente e transmite a posição da moto segundo a malha viária real (OSRM).*

---

#### 🔹 Opção B: Subir Tudo Conteinerizado via Docker Compose
Se preferir rodar todos os serviços empacotados em contêineres sem rodar comandos locais:
```bash
docker compose up -d --build
```
*(Para parar: `docker compose down`)*

---

### Passo 7: Testando o Sistema no Navegador! 🎉
1. Abra o navegador e acesse: **[http://localhost:3001](http://localhost:3001)**
2. Faça login com o lojista:
   - **E-mail:** `loja@elo.com`
   - **Senha:** `Merchant@123!`
3. Clique em **Criar Pedido**:
   - Defina o endereço de entrega do cliente.
   - O sistema exibirá o **PIN de 4 dígitos** gerado para o cliente.
4. Clique em **Despachar Pedido**:
   - O Motor de Despacho disparará o evento no RabbitMQ.
   - O Simulador (`simulator`) interceptará a notificação, aceitará a corrida e iniciará a rota.
   - Você verá a moto se deslocando no mapa em tempo real através do streaming RPC!

---

## 🧪 Catálogo Completo de Comandos (Makefile & CLI)

O projeto possui um catálogo abrangente de comandos centralizados no `Makefile`. Você pode rodar `make help` para ver uma listagem colorida a qualquer momento.

### 🚀 1. Gerenciamento da Infraestrutura Docker

| Comando | O que ele faz? | Comando CLI Direto Equivalente |
|---|---|---|
| `make up` | Inicia todos os serviços de infraestrutura em segundo plano | `docker compose up -d` |
| `make down` | Para todos os contêineres da infraestrutura | `docker compose down` |
| `make down-v` | Para contêineres e **apaga todos os dados persistentes** (banco, filas, volumes) | `docker compose down -v` |
| `make restart` | Reinicia todos os contêineres ativos | `docker compose restart` |
| `make ps` | Mostra o status e a saúde de todos os contêineres | `docker compose ps` |
| `make logs` | Acompanha os logs unificados de todos os serviços no terminal | `docker compose logs -f` |
| `make logs-api` | Exibe os logs em tempo real apenas da API | `docker compose logs -f api` |
| `make logs-simulator`| Exibe os logs em tempo real apenas do Simulador | `docker compose logs -f simulator` |
| `make logs-db` | Exibe os logs do PostgreSQL | `docker compose logs -f postgres` |
| `make logs-rabbitmq` | Exibe os logs do broker RabbitMQ | `docker compose logs -f rabbitmq` |
| `make db-shell` | Abre o terminal interativo SQL (`psql`) dentro do PostgreSQL | `docker compose exec postgres psql -U root -d logistics` |
| `make redis-cli` | Abre a interface de linha de comando (`redis-cli`) no Redis | `docker compose exec redis redis-cli` |

---

### 🗄️ 2. Banco de Dados e Migrações

| Comando | O que ele faz? | Observações |
|---|---|---|
| `make migrate-up` | Executa todas as migrações SQL pendentes na pasta `migrations/` | Constrói tabelas, índices e funções geoespaciais |
| `make migrate-down` | Desfaz a última migração aplicada (rollback de 1 versão) | Útil quando uma migração recente precisa ser ajustada |
| `make migrate-force V=X`| Destrava o banco quando ocorre erro de versão suja (*dirty state*) | Exemplo: `make migrate-force V=8` |
| `make migrate-create NAME=x`| Cria um novo par de arquivos `.up.sql` e `.down.sql` versionados | Exemplo: `make migrate-create NAME=add_phone_to_users` |
| `make seed` | Executa o seeder de dados iniciais (lojistas, entregadores, credenciais) | Seguro para rodar múltiplas vezes (idempotente) |

---

### 📦 3. Compilação e Execução dos Serviços

| Comando | O que ele faz? |
|---|---|
| `make run-api` | Inicia o servidor Go da API em modo desenvolvimento (`:8080`) |
| `make run-simulator` | Inicia o simulador autônomo de corridas localmente |
| `make run-web` | Inicia o servidor de desenvolvimento do Next.js (`:3001`) |
| `make build` | Compila todos os binários Go em `bin/` e valida o build de produção do frontend |
| `make build-api` | Compila o binário otimizado da API em `bin/api` |
| `make build-simulator`| Compila o binário do simulador em `bin/simulator` |
| `make build-web` | Executa `npm run build` no frontend Next.js |

---

### 🧪 4. Testes e Qualidade de Software

| Comando | O que ele faz? |
|---|---|
| `make test` | Executa todos os testes automatizados do workspace (API e Simulador) |
| `make test-unit` | Executa apenas os testes unitários rápidos (`-short`) |
| `make test-api` | Executa todos os testes unitários e de integração do módulo API |
| `make test-simulator`| Executa todos os testes do módulo do Simulador |
| `make test-integration`| Executa testes com a tag `integration` (requer RabbitMQ e OSRM ativos) |
| `make test-e2e` | Executa teste ponta a ponta (E2E) simulando requisições reais |
| `make test-coverage` | Executa os testes e imprime um relatório de cobertura por função no terminal |
| `make test-coverage-html`| Gera um relatório visual de cobertura de código em HTML (`coverage.html`) |
| `make test-web` | Executa a verificação de tipagem TypeScript e lint no frontend |
| `make test-op` | Executa o teste operacional completo (Simulador -> API -> Stream) |
| `make loadtest` | Executa o teste de estresse e saturação OpenTelemetry na API |

---

### 🛠️ 5. Qualidade de Código, Linting e Limpeza

| Comando | O que ele faz? |
|---|---|
| `make fmt` | Formata automaticamente todos os arquivos Go de acordo com as convenções oficiais |
| `make lint` | Executa os linters de código do backend (`golangci-lint`) e do frontend (`eslint`) |
| `make tidy` | Limpa e sincroniza as dependências dos arquivos `go.mod` e `go.work` |
| `make clean` | Remove binários gerados em `bin/`, relatórios de cobertura e limpa caches |

---

### ⚡ 6. Geração de Código (Protobuf & SQL)

| Comando | O que ele faz? |
|---|---|
| `make gen` | Executa todos os geradores de código do projeto (`proto-gen` e `sqlc-gen`) |
| `make proto-gen` | Compila as definições `.proto` usando o Buf, gerando código Go e TypeScript |
| `make buf-lint` | Executa checagem de estilo e boas práticas nos arquivos `.proto` |
| `make sqlc-gen` | Gera código Go fortemente tipado a partir das queries SQL em `api/internal/db` |

---

## 🔐 Portas, Painéis e Acessos Rápidos

Durante o desenvolvimento, diversos serviços e painéis de controle ficam disponíveis na sua máquina:

| Serviço | URL / Porta | Usuário Padrão | Senha Padrão | Descrição |
|---|---|---|---|---|
| **Portal do Lojista** | [http://localhost:3001](http://localhost:3001) | `loja@elo.com` | `Merchant@123!` | Interface web onde o lojista cria pedidos e acompanha entregadores no mapa |
| **API Backend** | [http://localhost:8080](http://localhost:8080) | — | — | Servidor Connect RPC. Métricas em `/metrics` e healthcheck em `/health` |
| **RabbitMQ Painel** | [http://localhost:15672](http://localhost:15672) | `root` | `password` | Painel de gestão do RabbitMQ: inspecione filas, taxas de publish e DLQs |
| **Grafana** | [http://localhost:3000](http://localhost:3000) | `admin` | `password` *(ou a do .env)* | Painéis visuais de telemetria, traces distribuídos, taxas de erro e latência |
| **Prometheus** | [http://localhost:9090](http://localhost:9090) | — | — | Interface de consulta de métricas brutas e alertas em tempo real |
| **PostgreSQL (PostGIS)**| `localhost:5432` | `root` | `password` | Banco de dados. Conecte via DBeaver, pgAdmin ou `make db-shell` |
| **Redis** | `localhost:6379` | — | — | Cache em memória e índices geoespaciais. Acesse com `make redis-cli` |
| **Grafana Tempo** | [http://localhost:3200](http://localhost:3200) | — | — | Armazenamento de traces distribuídos OTLP |
| **Grafana Loki** | [http://localhost:3100](http://localhost:3100) | — | — | Endpoint de consulta de logs estruturados correlacionados a traces |

---

## 🤝 Fluxo de Trabalho Git da Equipe

Para garantir que o código continue estável e de fácil integração para todos, seguimos um fluxo simples de colaboração:

### 1. Padrão de Nomenclatura de Branches
Nunca comite diretamente na branch `main`. Crie uma branch para o que você for fazer:
- `feat/nome-da-funcionalidade` (Exemplo: `feat/add-order-filter`)
- `fix/descricao-do-bug` (Exemplo: `fix/geofence-calculation`)
- `docs/o-que-mudou` (Exemplo: `docs/update-readme`)
- `chore/tarefa-tecnica` (Exemplo: `chore/update-dependencies`)

### 2. Mensagens de Commit (Conventional Commits)
Use mensagens claras e em português ou inglês:
- `feat: adiciona botão de cancelamento de pedido`
- `fix: corrige validação de raio no cadastro de entregador`
- `docs: adiciona instruções no README`
- `test: adiciona testes unitários para a máquina de estados`

### 3. Checklist antes de abrir um Pull Request (PR)
Antes de enviar o seu código para o GitHub, rode este checklist rápido:
```bash
# 1. Formata o código
make fmt

# 2. Roda os testes para garantir que nada quebrou
make test

# 3. Valida os linters
make lint
```
Se tudo passou sem erros, faça o push e abra o seu Pull Request apontando para a branch `main`!

---

## 🛠️ Resolução de Problemas Frequentes (Troubleshooting / FAQ)

Separamos os problemas mais comuns que acontecem ao configurar o projeto pela primeira vez e como resolvê-los em segundos:

### 1. Erro: "Port 5432 (ou 8080, 3001, 5672) is already in use"
**Causa:** Você já possui um serviço (como um PostgreSQL ou Redis instalado na máquina física) rodando na mesma porta.  
**Como resolver:**
- Descubra quem está usando a porta:
  ```bash
  sudo lsof -i :5432
  ```
- Se for um serviço local do sistema (ex: Postgres nativo), você pode pausá-lo temporariamente:
  ```bash
  sudo systemctl stop postgresql
  ```
- Ou mate o processo que estiver ocupando a porta: `sudo kill -9 <PID>`.

---

### 2. Erro: "Dirty database version X. Fix and force version."
**Causa:** Uma migração falhou no meio da execução (por exemplo, erro de sintaxe SQL) e o banco ficou travado por segurança para não corromper os dados.  
**Como resolver:**
- Identifique a versão indicada no erro (exemplo: versão 8).
- Destrave o banco forçando a versão com o Makefile:
  ```bash
  make migrate-force V=8
  ```
- Corrija o arquivo SQL da migração e tente aplicar novamente com `make migrate-up`.

---

### 3. Erro: "Cannot connect to the Docker daemon" ou "Permission denied"
**Causa:** O serviço do Docker não está ligado ou o seu usuário do Linux não tem permissão de executar o Docker sem `sudo`.  
**Como resolver:**
- Inicie o Docker:
  ```bash
  sudo systemctl start docker
  ```
- Adicione seu usuário ao grupo do Docker:
  ```bash
  sudo usermod -aG docker $USER
  ```
  *(Depois disso, feche o terminal e abra novamente).*

---

### 4. Erro: "make: command not found"
**Causa:** O utilitário `make` não está instalado no seu sistema.  
**Como resolver:**
- No Ubuntu/Debian: `sudo apt install make`
- No Mac: `brew install make`
- No Windows (PowerShell): utilize o Docker diretamente (`docker compose up -d`) ou instale o make via `winget install GnuWin32.Make`.

---

### 5. Erro: "Simulador falhou na autenticação (Unauthorized)"
**Causa:** O simulador tenta logar com `entregador@elo.com` e a senha definida no `.env`. Se você alterou a senha no `.env` depois de rodar o `seed`, os valores estão divergentes.  
**Como resolver:**
- Verifique se as variáveis `SEED_DELIVERER_PASSWORD` e `DELIVERER_PASSWORD` no seu `.env` possuem o mesmo valor.
- Caso necessário, recrie os contêineres e o banco limpo:
  ```bash
  make down-v
  make up
  make migrate-up
  make seed
  ```

---

### 6. Mudei um arquivo `.proto` e as alterações não apareceram no código
**Causa:** Arquivos Protocol Buffers precisam ser compilados para gerar as interfaces Go e TypeScript.  
**Como resolver:**
- Execute o gerador de contratos:
  ```bash
  make proto-gen
  ```

---

## 📚 Documentações Complementares

Para aprofundar nos detalhes de arquitetura, decisões técnicas e regras de negócio:

- 🏛️ [Arquitetura Geral e Bounded Contexts (C4 Model)](docs/architecture.md)
- 📜 [Contratos de API (Protobuf & Connect RPC)](docs/api-contracts.md)
- 🗃️ [Esquema e Dicionário de Dados do Banco](docs/database-schema.md)
- 💼 [Regras de Negócio e Ciclo de Vida da Entrega](docs/business-rules.md)
- 🧪 [Estratégia de Testes e Concorrência](docs/testing.md)
- 📋 [Guia de Contribuição Completo](CONTRIBUTING.md)
- 📐 [Índice de Decisões de Arquitetura (ADRs 001 a 019)](docs/adrs/)
  - [ADR-002: Padrão Transactional Outbox](docs/adrs/002-transactional-outbox.md)
  - [ADR-013: Observabilidade Unificada com OpenTelemetry](docs/adrs/013-observabilidade-otel.md)
  - [ADR-018: Autenticação Zero Trust e Sessões](docs/adrs/018-autenticacao-gestao-sessoes.md)
  - [ADR-019: Topologia Declarativa RabbitMQ e Pacing](docs/adrs/019-topologia-rabbitmq-declarativa-e-pacing-telemetria.md)

---

<div align="center">
  <b>Projeto Integrador - Engenharia de Software</b><br>
  Construído com foco em resiliência, baixa latência e apoio ao comércio de bairro.
</div>
