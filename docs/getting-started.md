# Developer Experience (DX) e Setup Local

Este documento descreve como preparar o ambiente de desenvolvimento local para o Monolito Modular do motor logístico. Nossa prioridade é garantir que qualquer novo engenheiro consiga rodar a stack completa com apenas um ou dois comandos, reduzindo o tempo de *onboarding* e evitando falhas manuais.

## Pré-requisitos

Certifique-se de que sua máquina possui as seguintes ferramentas instaladas:
- **Docker e Docker Compose** (para infraestrutura local: Postgres, Redis, RabbitMQ, OTel)
- **Go 1.21+** (Backend)
- **Node.js 20+ e npm/pnpm** (Frontend Next.js)
- **Buf CLI** (Geração de código Protobuf para o Connect RPC)
- **golang-migrate ou goose** (Migrations do banco de dados)
- **Make** (Gerenciador de tarefas nativo do Unix)

## O Workflow com Makefile

A raiz do nosso Monorepo possui um arquivo `Makefile` que abstrai todos os comandos complexos. **Nunca rode comandos complexos manualmente.** Utilize as seguintes abstrações:

### 1. Subindo a Infraestrutura
```bash
make up
```
*O que faz:* Sobe o `docker-compose.yml` em background (`-d`), instanciando o Postgres 18 (com PostGIS), Redis 8, RabbitMQ 4.3.5 (com Quorum Queues e Khepri) e o OTel Collector.

### 2. Rodando as Migrações
```bash
make migrate-up
```
*O que faz:* Executa o utilitário de migração (ex: `golang-migrate`) aplicando os arquivos `.up.sql` da pasta `migrations/` contra o banco Postgres que acabou de subir.

### 3. Geração de Código de Contratos (RPC)
```bash
make proto-gen
```
*O que faz:* Lê os contratos em `proto/` e utiliza o Buf para gerar a tipagem estrita tanto para o Go (Backend) quanto para o TypeScript (Frontend).

### 4. Code Quality e Linting
```bash
make lint
```
*O que faz:* Executa o `golangci-lint` no backend e o `eslint` no frontend, garantindo que o código não infrinja nenhuma das regras arquiteturais de Go (ex: variáveis *shadowed*, *error checking* solto, etc).

## Iniciando o Desenvolvimento

Após executar `make up` e `make migrate-up`, você pode inicializar as camadas da aplicação:

**Para rodar a API (Go):**
```bash
cd api
go run cmd/server/main.go
```

**Para rodar o Painel do Lojista (Next.js):**
```bash
cd web
npm run dev
```

*Nota:* Lembre-se de configurar o arquivo `.env` na raiz conforme detalhado no ADR-016 (12-Factor App) utilizando o arquivo `.env.example` como base.
