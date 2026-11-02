# ==============================================================================
# PROJETO INTEGRADOR 4 - PLATAFORMA DE LOGÍSTICA EM TEMPO REAL
# ==============================================================================

.DEFAULT_GOAL := help
SHELL         := /usr/bin/env bash

# Configurações e Variáveis
DOCKER_COMPOSE ?= $(shell which docker-compose 2>/dev/null || echo "docker compose")
GO             ?= go
MIGRATIONS_DIR ?= migrations/
DB_URL         ?= postgres://root:password@localhost:5432/logistics?sslmode=disable
TEST_TIMEOUT   ?= 120s

.PHONY: help \
	up down down-v restart ps logs logs-api logs-simulator logs-db logs-rabbitmq \
	db-shell redis-cli \
	migrate-up migrate-down migrate-force migrate-create seed \
	test test-unit test-api test-simulator test-integration test-e2e \
	test-coverage test-coverage-html test-web test-op loadtest \
	build build-api build-simulator build-web \
	run-api run-simulator run-web \
	lint lint-api lint-web fmt tidy clean \
	gen proto-gen buf-lint sqlc-gen

##@ 📖 Ajuda
help: ## Exibe este menu de ajuda organizado por categorias
	@awk 'BEGIN {FS = ":.*##"; printf "\n\033[1mComandos disponíveis:\033[0m\n"} \
		/^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2 } \
		/^##@/ { printf "\n\033[1;33m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ 🚀 Docker & Infraestrutura
up: ## Sobe toda a infraestrutura em background via Docker Compose
	$(DOCKER_COMPOSE) up -d

down: ## Para todos os contêineres da infraestrutura
	$(DOCKER_COMPOSE) down

down-v: ## Para todos os contêineres e REMOVE volumes de dados persistentes
	$(DOCKER_COMPOSE) down -v

restart: ## Reinicia todos os contêineres da infraestrutura
	$(DOCKER_COMPOSE) restart

ps: ## Exibe o status operacional de todos os contêineres
	$(DOCKER_COMPOSE) ps

logs: ## Acompanha os logs unificados de todos os serviços em tempo real
	$(DOCKER_COMPOSE) logs -f

logs-api: ## Acompanha os logs exclusivos da API
	$(DOCKER_COMPOSE) logs -f api

logs-simulator: ## Acompanha os logs exclusivos do Simulador
	$(DOCKER_COMPOSE) logs -f simulator

logs-db: ## Acompanha os logs exclusivos do PostgreSQL/PostGIS
	$(DOCKER_COMPOSE) logs -f postgres

logs-rabbitmq: ## Acompanha os logs exclusivos do RabbitMQ
	$(DOCKER_COMPOSE) logs -f rabbitmq

db-shell: ## Abre sessão interativa psql no contêiner do PostgreSQL
	$(DOCKER_COMPOSE) exec postgres psql -U root -d logistics

redis-cli: ## Abre interface interativa redis-cli no contêiner do Redis
	$(DOCKER_COMPOSE) exec redis redis-cli

##@ 🧪 Testes
test: ## Executa todos os testes do workspace (API e Simulador)
	$(GO) test -v ./api/... ./simulator/...

test-unit: ## Executa testes unitários rápidos (-short)
	$(GO) test -v -short ./api/... ./simulator/...

test-api: ## Executa todos os testes do módulo API
	cd api && $(GO) test -v ./...

test-simulator: ## Executa todos os testes do módulo Simulador
	cd simulator && $(GO) test -v ./...

test-integration: ## Executa testes com tag integration (ex: OSRM e RabbitMQ do Simulador)
	cd simulator && $(GO) test -v -tags integration -timeout $(TEST_TIMEOUT) ./internal/engine/...

test-e2e: ## Executa teste ponta a ponta (E2E) da API (requer Postgres e RabbitMQ rodando)
	cd api && $(GO) test -v -tags e2e -timeout $(TEST_TIMEOUT) ./e2e_test.go

test-coverage: ## Executa os testes e exibe relatório de cobertura por função no terminal
	$(GO) test -coverprofile=coverage.out ./api/... ./simulator/...
	$(GO) tool cover -func=coverage.out

test-coverage-html: ## Executa os testes e gera relatório de cobertura visual em HTML (coverage.html)
	$(GO) test -coverprofile=coverage.out ./api/... ./simulator/...
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "Relatório gerado em: coverage.html"

test-web: ## Executa checagem de tipos e lint do portal Web
	cd web && npm run lint

test-op: ## Executa o teste operacional completo (Simulador -> API -> Stream)
	$(GO) run ./api/cmd/operational_test

loadtest: ## Executa teste de estresse/saturação OpenTelemetry na API
	$(GO) run ./api/cmd/loadtest

##@ 🗄️ Banco de Dados & Migrations
migrate-up: ## Executa todas as migrações pendentes no banco
	migrate -path $(MIGRATIONS_DIR) -database "$(DB_URL)" up

migrate-down: ## Desfaz a última migração aplicada no banco
	migrate -path $(MIGRATIONS_DIR) -database "$(DB_URL)" down 1

migrate-force: ## Força versão da migração em caso de dirty state (uso: make migrate-force V=8)
	@if [ -z "$(V)" ]; then echo "Erro: informe a versão com V=<versao>"; exit 1; fi
	migrate -path $(MIGRATIONS_DIR) -database "$(DB_URL)" force $(V)

migrate-create: ## Cria novo par de migrações up/down (uso: make migrate-create NAME=nome_da_migracao)
	@if [ -z "$(NAME)" ]; then echo "Erro: informe o nome com NAME=<nome>"; exit 1; fi
	migrate create -ext sql -dir $(MIGRATIONS_DIR) -seq $(NAME)

seed: ## Executa o seeder de dados iniciais (usuários, perfis de entregadores)
	$(GO) run ./api/cmd/seed

##@ 📦 Build & Execução Local
build: build-api build-simulator build-web ## Compila os binários em bin/ e compila o portal Web

build-api: ## Compila o binário da API em bin/api
	@mkdir -p bin
	cd api && $(GO) build -o ../bin/api ./cmd/server

build-simulator: ## Compila o binário do simulador em bin/simulator
	@mkdir -p bin
	cd simulator && $(GO) build -o ../bin/simulator .

build-web: ## Compila o build de produção do Next.js
	cd web && npm run build

run-api: ## Executa a API localmente a partir do código fonte
	cd api && $(GO) run ./cmd/server

run-simulator: ## Executa o simulador localmente a partir do código fonte
	cd simulator && $(GO) run .

run-web: ## Executa o servidor de desenvolvimento do portal Web (porta 3001)
	cd web && npm run dev

##@ 🛠️ Qualidade de Código & Limpeza
lint: lint-api lint-web ## Executa todos os linters (Go e Next.js)

lint-api: ## Executa golangci-lint no módulo da API
	cd api && golangci-lint run

lint-web: ## Executa o linter ESLint no módulo Web
	cd web && npm run lint

fmt: ## Formata todo o código Go do projeto
	$(GO) fmt ./api/... ./simulator/...

tidy: ## Sincroniza e limpa as dependências go.mod de todos os módulos
	cd api && $(GO) mod tidy
	cd simulator && $(GO) mod tidy
	$(GO) work sync

clean: ## Remove binários compilados, relatórios de cobertura e arquivos temporários
	rm -rf bin/ coverage.out coverage.html
	$(GO) clean -cache -testcache

##@ ⚡ Geração de Código
gen: proto-gen sqlc-gen ## Executa todos os geradores de código (Protobuf e sqlc)

proto-gen: ## Compila definições Protobuf com buf generate
	buf generate

buf-lint: ## Executa linter nos arquivos .proto com buf lint
	buf lint

sqlc-gen: ## Gera código Go das queries SQL via sqlc
	cd api && sqlc generate
