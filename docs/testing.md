# Estratégia de Qualidade e Testes (QA & Testing Strategy)

Como um sistema distribuído de missão crítica (despacho logístico sob pico de carga), falhas em produção custam dinheiro e reputação. Esta documentação estabelece o direcionamento para testes automatizados, seguindo os preceitos de Engenharia de Software Moderna.

## 1. Pirâmide de Testes e Tipologia

Adotamos a pirâmide de testes pragmática: ampla na base (unitários e rápidos) e concentrada no meio (integração real).

### 1.1 Testes Unitários (Base)
**Onde atuar:** Camada de `domain` e `application` (Regras de negócio, *State Machine* dos Pedidos, algoritmos de cálculo de raio e de *Backoff Jitter*).
- **Padrão em Go:** Uso de *Table Driven Tests* (tabelas de *structs* anônimos) para cobrir permutações extensas de parâmetros (ex: testes de validação de payload).
- **Mocks:** Utilizaremos `gomock` para simular as interfaces dos repositórios (`OrderRepository`) e *Publishers*. Nenhum teste unitário pode encostar em I/O (rede ou disco).
- **Frontend:** Uso de `Vitest` + `React Testing Library` para componentes vitais puros.

### 1.2 Testes de Integração (Meio - O Coração do Projeto)
Em Go, testes de integração são essenciais, pois as queries e concorrências de banco não podem ser plenamente garantidas apenas com Mocks.
- **Tecnologia:** Uso massivo de **Testcontainers-Go**. O Go sobe contêineres efêmeros (PostgreSQL, Redis, RabbitMQ) via Docker antes do teste, aplica as *migrations* e destrói após a suíte rodar.
- **Alvos Críticos:**
  - **Atomic State Update (ADR-003):** Rodar `go test -race` disparando dezenas de `goroutines` chamando `AcceptDelivery` simultaneamente, assegurando que o `UPDATE ... WHERE status = 'DISPATCHED'` impede o *Double Booking* no banco real.
  - **Transactional Outbox (ADR-002):** Validar a integridade entre o `INSERT` do Pedido e do Evento.

### 1.3 Testes E2E (Topo)
Testes que emulam o ciclo de vida completo do usuário final.
- Serão enxutos. Focados no **Caminho Feliz (Happy Path)** e falhas críticas.
- Validarão se o contrato do *Connect RPC* está fluido entre o Next.js e o backend em Go.

## 2. Metas de Cobertura (Coverage Goals)

O sistema rejeita a busca cega por "100% de cobertura global" (*Coverage Vanity*). Em vez disso, adotamos o pragmatismo focado no risco:

1. **Meta Global (Base):** O `lefthook` (pré-commit) exigirá um mínimo de **80% de cobertura de código** global.
2. **Meta Core Domain:** Os pacotes `internal/order/domain` e `internal/dispatch/domain` exigem **100% de cobertura rigorosa**. Bugs lógicos aqui causam perdas financeiras.
3. **Exclusões de Linting:** Código autogerado pelo Buf (`gen/logistics/v1/`) ou mocks (`mock_*.go`) serão excluídos do cálculo de *coverage* via `go test -coverpkg`.

## 3. Padrões de Escrita de Testes (AAA Pattern)

Todo teste, seja em Go ou TS, deve respeitar a legibilidade do padrão **Arrange, Act, Assert**.

```go
func TestOrder_AcceptDelivery(t *testing.T) {
	// Arrange (Preparo do Estado)
	order := domain.NewOrder(merchantID, 150.00)
	order.Status = domain.StatusDispatched
	delivererID := uuid.New()

	// Act (Ação)
	err := order.AcceptDelivery(delivererID)

	// Assert (Validação)
	assert.NoError(t, err) // usando github.com/stretchr/testify/assert
	assert.Equal(t, domain.StatusAccepted, order.Status)
	assert.Len(t, order.Events(), 1, "deve emitir o evento de DeliveryAccepted")
}
```

## 4. Integração no Pipeline
O `go test -v -cover -race ./...` é executado:
1. Pelo **Lefthook** na máquina do desenvolvedor antes do commit (local).
2. Pelo **GitHub Actions** em todas as Pull Requests. PRs que degradarem a cobertura abaixo do percentual vigente não poderão ser mergiados.
