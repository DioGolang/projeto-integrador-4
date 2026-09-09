# ADR-003: Resolução de Concorrência no Aceite de Pedidos

## Status
Aceito

## Contexto

RF03 exige que, quando múltiplos entregadores tentam aceitar a mesma corrida simultaneamente, apenas o primeiro tenha sucesso e os demais sejam notificados de que a corrida não está mais disponível. Três estratégias foram avaliadas:

1. **Pessimistic Locking** (`SELECT ... FOR UPDATE`): trava a linha do pedido para o primeiro request; os demais esperam na fila do banco até a trava liberar.
2. **Optimistic Locking** (coluna `version`): cada update exige que a versão atual coincida; o segundo request recebe um erro de conflito de versão.
3. **Atomic State Update** (`UPDATE ... WHERE status = 'DISPATCHED'`): a query de aceite já embute a condição do estado atual como parte do `WHERE`; apenas uma linha é afetada.

## Decisão

Adotamos **Atomic State Update** como mecanismo primário de resolução de concorrência:

```sql
UPDATE orders
SET status = 'ACCEPTED', deliverer_id = $1, accepted_at = now(), updated_at = now()
WHERE id = $2 AND status = 'DISPATCHED';
```

A aplicação verifica `RowsAffected()`: se `1`, o aceite foi bem-sucedido; se `0`, outro entregador já havia aceitado, e a aplicação retorna `domain.ErrConflict` (tratado como resultado de negócio esperado, não como erro de sistema — ver `AcceptDeliveryUseCase`).

Essa garantia de concorrência vive na camada de infraestrutura (`OrderRepository.AcceptAtomic`), enquanto a regra de negócio de transição de estado (`DISPATCHED → ACCEPTED`) é validada de forma independente no método `Order.Accept()` do agregado de domínio — as duas camadas se reforçam, mas têm responsabilidades distintas.

Descartamos Pessimistic Locking por manter transações abertas e criar gargalos sob alta volumetria em horários de pico (RNF03), e Optimistic Locking clássico por exigir uma coluna `version` e lógica de retry adicional que não trazem benefício sobre o Atomic Update para uma máquina de estados simples como a do `Order`.

## Consequências

**Positivas:**
- Não há locks de linha mantidos abertos — cada tentativa de aceite é uma única query atômica e rápida, ideal para alta concorrência (RNF03).
- Não requer coluna de controle de versão nem lógica de retry no backend.
- A condição de corrida é resolvida inteiramente pelo próprio motor transacional do PostgreSQL (garantia de isolamento a nível de linha), sem necessidade de locks explícitos na aplicação.

**Negativas / trade-offs:**
- Estratégia acoplada a esta máquina de estados específica; se o domínio crescer para exigir controle de concorrência em múltiplos campos independentes (não apenas `status`), Optimistic Locking com `version` pode se tornar necessário no futuro.
- Exige testes de carga (RNF08 — k6/Vegeta) para validar empiricamente que, sob concorrência real, apenas um `RowsAffected() == 1` ocorre por pedido.
