# ADR-012: Upgrade Estratégico de Infraestrutura (RabbitMQ 4.x, PostgreSQL 18, Redis 8)

## Status
Aceito

## Contexto
Durante o planejamento final da infraestrutura do Motor Logístico, avaliamos o ciclo de vida (EOL) e as novas capacidades oferecidas pelas versões mais recentes das nossas dependências principais. Identificamos que manter as versões estabilizadas (Postgres 16, Redis 7 e RabbitMQ 3.13) nos impediria de tirar proveito de melhorias drásticas de performance e resiliência, além de nos colocar em risco de obsolescência no médio prazo.

O destaque primário desta decisão recai sobre a **série 4.x do RabbitMQ**, que introduz uma reescrita fundacional do armazenamento do broker, além do ciclo de vida: versões como a 4.1 e 4.2 alcançarão o fim do suporte oficial (EOL) em 2026, não recebendo mais correções de segurança.

## Decisão
Decidimos atualizar todas as *engines* de infraestrutura para o topo de linha disponível (State of the Art):

1. **PostgreSQL 18**
2. **Redis 8**
3. **RabbitMQ 4.3.5**

### Justificativas (RabbitMQ 4.x)
- **Armazenamento (Khepri):** A partir da série 4, o *Khepri* (baseado em Raft) tornou-se o único mecanismo padrão de armazenamento de metadados do broker, substituindo tecnologias antigas baseadas no Mnesia. Isso garante replicação de estado muito mais segura.
- **Quorum Queues:** O modelo de filas de quórum recebeu melhorias significativas em resiliência. Duas novas funções essenciais que utilizaremos no projeto:
  1. Suporte nativo a retentativas atrasadas (*delayed retries*), fundamental para o nosso padrão de resiliência e Dead Letter Queues (ADR-009).
  2. Prioridade estrita de mensagens (para pedidos VIP ou roteamento crítico).

## Consequências

**Positivas:**
- O projeto nasce à prova do futuro (Future-proof), evitando migrações forçadas de versão nos próximos anos.
- Maior consistência de dados no cluster do RabbitMQ usando Khepri.
- Maior velocidade no processamento geospacial do Redis 8.

**Negativas / trade-offs:**
- Requerimento de máquinas virtuais ou containers mais atualizados (compatibilidade).
- Ferramentas de GUI antigas ou drivers de terceiros podem não ter total aderência inicial às versões lançadas muito recentemente.
