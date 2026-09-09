# Domínio: Reputação e Precificação (Reputation & Pricing)

Cobre o score de reputação (entregador e lojista, simétrico) e a fórmula de precificação do frete calculada pela plataforma.

---

## Score de Reputação do Entregador

- Escala de 0 a 100, com decaimento por janela móvel de 90 dias. Composição:
  - **Taxa de no-show** (ver SLA de coleta em `order-lifecycle.md`/`dispatch-fleet.md`): peso alto — trava um pedido e o devolve à fila.
  - **Taxa de cancelamento pós-`ACCEPTED`:** peso médio.
  - **Casos de `SUSPECTED_THEFT` confirmados** em revisão manual (ver `security-zero-trust.md`): zera o score e suspende a conta permanentemente — desqualificação, não desconto de pontos.
  - **Avaliação do lojista pós-entrega** (1 a 5 estrelas, opcional no MVP): peso baixo, sinal complementar.

### Efeito Prático no Fan-Out
- Score acima de um limiar (ex: 70): sem alteração no Fan-out.
- Score abaixo do limiar: recebe a notificação de nova corrida com delay artificial curto (ex: 5-10 segundos) em relação aos demais entregadores — o modelo permanece FCFS como princípio formal (ver `order-lifecycle.md`/`dispatch-fleet.md`), mas bom histórico dá vantagem de fato.
- Score abaixo de um segundo limiar mais baixo (ex: 30): conta suspensa automaticamente para revisão manual.

## Score de Reputação do Lojista (simétrico)

- **Taxa de cancelamento pós-`DISPATCHED`** (já bloqueou um entregador à toa): comportamento equivalente ao no-show do entregador, com consequência simétrica.
- **Taxa de disputa** (`UNDER_DISPUTE`, ver `payment.md`) por item errado/faltante.
- Acima de um limiar de cancelamentos indevidos: aviso ao lojista; persistindo, entra na fila de revisão manual (mesma estrutura operacional usada para `SUSPECTED_THEFT`).

## Precificação do Frete

- **Fórmula calculada pela plataforma, não editável pelo lojista:**

  ```text
  frete = taxa_base + (distância_km × taxa_por_km)
  ```

  com piso mínimo (ex: R$ 6,00) para proteger o entregador em corridas muito curtas.
- Distância calculada via rota real (não linha reta), usando a mesma infraestrutura de geolocalização/roteamento já prevista para o raio máximo de entrega (PostGIS/OSRM ou API de rotas do provedor de mapas escolhido — ver `dispatch-fleet.md`).
- O lojista informa origem/destino no RF01 (ver `order-lifecycle.md`); o backend calcula e retorna o valor do frete como parte da resposta de criação do pedido, para confirmação — nunca edição livre. Isso fecha a superfície de manipulação de preço que existia quando o valor era um campo digitado livremente.
