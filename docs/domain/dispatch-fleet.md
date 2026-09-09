# Domínio: Despacho e Frota (Dispatch & Fleet)

Cobre o mecanismo de Fan-out, a máquina de estados finita do entregador, a mitigação de Dead Miles, e os limites operacionais de raio/área de cobertura. Referencia ADR-001 (comunicação assíncrona), RF02/RF05 (definidos em `order-lifecycle.md`).

---

## Fan-Out e Redespacho

- Ao entrar em `DISPATCHED`, o sistema identifica entregadores candidatos ativos na região:
  1. **Busca Geográfica (Redis GEOSEARCH):** O motor busca no Redis os IDs dos entregadores que estão dentro do raio definido e com status `is_active = true`.
  2. **Filtro de Capacidade (Vehicle Constraints):** Se o pedido for sinalizado com `PACKAGE_SIZE_LARGE` (volume alto, ex: 15 pizzas), o motor descarta sumariamente os entregadores com `vehicle_type == 'BICYCLE'`, evitando cancelamentos tardios por incapacidade de transporte.
  3. **Broadcast FCFS:** A notificação (push) é enviada simultaneamente aos `N` entregadores qualificados.
- Priorização por score de reputação: entregadores com score abaixo do limiar definido em `reputation-pricing.md` recebem a notificação com um pequeno delay artificial em relação aos demais — o modelo permanece FCFS, mas bom histórico dá vantagem de fato sem excluir ninguém.
- **Redespacho (RF05):** pedido `DISPATCHED` sem aceite dentro do tempo limite (ex: 10 minutos) cancela a busca atual e reinicia um novo ciclo de Fan-out.
- **Esgotamento:** após um número máximo de ciclos de redespacho (ex: 3 × 10 minutos, configurável), o pedido transiciona para `CANCELLED_NO_DELIVERER` (estorno tratado em `payment.md`).

## Máquina de Estados Finita (FSM) do Entregador

- Um entregador só entra no raio de busca do Fan-out se obedecer duas condições simultâneas:
  1. `driver.status == 'ONLINE'`
  2. `driver.active_order_id == NULL` (sem batching de corridas no MVP — o entregador leva apenas um pedido por vez).
- Transição para `ONLINE` exige verificação de liveness obrigatória (regra completa em `security-zero-trust.md`) — não é apenas uma flag de disponibilidade, é um gate de segurança.

## SLA de Deslocamento (Mitigação de No-Show)

- **O Problema:** Um entregador aceita o pedido (`ACCEPTED`), o lojista inicia o preparo (JIT Prep), mas o entregador nunca chega à loja (pneu furado, bateria acabou, etc), mantendo o pedido refém.
- **A Regra (Auto-Reatribuição):**
  1. Ao transicionar para `ACCEPTED`, a API publica uma mensagem *Delayed* no RabbitMQ (ex: timeout de 15 minutos baseados no ETA do percurso até a loja).
  2. Ao consumir essa mensagem no futuro, o sistema verifica se o status do pedido ainda é `ACCEPTED` (ou seja, o entregador não fez o check-in geográfico de `AT_STORE`).
  3. Em caso afirmativo, o sistema **remove autonomamente** o entregador do pedido, aplica uma forte penalidade no seu *Reputation Score* (Abandono de Corrida), reverte o pedido para `DISPATCHED` e dispara um novo *Fan-out* imediato para salvar o pedido antes que a comida esfrie.

## Limites Operacionais

### Raio Máximo de Entrega
- Distância loja→cliente (calculada por rota real, não linha reta — mesma infraestrutura usada para o cálculo de frete em `reputation-pricing.md`) acima de um limite (ex: 8 km) é **rejeitada na criação do pedido**, com mensagem clara ao lojista.

### Área de Cobertura (Região Piloto)
- Tabela `coverage_areas` com polígonos (PostGIS `ST_Contains`) define onde a plataforma opera.
- Pedido com origem fora da área de cobertura ativa é **rejeitado na criação**, não deixado para falhar silenciosamente 10-15 minutos depois via timeout do RF05.

## Mitigação de Dead Miles

Resolvido estruturalmente pela ausência de qualquer equipamento de cobrança físico no fluxo do entregador (pagamento é 100% antecipado — ver `payment.md` e ADR-007), o que remove qualquer obrigação de retorno ao ponto de origem após a entrega.

- **Roteamento Preditivo/Chaining (mecanismo primário):** ao completar uma corrida, a posição atual do entregador (não a loja de origem) é imediatamente atualizada no Redis GeoSearch, tornando-o elegível para o próximo Fan-out da região onde ele já está.
- **Heatmap (camada complementar de UI):** visualização de zonas com pedidos não aceitos, como sinal informativo — não como mecanismo de decisão do sistema.
- **"Indo para Casa" (destino fixo por geometria de vetor):** descartado do MVP por complexidade/baixo retorno inicial; candidato a evolução futura.
