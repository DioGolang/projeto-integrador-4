# ADR-007: Ingestão e Armazenamento de Geolocalização via Redis

## Status
Aceito

## Contexto
O requisito **RF06** e **RF02** estipulam que o sistema precisa identificar entregadores ativos em uma determinada região (raio de atuação) no momento do fan-out.
Para isso, os aplicativos dos entregadores precisam reportar suas coordenadas geográficas periodicamente (ex: a cada 10 a 15 segundos).

Os desafios arquiteturais envolvem:
1. **Frequência de Ingestão:** Atualizar a posição a cada 10-15 segundos gera um altíssimo volume de requests (write-heavy). Se tivermos 1.000 entregadores, são milhares de requests por minuto.
2. **Armazenamento Efêmero vs Persistente:** A posição de um entregador há 1 hora não importa para o roteamento atual; apenas o dado fresco ("em tempo real") interessa. Fazer um `UPDATE` massivo em um banco relacional como o PostgreSQL gera severa sobrecarga de I/O e *bloat* (acúmulo de *dead tuples*), degradando a performance geral do banco que deveria estar focado em transações de pedidos (ACID).

## Decisão
Adotamos uma arquitetura híbrida, separando os dados cadastrais (persistentes) dos dados de localização (efêmeros), introduzindo o **Redis** na stack:

1. **Ingestão (Write):** O App do entregador envia um request HTTP `PUT /api/v1/fleet/location` contendo lat/long. A API em Go recebe o request e executa um comando `GEOADD` no **Redis**. 
2. **Armazenamento Efêmero:** O Redis armazena os dados em memória com extrema velocidade. O comando `EXPIRE` ou rotinas de limpeza podem ser usados para expirar localizações antigas (entregadores que ficaram offline sem avisar).
3. **Consulta de Raio (Read):** No momento em que um pedido é criado e o serviço `dispatcher` precisa realizar o Fan-out, ele consulta o Redis usando o comando `GEOSEARCH` (ou equivalente `GEORADIUS`), passando a coordenada do Lojista e o raio desejado (ex: 5km). O Redis retorna instantaneamente a lista de entregadores (`deliverer_id`) na região.
4. **PostgreSQL:** O banco relacional continua armazenando o perfil do entregador (`deliverers`), mas sem as colunas que mudam a cada segundo (`current_lat`, `current_lng`).

## Consequências

**Positivas:**
- **Alta Performance e Escalabilidade:** O Redis foi feito para absorver cargas massivas de leitura e escrita em memória (centenas de milhares de TPS), isolando perfeitamente a base transacional (Postgres).
- **Semântica Geospacial Nativa:** Os comandos `GEOADD` e `GEOSEARCH` do Redis facilitam imensamente o cálculo matemático de raio.
- **Rigor Acadêmico:** Demonstra à banca um sólido entendimento sobre separação de dados transacionais (*cold/warm data*) e dados efêmeros (*hot data*), evitando anti-patterns clássicos de banco de dados.

**Negativas / trade-offs:**
- **Complexidade de Infraestrutura:** Introduz uma nova peça móvel (Redis) no `docker-compose` e nos ambientes de produção.
- **Consistência Eventual:** Se o Redis reiniciar sem persistência de disco (AOF/RDB), perdemos as localizações atuais. Como a localização é efêmera, os apps enviarão novas posições em 15 segundos, recarregando o cache (tolerável pelo negócio).
