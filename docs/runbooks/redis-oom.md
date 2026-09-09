# Runbook: Out of Memory (OOM) no Redis

Este playbook orienta a remediação rápida caso o cluster Redis, utilizado para ingestão espacial e *Fan-out* via `GEOADD` (ADR-007), atinja seu limite de memória RAM (OOM) e pare de aceitar novas requisições.

## Sintomas
- Alertas do Prometheus: `redis_memory_used_bytes` se aproxima de `redis_memory_max_bytes`.
- A API em Go começa a cuspir logs de erro de rede `OOM command not allowed when used memory > 'maxmemory'`.
- Lojistas não conseguem despachar pedidos novos (Efeito cascata).

## 1. Ação de Contenção Imediata (Mitigação)
O objetivo principal é restaurar a funcionalidade da API em menos de 1 minuto, descartando dados não críticos.

1. Acesse o servidor ou contêiner do Redis via `redis-cli`.
2. Como armazenamos coordenadas efêmeras de motoboys, esses dados expiram naturalmente. Podemos forçar a deleção das chaves mais volumosas utilizando `UNLINK` (assíncrono) para não travar o *thread* principal do Redis:
   ```bash
   redis-cli KEYS "fleet:locations:*" | xargs redis-cli UNLINK
   ```
3. Alternativamente, ajuste dinamicamente a política de expulsão de memória para matar conexões antigas ou dados voláteis:
   ```bash
   redis-cli CONFIG SET maxmemory-policy allkeys-lru
   ```

## 2. Ação Definitiva (Resolução Raiz)
1. Verifique se as novas localizações publicadas pela frota estão, de fato, utilizando um tempo de expiração (`EXPIRE`) agressivo. Um bug no cliente Go pode ter removido a instrução de TTL.
2. Dimensione corretamente a RAM do servidor no `docker-compose.yml` ou orquestrador. Se a malha logística (quantidade de motoboys ativos) dobrou, a reserva de RAM para o cache deve aumentar proporcionalmente.
