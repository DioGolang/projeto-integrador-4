# Runbook: Rollback de Migrations no Banco de Dados

Este playbook orienta o procedimento de segurança caso um *deploy* execute uma migração no PostgreSQL (`make migrate-up`) que gere dados corrompidos, locks de tabelas indesejados ou performance degradada em produção.

## 1. Pare a Hemorragia (Desligue a API)
Não tente reverter o esquema do banco enquanto a API Go em produção ainda tenta escrever na nova estrutura. Se a migração gerou downtime lógico, force o desligamento das réplicas da aplicação no cluster/orquestrador.

## 2. Executando o Rollback (`down.sql`)
Todas as migrações (usando `golang-migrate` ou `goose`) são compostas de pares estritos. Exemplo:
- `000002_add_vehicle_capacity.up.sql`
- `000002_add_vehicle_capacity.down.sql`

O comando de rollback reverte a última migração executada aplicando o arquivo `.down.sql` correspondente.

### Se estiver usando golang-migrate local/CLI:
```bash
migrate -path migrations/ -database "postgres://user:pass@host:5432/db?sslmode=disable" down 1
```
O número `1` indica que o sistema deve desfazer apenas 1 migração (a última). 

## 3. Lidando com Migrações "Sujas" (Dirty State)
Se a execução do `.up.sql` falhou no meio por um erro de sintaxe (e não havia suporte a DDL transacional na query), a ferramenta travará o estado do banco como `dirty = true`. 
Você não conseguirá rodar novas migrações nem realizar `down` sem antes "limpar" o histórico de versão (ignorando o erro):
```bash
migrate -path migrations/ -database "postgres://..." force <versao_anterior_segura>
```
Após forçar a versão correta no controle de migração, limpe as alterações sujas manualmente via `psql` (se necessário).

## 4. Retomada
- Corrija o arquivo SQL defeituoso, abra um novo Pull Request.
- Reinicie os serviços da API.
