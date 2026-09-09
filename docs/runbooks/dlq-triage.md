# Runbook: Triagem de Dead Letter Queue (DLQ)

Este playbook orienta a operação em caso de disparo de alerta de acumulação de mensagens na DLQ do RabbitMQ, instituída pelo ADR-009 para proteger o sistema contra *Poison Pills* (mensagens corrompidas que quebram o parser ou regras de negócio).

## 1. Identificação e Diagnóstico

Quando o alerta do Prometheus apontar > 0 mensagens na DLQ (`rabbitmq_queue_messages{queue="dead_letter_queue"}`):
1. Abra a interface de administração do RabbitMQ (porta padrão `15672`).
2. Navegue até a aba **Queues** e acesse a fila `dead_letter_queue`.
3. Use a funcionalidade **Get Message(s)** para extrair a primeira mensagem sem dar `ACK` (Requeue = True).
4. Inspecione os cabeçalhos (`headers`). O RabbitMQ injeta automaticamente o cabeçalho `x-death`, que mostra por que a mensagem foi roteada para lá (ex: `rejected`).

## 2. Classificação do Erro

Inspecione o payload da mensagem e os logs do Grafana Loki utilizando o `Trace-ID` (injetado no header pelo OTel).

- **Poison Pill de Formato:** O JSON está malformado ou há erro de conversão de tipo (ex: tentou fazer *unmarshal* de string para int).
- **Poison Pill de Domínio:** O pedido faz referência a um `merchant_id` que foi deletado fisicamente do banco (violando foreign keys ou lógicas críticas do motor).
- **Falsa Positiva (Instabilidade Mascarada):** O erro foi gerado por falha no banco de dados, mas o desenvolvedor usou um tipo genérico de retorno de erro e o sistema classificou erroneamente como não-recuperável.

## 3. Remediação

### Passo 1: Correção do Bug
Se for uma *Poison Pill* genuína (formato ou domínio), o código de processamento no Go ou a estrutura de dados no Postgres precisa ser corrigida via Pull Request. **Não tente reprocessar a mensagem imediatamente**, ela continuará quebrando o sistema.

### Passo 2: Reinjeção (Shoveling)
Após o código ser corrigido e o novo *deploy* da API estar em produção:
1. No RabbitMQ, utilize o plugin **Shovel Management** (via painel Admin).
2. Configure um novo Shovel de origem (a DLQ) para o destino (a fila original, ex: `order_dispatch_queue`).
3. O Shovel puxará as mensagens e as devolverá para o processamento normal.
4. Monitore a aba Queues para garantir que a fila original está consumindo (crescimento e queda rápidos) e que a DLQ esvaziou permanentemente.
