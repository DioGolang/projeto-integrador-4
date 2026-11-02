#!/usr/bin/env bash
set -euo pipefail

echo "================================================================="
echo "  TESTE CONTROLADO DE IDEMPOTÊNCIA E REDELIVERY (CONSUMIDOR)     "
echo "================================================================="

# 1. Login Lojista
echo "1. Autenticando lojista..."
MERCHANT_TOKEN=$(docker run --rm --network projeto-integrador-4_default curlimages/curl:latest -s -X POST http://api:8080/auth.v1.AuthService/Login \
  -H "Content-Type: application/json" \
  -d '{"email":"loja@elo.com","password":"Merchant@123!"}' | grep -o '"accessToken":"[^"]*' | cut -d'"' -f4)

# 2. Criar Pedido Exclusivo de Idempotência
echo "2. Criando pedido exclusivo para validação de idempotência..."
CREATE_RES=$(docker run --rm --network projeto-integrador-4_default curlimages/curl:latest -s -X POST http://api:8080/logistics.v1.LogisticsService/CreateOrder \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${MERCHANT_TOKEN}" \
  -d '{
    "customer": {"name": "idempotency-test-customer", "phone": "+5511988888888"},
    "destination": {"lat": -23.55052, "lng": -46.633308, "address": "Av Paulista, 2000"},
    "packageSize": 1,
    "items": [{"name": "item-idempotency", "quantity": 1, "unitPrice": 50.0}]
  }')

ORDER_ID=$(echo "${CREATE_RES}" | grep -o '"orderId":"[^"]*' | cut -d'"' -f4)
echo "   Pedido criado com sucesso: ${ORDER_ID}"

# 3. Login Entregador
echo "3. Autenticando entregador..."
DRIVER_TOKEN=$(docker run --rm --network projeto-integrador-4_default curlimages/curl:latest -s -X POST http://api:8080/auth.v1.AuthService/Login \
  -H "Content-Type: application/json" \
  -d '{"email":"entregador@elo.com","password":"Entregador@123!"}' | grep -o '"accessToken":"[^"]*' | cut -d'"' -f4)

# 4. Aceitar Pedido (Gera DeliveryCreated no Outbox)
echo "4. Entregador aceita pedido (gerando DeliveryCreated)..."
ACCEPT_RES=$(docker run --rm --network projeto-integrador-4_default curlimages/curl:latest -s -X POST http://api:8080/logistics.v1.LogisticsService/AcceptDelivery \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${DRIVER_TOKEN}" \
  -d "{\"orderId\":\"${ORDER_ID}\"}")

echo "   Resultado AcceptDelivery: ${ACCEPT_RES}"
echo "   Aguardando 2s para publicação do Outbox e projeção inicial..."
sleep 2

# 5. Ler Estado Inicial na Projeção e Obter o Envelope AMQP Publicado
echo "5. Verificando estado inicial no read model e coletando envelope do Outbox..."
INITIAL_STATE=$(docker exec -i logistics-postgres psql -U root -d logistics -t -A -c "
SELECT state || '|' || sequence FROM delivery_read_model WHERE delivery_id = '${ORDER_ID}';
")
echo "   Estado inicial no delivery_read_model: ${INITIAL_STATE}"

OUTBOX_DATA=$(docker exec -i logistics-postgres psql -U root -d logistics -t -A -c "
SELECT stream_position || '###' || payload FROM outbox_events 
WHERE aggregate_id = '${ORDER_ID}' AND event_type = 'DeliveryCreated' LIMIT 1;
")

STREAM_POS=$(echo "${OUTBOX_DATA}" | awk -F'###' '{print $1}')
RAW_PAYLOAD=$(echo "${OUTBOX_DATA}" | awk -F'###' '{print $2}')

echo "   StreamPosition: ${STREAM_POS}"
echo "   Payload: ${RAW_PAYLOAD}"

# 6. Simular Redelivery da Mesma Mensagem (Idêntica) via RabbitMQ
echo "6. Publicando mensagem IDÊNTICA no RabbitMQ para simular redelivery pós-crash/timeout..."
AMQP_BODY=$(jq -n \
  --arg agg "${ORDER_ID}" \
  --argjson pos "${STREAM_POS}" \
  --arg ev "DeliveryCreated" \
  --argjson payload "${RAW_PAYLOAD}" \
  '{aggregate_id: $agg, stream_position: $pos, event_type: $ev, payload: $payload}')

# Publicar via rabbitmqadmin com credenciais
docker exec -i logistics-rabbitmq rabbitmqadmin -u root -p password publish message \
  -e "logistics.events" \
  -k "DeliveryCreated" \
  -m "${AMQP_BODY}"

echo "   Mensagem retransmitida com sucesso. Aguardando 2s pelo processamento do consumidor..."
sleep 2

# 7. Auditoria Pós-Redelivery
echo "7. Auditoria do comportamento pós-redelivery:"
FINAL_STATE=$(docker exec -i logistics-postgres psql -U root -d logistics -t -A -c "
SELECT state || '|' || sequence FROM delivery_read_model WHERE delivery_id = '${ORDER_ID}';
")
ROW_COUNT=$(docker exec -i logistics-postgres psql -U root -d logistics -t -A -c "
SELECT count(*) FROM delivery_read_model WHERE delivery_id = '${ORDER_ID}';
")
DLQ_COUNT=$(docker exec -i logistics-rabbitmq rabbitmqctl list_queues name messages --formatter json | jq -r '.[] | select(.name=="api.delivery_projection.dlq") | .messages')
PROJ_COUNT=$(docker exec -i logistics-rabbitmq rabbitmqctl list_queues name messages --formatter json | jq -r '.[] | select(.name=="api.delivery_projection") | .messages')

echo "   - Total de registros em delivery_read_model: ${ROW_COUNT} (Esperado: 1)"
echo "   - Estado final: ${FINAL_STATE} (Esperado: igual ao inicial ${INITIAL_STATE})"
echo "   - Mensagens restantes na fila de projeção: ${PROJ_COUNT} (Esperado: 0 - mensagem consumida com ACK)"
echo "   - Mensagens na DLQ: ${DLQ_COUNT} (Esperado: 0 - sem descarte para DLQ)"

if [ "${ROW_COUNT}" -eq 1 ] && [ "${FINAL_STATE}" = "${INITIAL_STATE}" ] && [ "${PROJ_COUNT}" -eq 0 ] && [ "${DLQ_COUNT}" -eq 0 ]; then
  echo "✅ IDEMPOTÊNCIA VALIDADA: Redelivery processada com sucesso sem duplicar registro, sem regredir estado e com ACK limpo."
else
  echo "❌ FALHA NA IDEMPOTÊNCIA!"
  exit 1
fi
