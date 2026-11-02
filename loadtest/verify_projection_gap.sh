#!/usr/bin/env bash
set -euo pipefail

echo "=== 1. Login Lojista ==="
MERCHANT_TOKEN=$(docker run --rm --network projeto-integrador-4_default curlimages/curl:latest -s -X POST http://api:8080/auth.v1.AuthService/Login \
  -H "Content-Type: application/json" \
  -d '{"email":"loja@elo.com","password":"Merchant@123!"}' | grep -o '"accessToken":"[^"]*' | cut -d'"' -f4)

echo "=== 2. Criar Pedido de Verificação de Gap ==="
CREATE_RES=$(docker run --rm --network projeto-integrador-4_default curlimages/curl:latest -s -X POST http://api:8080/logistics.v1.LogisticsService/CreateOrder \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${MERCHANT_TOKEN}" \
  -d '{
    "customer": {"name": "gap-verification-test-customer", "phone": "+5511999999999"},
    "destination": {"lat": -23.55052, "lng": -46.633308, "address": "Av Paulista, 1000"},
    "packageSize": 1,
    "items": [{"name": "item-gap-test", "quantity": 1, "unitPrice": 15.0}]
  }')

ORDER_ID=$(echo "${CREATE_RES}" | grep -o '"orderId":"[^"]*' | cut -d'"' -f4)
echo "Pedido criado: ${ORDER_ID}"

echo "=== 3. Login Entregador ==="
DRIVER_TOKEN=$(docker run --rm --network projeto-integrador-4_default curlimages/curl:latest -s -X POST http://api:8080/auth.v1.AuthService/Login \
  -H "Content-Type: application/json" \
  -d '{"email":"gate13_driver_1@elo.com","password":"Entregador@123!"}' | grep -o '"accessToken":"[^"]*' | cut -d'"' -f4)

echo "=== 4. Entregador Aceita Pedido (Gera DeliveryCreated) ==="
ACCEPT_RES=$(docker run --rm --network projeto-integrador-4_default curlimages/curl:latest -s -X POST http://api:8080/logistics.v1.LogisticsService/AcceptDelivery \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${DRIVER_TOKEN}" \
  -d "{\"orderId\":\"${ORDER_ID}\"}")

echo "Resultado AcceptDelivery: ${ACCEPT_RES}"

echo "=== 5. Aguardando 2s para publicação do Outbox e consumo da Projeção ==="
sleep 2

echo "=== 6. Auditoria no PostgreSQL ==="
docker exec -i logistics-postgres psql -U root -d logistics -c "
-- Stream Position do evento DeliveryCreated gerado
SELECT aggregate_id, event_type, stream_position, status
FROM outbox_events
WHERE aggregate_id = '${ORDER_ID}';

-- Checkpoint atualizado da projeção
SELECT projection_name, last_sequence
FROM projection_checkpoints
WHERE projection_name = 'DeliveryProjection';

-- Registro correspondente em delivery_read_model
SELECT delivery_id, driver_id, state, sequence
FROM delivery_read_model
WHERE delivery_id = '${ORDER_ID}';
"

echo "=== 7. Estado das Filas no RabbitMQ ==="
docker exec -i logistics-rabbitmq rabbitmqctl list_queues name messages messages_ready messages_unacknowledged
