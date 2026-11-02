package rabbitmq_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/dvasconcellos/elo/api/internal/db"
	"github.com/dvasconcellos/elo/api/internal/delivery"
	"github.com/dvasconcellos/elo/api/internal/postgres"
	"github.com/dvasconcellos/elo/api/internal/rabbitmq"
	"github.com/dvasconcellos/elo/api/internal/stream"
)

// -----------------------------------------------------------------------------
// GATE 1.5B-3R: TIPO DE EVENTO DESCONHECIDO ENCAMINHADO PARA DLQ
// -----------------------------------------------------------------------------

func TestGate15B3R_UnknownEventType_GoesToDLQ(t *testing.T) {
	dbConn := setupGate15B3DB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	delID := uuid.New()
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b3r-unknown-dlq-%s", delID))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	// Mensagem com tipo desconhecido / incompatível
	envBytes, _ := json.Marshal(map[string]interface{}{
		"event_id":        "evt-unknown-999",
		"aggregate_id":    delID,
		"stream_position": int64(999001),
		"event_type":      "UnknownLegacyEventTypeV0",
		"payload":         json.RawMessage(`{"some_field": "val"}`),
	})

	validator := stream.NewSequenceValidator()
	streamConsumer := stream.NewConsumer(validator, nil, delivery.NewProjection(dbConn, "DeliveryProjection"), nil)
	rabbitConsumer := rabbitmq.NewConsumer(topo.ch, streamConsumer)

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envBytes})
	msgChan, err := topo.ch.Consume(topo.queueName, "c1", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume: %v", err)
	}
	d := <-msgChan

	err = rabbitConsumer.HandleDelivery(context.Background(), d)

	// 1. Deve classificar como ErrPoisonMessage e retornar erro
	if err == nil {
		t.Fatal("esperava erro ao processar event_type desconhecido, obteve nil")
	}
	t.Logf("HandleDelivery retornou erro esperado: %v", err)

	// 2. Mensagem removida da fila principal (não fica presa nem em loop)
	time.Sleep(50 * time.Millisecond)
	qMain, _ := topo.ch.QueueInspect(topo.queueName)
	if qMain.Messages != 0 {
		t.Fatalf("esperava 0 mensagens na fila principal, obteve %d", qMain.Messages)
	}
	t.Log("Confirmação: Mensagem removida da fila principal.")

	// 3. Mensagem encaminhada à DLQ pelo broker (Reject com requeue=false)
	dlqCount := waitForDLQCount(t, topo.ch, topo.dlqName, 1, 2*time.Second)
	if dlqCount != 1 {
		t.Fatalf("esperava exatamente 1 mensagem na DLQ, obteve %d", dlqCount)
	}
	t.Log("Confirmação: Mensagem encaminhada com sucesso para a DLQ.")

	// 4. Nenhum registro em processed_events
	var processedCount int
	_ = dbConn.QueryRow("SELECT count(*) FROM gate15b3_test.processed_events WHERE event_id = 'evt-unknown-999'").Scan(&processedCount)
	if processedCount != 0 {
		t.Fatalf("esperava 0 registros em processed_events, obteve %d", processedCount)
	}
	t.Log("Confirmação: Nenhum registro em processed_events.")

	// 5. Nenhuma alteração no read model
	var rmCount int
	_ = dbConn.QueryRow("SELECT count(*) FROM gate15b3_test.delivery_read_model WHERE delivery_id = $1", delID).Scan(&rmCount)
	if rmCount != 0 {
		t.Fatalf("esperava 0 registros em delivery_read_model, obteve %d", rmCount)
	}
	t.Log("Confirmação: Nenhuma alteração no read model.")

	// 6. Nenhum avanço indevido do checkpoint
	var cpCount int
	_ = dbConn.QueryRow("SELECT count(*) FROM gate15b3_test.projection_checkpoints WHERE projection_name = 'DeliveryProjection'").Scan(&cpCount)
	if cpCount != 0 {
		t.Fatalf("esperava 0 checkpoints gravados, obteve %d", cpCount)
	}
	t.Log("Confirmação: Nenhum avanço indevido do checkpoint.")
	t.Log("=== TESTE TestGate15B3R_UnknownEventType_GoesToDLQ APROVADO ===")
}

// -----------------------------------------------------------------------------
// AUDITORIA PONTO 3: CHECKPOINT GLOBAL COM 2 AGREGADOS E POSIÇÕES INTERCALADAS
// -----------------------------------------------------------------------------

func TestAudit_Gate15B3R_InterleavedAggregatesAndGlobalCheckpoint(t *testing.T) {
	dbConn := setupGate15B3DB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	delA := uuid.New()
	delB := uuid.New()
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b3r-audit-interleaved-%s", delA))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	validator := stream.NewSequenceValidator()
	rec := stream.NewGapRecovery(postgres.NewEventStore(db.New(dbConn)), validator, delivery.NewProjection(dbConn, "DeliveryProjection"), nil)
	streamConsumer := stream.NewConsumer(validator, rec, delivery.NewProjection(dbConn, "DeliveryProjection"), nil)
	rabbitConsumer := rabbitmq.NewConsumer(topo.ch, streamConsumer)

	// Eventos intercalados:
	// Pos 100: Del A, Seq 1 (Created)
	// Pos 101: Del B, Seq 1 (Created)
	// Pos 102: Del A, Seq 2 (DriverArrivedAtPickup)
	// Pos 103: Del B, Seq 2 (DriverArrivedAtPickup)

	evA1 := delivery.EventDeliveryCreated{ID: "evt-a1", DeliveryID: delA, DriverID: "driver-a", Seq: 1, PickupLat: -23.55, PickupLng: -46.63, DropoffLat: -23.56, DropoffLng: -46.64}
	pA1, _ := json.Marshal(evA1)
	envA1, _ := json.Marshal(map[string]interface{}{"event_id": "evt-a1", "aggregate_id": delA, "stream_position": int64(100), "event_type": "DeliveryCreated", "payload": json.RawMessage(pA1)})

	evB1 := delivery.EventDeliveryCreated{ID: "evt-b1", DeliveryID: delB, DriverID: "driver-b", Seq: 1, PickupLat: -23.55, PickupLng: -46.63, DropoffLat: -23.56, DropoffLng: -46.64}
	pB1, _ := json.Marshal(evB1)
	envB1, _ := json.Marshal(map[string]interface{}{"event_id": "evt-b1", "aggregate_id": delB, "stream_position": int64(101), "event_type": "DeliveryCreated", "payload": json.RawMessage(pB1)})

	evA2 := delivery.EventDriverArrivedAtPickup{ID: "evt-a2", DeliveryID: delA, Seq: 2}
	pA2, _ := json.Marshal(evA2)
	envA2, _ := json.Marshal(map[string]interface{}{"event_id": "evt-a2", "aggregate_id": delA, "stream_position": int64(102), "event_type": "DriverArrivedAtPickup", "payload": json.RawMessage(pA2)})

	evB2 := delivery.EventDriverArrivedAtPickup{ID: "evt-b2", DeliveryID: delB, Seq: 2}
	pB2, _ := json.Marshal(evB2)
	envB2, _ := json.Marshal(map[string]interface{}{"event_id": "evt-b2", "aggregate_id": delB, "stream_position": int64(103), "event_type": "DriverArrivedAtPickup", "payload": json.RawMessage(pB2)})

	// Simular entrega fora de ordem global:
	// Entregar Pos 101 (Del B, Seq 1) ANTES de Pos 100 (Del A, Seq 1)!
	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envB1}) // pos 101
	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envA1}) // pos 100
	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envA2}) // pos 102
	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envB2}) // pos 103

	msgChan, err := topo.ch.Consume(topo.queueName, "c1", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume: %v", err)
	}

	// 1. Processar Pos 101 (Del B, Seq 1)
	d1 := <-msgChan
	if err := rabbitConsumer.HandleDelivery(context.Background(), d1); err != nil {
		t.Fatalf("falha ao processar B1 (pos 101): %v", err)
	}

	var cp1 int64
	_ = dbConn.QueryRow("SELECT last_sequence FROM gate15b3_test.projection_checkpoints WHERE projection_name = 'DeliveryProjection'").Scan(&cp1)
	t.Logf("Após processar Pos 101 (B1): checkpoint = %d", cp1)
	if cp1 != 101 {
		t.Fatalf("esperava checkpoint 101, obteve %d", cp1)
	}

	// 2. Processar Pos 100 (Del A, Seq 1) — posição menor que o checkpoint atual (100 < 101)!
	d2 := <-msgChan
	if err := rabbitConsumer.HandleDelivery(context.Background(), d2); err != nil {
		t.Fatalf("falha ao processar A1 (pos 100): %v", err)
	}

	var cp2 int64
	_ = dbConn.QueryRow("SELECT last_sequence FROM gate15b3_test.projection_checkpoints WHERE projection_name = 'DeliveryProjection'").Scan(&cp2)
	t.Logf("Após processar Pos 100 (A1): checkpoint = %d (não regrediu)", cp2)
	if cp2 != 101 {
		t.Fatalf("checkpoint regrediu! esperava 101 mantido, obteve %d", cp2)
	}

	// Verificar que Del A foi aplicada perfeitamente no read model apesar da posição menor
	var rmSeqA int64
	_ = dbConn.QueryRow("SELECT sequence FROM gate15b3_test.delivery_read_model WHERE delivery_id = $1", delA).Scan(&rmSeqA)
	if rmSeqA != 1 {
		t.Fatalf("read model de A não foi aplicado: seq=%d", rmSeqA)
	}

	// 3. Processar Pos 102 (Del A, Seq 2)
	d3 := <-msgChan
	if err := rabbitConsumer.HandleDelivery(context.Background(), d3); err != nil {
		t.Fatalf("falha ao processar A2 (pos 102): %v", err)
	}

	var cp3 int64
	_ = dbConn.QueryRow("SELECT last_sequence FROM gate15b3_test.projection_checkpoints WHERE projection_name = 'DeliveryProjection'").Scan(&cp3)
	t.Logf("Após processar Pos 102 (A2): checkpoint = %d", cp3)
	if cp3 != 102 {
		t.Fatalf("esperava checkpoint 102, obteve %d", cp3)
	}

	// 4. Processar Pos 103 (Del B, Seq 2)
	d4 := <-msgChan
	if err := rabbitConsumer.HandleDelivery(context.Background(), d4); err != nil {
		t.Fatalf("falha ao processar B2 (pos 103): %v", err)
	}

	var cp4 int64
	_ = dbConn.QueryRow("SELECT last_sequence FROM gate15b3_test.projection_checkpoints WHERE projection_name = 'DeliveryProjection'").Scan(&cp4)
	t.Logf("Após processar Pos 103 (B2): checkpoint = %d", cp4)
	if cp4 != 103 {
		t.Fatalf("esperava checkpoint 103, obteve %d", cp4)
	}

	// Validação final de consistência de ambos os read models
	var stateA, stateB string
	var seqA, seqB int64
	_ = dbConn.QueryRow("SELECT sequence, state FROM gate15b3_test.delivery_read_model WHERE delivery_id = $1", delA).Scan(&seqA, &stateA)
	_ = dbConn.QueryRow("SELECT sequence, state FROM gate15b3_test.delivery_read_model WHERE delivery_id = $1", delB).Scan(&seqB, &stateB)

	if seqA != 2 || stateA != "AT_PICKUP" || seqB != 2 || stateB != "AT_PICKUP" {
		t.Fatalf("estado final corrompido: A=(seq=%d, state=%s), B=(seq=%d, state=%s)", seqA, stateA, seqB, stateB)
	}

	t.Logf("AUDITORIA CHECKPOINT CONCLUÍDA COM SUCESSO: Del A=(seq=%d, %s), Del B=(seq=%d, %s), Checkpoint final=%d", seqA, stateA, seqB, stateB, cp4)
}
