package rabbitmq_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/dvasconcellos/elo/api/internal/db"
	"github.com/dvasconcellos/elo/api/internal/delivery"
	"github.com/dvasconcellos/elo/api/internal/postgres"
	"github.com/dvasconcellos/elo/api/internal/rabbitmq"
	"github.com/dvasconcellos/elo/api/internal/stream"
)

// -----------------------------------------------------------------------------
// GATE 1.5B-4B: TESTES DE INTEGRAÇÃO COM RABBITMQ E POSTGRESQL REAIS
// -----------------------------------------------------------------------------

// 1. Identity conflict → permanent → DLQ
func TestGate15B4B_1_IdentityConflict_GoesToDLQ(t *testing.T) {
	dbConn := setupGate15B3DB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	delID := uuid.New()
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b4b-ident-%s", delID))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	validator := stream.NewSequenceValidator()
	rec := stream.NewGapRecovery(postgres.NewEventStore(db.New(dbConn)), validator, delivery.NewProjection(dbConn, "DeliveryProjection"), nil)
	streamConsumer := stream.NewConsumer(validator, rec, delivery.NewProjection(dbConn, "DeliveryProjection"), nil)
	rabbitConsumer := rabbitmq.NewConsumer(topo.ch, streamConsumer)

	// Inserir registro com event_id = "evt-15b4b-1" e payload original
	ev1 := delivery.EventDeliveryCreated{
		ID:         "evt-15b4b-1",
		DeliveryID: delID,
		DriverID:   "driver-1",
		Seq:        1,
		PickupLat:  -23.55,
		PickupLng:  -46.63,
		DropoffLat: -23.56,
		DropoffLng: -46.64,
	}
	p1, _ := json.Marshal(ev1)
	env1, _ := json.Marshal(map[string]interface{}{
		"event_id":        "evt-15b4b-1",
		"aggregate_id":    delID,
		"stream_position": int64(10),
		"event_type":      "DeliveryCreated",
		"payload":         json.RawMessage(p1),
	})

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: env1})
	msgs, _ := topo.ch.Consume(topo.queueName, "c1", false, false, false, false, nil)
	d1 := <-msgs
	if err := rabbitConsumer.HandleDelivery(context.Background(), d1); err != nil {
		t.Fatalf("primeira entrega legítima falhou: %v", err)
	}

	// Segundo evento: mesmo event_id ("evt-15b4b-1"), mas com payload adulterado (driver diferente)
	ev2 := delivery.EventDeliveryCreated{
		ID:         "evt-15b4b-1",
		DeliveryID: delID,
		DriverID:   "driver-ADULTERADO",
		Seq:        1,
		PickupLat:  -23.55,
		PickupLng:  -46.63,
		DropoffLat: -23.56,
		DropoffLng: -46.64,
	}
	p2, _ := json.Marshal(ev2)
	env2, _ := json.Marshal(map[string]interface{}{
		"event_id":        "evt-15b4b-1",
		"aggregate_id":    delID,
		"stream_position": int64(11),
		"event_type":      "DeliveryCreated",
		"payload":         json.RawMessage(p2),
	})

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: env2})
	d2 := <-msgs
	err := rabbitConsumer.HandleDelivery(context.Background(), d2)
	if err == nil {
		t.Fatal("esperava erro por conflito de identidade, obteve nil")
	}

	// Confirmar que foi para DLQ
	dlqCount := waitForDLQCount(t, topo.ch, topo.dlqName, 1, 2*time.Second)
	if dlqCount != 1 {
		t.Fatalf("esperava 1 mensagem na DLQ por conflito de identidade, obteve %d", dlqCount)
	}
	t.Log("Confirmação: Identity Conflict classificado como Permanent e roteado para DLQ.")
}

// 2. Causal conflict (uq_processed_events_consumer_agg_seq) → permanent → DLQ
func TestGate15B4B_2_CausalConflict_GoesToDLQ(t *testing.T) {
	dbConn := setupGate15B3DB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	delID := uuid.New()
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b4b-causal-%s", delID))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	validator := stream.NewSequenceValidator()
	rec := stream.NewGapRecovery(postgres.NewEventStore(db.New(dbConn)), validator, delivery.NewProjection(dbConn, "DeliveryProjection"), nil)
	streamConsumer := stream.NewConsumer(validator, rec, delivery.NewProjection(dbConn, "DeliveryProjection"), nil)
	rabbitConsumer := rabbitmq.NewConsumer(topo.ch, streamConsumer)

	// Primeiro evento: seq=1 com event_id = "evt-seq1-A"
	ev1 := delivery.EventDeliveryCreated{ID: "evt-seq1-A", DeliveryID: delID, DriverID: "driver-1", Seq: 1, PickupLat: -23.55, PickupLng: -46.63, DropoffLat: -23.56, DropoffLng: -46.64}
	p1, _ := json.Marshal(ev1)
	env1, _ := json.Marshal(map[string]interface{}{"event_id": "evt-seq1-A", "aggregate_id": delID, "stream_position": int64(20), "event_type": "DeliveryCreated", "payload": json.RawMessage(p1)})

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: env1})
	msgs, _ := topo.ch.Consume(topo.queueName, "c1", false, false, false, false, nil)
	d1 := <-msgs
	if err := rabbitConsumer.HandleDelivery(context.Background(), d1); err != nil {
		t.Fatalf("primeira entrega legítima falhou: %v", err)
	}

	// Segundo evento: seq=1, mas com event_id diferente = "evt-seq1-B" (disputa pelo slot causal 1)
	ev2 := delivery.EventDeliveryCreated{ID: "evt-seq1-B", DeliveryID: delID, DriverID: "driver-2", Seq: 1, PickupLat: -23.55, PickupLng: -46.63, DropoffLat: -23.56, DropoffLng: -46.64}
	p2, _ := json.Marshal(ev2)
	env2, _ := json.Marshal(map[string]interface{}{"event_id": "evt-seq1-B", "aggregate_id": delID, "stream_position": int64(21), "event_type": "DeliveryCreated", "payload": json.RawMessage(p2)})

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: env2})
	d2 := <-msgs
	err := rabbitConsumer.HandleDelivery(context.Background(), d2)
	if err == nil {
		t.Fatal("esperava erro por conflito causal, obteve nil")
	}

	// Confirmar que foi para DLQ
	dlqCount := waitForDLQCount(t, topo.ch, topo.dlqName, 1, 2*time.Second)
	if dlqCount != 1 {
		t.Fatalf("esperava 1 mensagem na DLQ por conflito causal, obteve %d", dlqCount)
	}
	t.Log("Confirmação: Causal Conflict classificado como Permanent e roteado para DLQ.")
}

// 3. Outra constraint 23505 → unknown → preservado na fila (NÃO vai para DLQ)
func TestGate15B4B_3_Other23505Constraint_Unknown_PreservedInQueue_NotDLQ(t *testing.T) {
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b4b-other23505-%s", uuid.New().String()))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	// Mock StreamProcessor que retorna pq.Error com código 23505 em constraint NÃO-causal
	mockProc := &mockStreamProcessor{
		processFunc: func(ctx context.Context, ev stream.Event) error {
			return fmt.Errorf("read model insert failed: %w", &pq.Error{
				Code:       "23505",
				Constraint: "delivery_read_model_pkey",
				Message:    "duplicate key value violates unique constraint delivery_read_model_pkey",
			})
		},
	}
	rabbitConsumer := rabbitmq.NewConsumer(topo.ch, mockProc)

	delID := uuid.New()
	ev := delivery.EventDeliveryCreated{ID: "evt-noncausal-1", DeliveryID: delID, DriverID: "driver-1", Seq: 1, PickupLat: -23.55, PickupLng: -46.63, DropoffLat: -23.56, DropoffLng: -46.64}
	p, _ := json.Marshal(ev)
	envBytes, _ := json.Marshal(map[string]interface{}{"event_id": "evt-noncausal-1", "aggregate_id": delID, "stream_position": int64(30), "event_type": "DeliveryCreated", "payload": json.RawMessage(p)})

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envBytes})
	msgs, _ := topo.ch.Consume(topo.queueName, "c1", false, false, false, false, nil)
	d := <-msgs

	err := rabbitConsumer.HandleDelivery(context.Background(), d)
	if err == nil {
		t.Fatal("esperava erro de retorno, obteve nil")
	}

	if !rabbitConsumer.IsSuspended() {
		t.Fatal("esperava consumidor suspenso após erro desconhecido")
	}

	// Confirmar que ZERO mensagens foram para a DLQ
	qDLQ, _ := topo.ch.QueueInspect(topo.dlqName)
	if qDLQ.Messages != 0 {
		t.Fatalf("esperava 0 mensagens na DLQ para constraint 23505 não-causal, obteve %d", qDLQ.Messages)
	}

	// Fechar o canal para que o broker libere a mensagem unacked de volta à fila
	_ = topo.ch.Close()
	ch2, err := topo.conn.Channel()
	if err != nil {
		t.Fatalf("falha ao abrir canal 2: %v", err)
	}
	defer ch2.Close()

	msgs2, err := ch2.Consume(topo.queueName, "c2", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("falha ao consumir canal 2: %v", err)
	}

	select {
	case d2 := <-msgs2:
		if !d2.Redelivered {
			t.Fatalf("esperava que a mensagem reentregue tivesse Redelivered=true")
		}
		_ = d2.Ack(false)
		t.Log("Confirmação: Violação de 23505 não-causal classificada como Unknown e PRESERVADA na fila (sem DLQ, recuperada após reconexão).")
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout aguardando reentrega da mensagem após fechamento do canal")
	}
	_ = ch2.Cancel("c2", false)
}

// 4. Falha operacional transitória → transient → recuperada por in-place retry (NÃO vai para DLQ)
func TestGate15B4B_4_TransientError_PreservedInQueue_NotDLQ(t *testing.T) {
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b4b-transient-%s", uuid.New().String()))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	attempts := 0
	// Mock retornando erro de deadlock 40P01 na 1ª tentativa e sucesso na 2ª
	mockProc := &mockStreamProcessor{
		processFunc: func(ctx context.Context, ev stream.Event) error {
			attempts++
			if attempts == 1 {
				return fmt.Errorf("transaction aborted: %w", &pq.Error{
					Code:    "40P01",
					Message: "deadlock detected",
				})
			}
			return nil
		},
	}
	fastBackoff := rabbitmq.NewBackoffCalculator(10*time.Millisecond, 50*time.Millisecond, nil)
	rabbitConsumer := rabbitmq.NewConsumer(topo.ch, mockProc, rabbitmq.WithConsumerBackoff(fastBackoff))

	delID := uuid.New()
	ev := delivery.EventDeliveryCreated{ID: "evt-transient-1", DeliveryID: delID, DriverID: "driver-1", Seq: 1, PickupLat: -23.55, PickupLng: -46.63, DropoffLat: -23.56, DropoffLng: -46.64}
	p, _ := json.Marshal(ev)
	envBytes, _ := json.Marshal(map[string]interface{}{"event_id": "evt-transient-1", "aggregate_id": delID, "stream_position": int64(40), "event_type": "DeliveryCreated", "payload": json.RawMessage(p)})

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envBytes})
	msgs, _ := topo.ch.Consume(topo.queueName, "c1", false, false, false, false, nil)
	d := <-msgs

	err := rabbitConsumer.HandleDelivery(context.Background(), d)
	if err != nil {
		t.Fatalf("esperava sucesso após in-place retry, obteve: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("esperava 2 tentativas, obteve %d", attempts)
	}

	// Zero na DLQ
	qDLQ, _ := topo.ch.QueueInspect(topo.dlqName)
	if qDLQ.Messages != 0 {
		t.Fatalf("esperava 0 mensagens na DLQ para falha transitória, obteve %d", qDLQ.Messages)
	}
	t.Log("Confirmação: Erro transitório (40P01) recuperado com in-place retry e ACK emitido com sucesso (zero DLQ).")
	_ = topo.ch.Cancel("c1", false)
}

// 5. Redelivered = true NÃO envia para DLQ se o erro for transitório (Eliminação do critério Redelivered)
func TestGate15B4B_5_RedeliveredTrue_DoesNotForceDLQ(t *testing.T) {
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b4b-redeliv-%s", uuid.New().String()))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	attempts := 0
	// Mock retornando falha de conexão 08006 na 1ª tentativa e sucesso na 2ª
	mockProc := &mockStreamProcessor{
		processFunc: func(ctx context.Context, ev stream.Event) error {
			attempts++
			if attempts == 1 {
				return fmt.Errorf("connection reset: %w", &pq.Error{
					Code:    "08006",
					Message: "connection failure",
				})
			}
			return nil
		},
	}
	fastBackoff := rabbitmq.NewBackoffCalculator(10*time.Millisecond, 50*time.Millisecond, nil)
	rabbitConsumer := rabbitmq.NewConsumer(topo.ch, mockProc, rabbitmq.WithConsumerBackoff(fastBackoff))

	delID := uuid.New()
	ev := delivery.EventDeliveryCreated{ID: "evt-redeliv-1", DeliveryID: delID, DriverID: "driver-1", Seq: 1, PickupLat: -23.55, PickupLng: -46.63, DropoffLat: -23.56, DropoffLng: -46.64}
	p, _ := json.Marshal(ev)
	envBytes, _ := json.Marshal(map[string]interface{}{"event_id": "evt-redeliv-1", "aggregate_id": delID, "stream_position": int64(50), "event_type": "DeliveryCreated", "payload": json.RawMessage(p)})

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envBytes})
	msgs, _ := topo.ch.Consume(topo.queueName, "c1", false, false, false, false, nil)
	d := <-msgs

	// Forçar a flag Redelivered = true na delivery simulando reentrega pós-crash anterior
	d.Redelivered = true

	// No código legado (Gate 1.5B-3): if isUnrecoverable || d.Redelivered mandava para DLQ!
	// No código Gate 1.5B-4B/4C: o classificador decide; como 08006 é Transient, JAMAIS vai para DLQ!
	err := rabbitConsumer.HandleDelivery(context.Background(), d)
	if err != nil {
		t.Fatalf("esperava sucesso após in-place retry com Redelivered=true, obteve: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("esperava 2 tentativas, obteve %d", attempts)
	}

	// DLQ deve permanecer ZERADA
	qDLQ, _ := topo.ch.QueueInspect(topo.dlqName)
	if qDLQ.Messages != 0 {
		t.Fatalf("REGRESSÃO CRÍTICA: Mensagem com Redelivered=true e falha transitória foi enviada indevidamente à DLQ! messages=%d", qDLQ.Messages)
	}
	t.Log("Confirmação: Mensagem com Redelivered=true e falha transitória NÃO foi descartada para DLQ. In-place retry operou normalmente.")
	_ = topo.ch.Cancel("c1", false)
}

// 6. Context Canceled (Shutdown) → contextual → preservado na fila (NÃO vai para DLQ)
func TestGate15B4B_6_ContextCanceled_PreservedInQueue_NotDLQ(t *testing.T) {
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b4b-shutdown-%s", uuid.New().String()))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	// Mock retornando context.Canceled simulando cancelamento de shutdown
	mockProc := &mockStreamProcessor{
		processFunc: func(ctx context.Context, ev stream.Event) error {
			return context.Canceled
		},
	}
	rabbitConsumer := rabbitmq.NewConsumer(topo.ch, mockProc)

	delID := uuid.New()
	ev := delivery.EventDeliveryCreated{ID: "evt-shutdown-1", DeliveryID: delID, DriverID: "driver-1", Seq: 1, PickupLat: -23.55, PickupLng: -46.63, DropoffLat: -23.56, DropoffLng: -46.64}
	p, _ := json.Marshal(ev)
	envBytes, _ := json.Marshal(map[string]interface{}{"event_id": "evt-shutdown-1", "aggregate_id": delID, "stream_position": int64(60), "event_type": "DeliveryCreated", "payload": json.RawMessage(p)})

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envBytes})
	msgs, _ := topo.ch.Consume(topo.queueName, "c1", false, false, false, false, nil)
	d := <-msgs

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	err := rabbitConsumer.HandleDelivery(canceledCtx, d)
	if err == nil {
		t.Fatal("esperava erro de context.Canceled, obteve nil")
	}

	// Zero na DLQ
	qDLQ, _ := topo.ch.QueueInspect(topo.dlqName)
	if qDLQ.Messages != 0 {
		t.Fatalf("esperava 0 mensagens na DLQ para context canceled, obteve %d", qDLQ.Messages)
	}

	// Fechar o canal para que o broker libere a mensagem unacked
	_ = topo.ch.Close()
	ch2, err := topo.conn.Channel()
	if err != nil {
		t.Fatalf("falha ao abrir canal 2: %v", err)
	}
	defer ch2.Close()

	msgs2, err := ch2.Consume(topo.queueName, "c2", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("falha ao consumir canal 2: %v", err)
	}

	select {
	case d2 := <-msgs2:
		if !d2.Redelivered {
			t.Fatalf("esperava que a mensagem reentregue tivesse Redelivered=true")
		}
		_ = d2.Ack(false)
		t.Log("Confirmação: Cancelamento de contexto classificado como ContextCanceled e PRESERVADO na fila (recuperado pós-reconexão).")
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout aguardando reentrega da mensagem após fechamento do canal")
	}
	_ = ch2.Cancel("c2", false)
}

// 7. Erro Desconhecido → unknown → consumidor suspenso e preservado na fila (NÃO vai para DLQ)
func TestGate15B4B_7_UnknownError_PreservedInQueue_NotDLQ(t *testing.T) {
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b4b-unknown-%s", uuid.New().String()))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	// Mock retornando erro completamente desconhecido
	mockProc := &mockStreamProcessor{
		processFunc: func(ctx context.Context, ev stream.Event) error {
			return errors.New("synthetic unclassified strange error")
		},
	}
	rabbitConsumer := rabbitmq.NewConsumer(topo.ch, mockProc)

	delID := uuid.New()
	ev := delivery.EventDeliveryCreated{ID: "evt-unknown-1", DeliveryID: delID, DriverID: "driver-1", Seq: 1, PickupLat: -23.55, PickupLng: -46.63, DropoffLat: -23.56, DropoffLng: -46.64}
	p, _ := json.Marshal(ev)
	envBytes, _ := json.Marshal(map[string]interface{}{"event_id": "evt-unknown-1", "aggregate_id": delID, "stream_position": int64(70), "event_type": "DeliveryCreated", "payload": json.RawMessage(p)})

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envBytes})
	msgs, _ := topo.ch.Consume(topo.queueName, "c1", false, false, false, false, nil)
	d := <-msgs

	err := rabbitConsumer.HandleDelivery(context.Background(), d)
	if err == nil {
		t.Fatal("esperava erro desconhecido, obteve nil")
	}

	if !rabbitConsumer.IsSuspended() {
		t.Fatal("esperava consumidor suspenso após erro desconhecido")
	}

	// Zero na DLQ
	qDLQ, _ := topo.ch.QueueInspect(topo.dlqName)
	if qDLQ.Messages != 0 {
		t.Fatalf("esperava 0 mensagens na DLQ para erro desconhecido, obteve %d", qDLQ.Messages)
	}

	// Fechar o canal para que o broker libere a mensagem unacked
	_ = topo.ch.Close()
	ch2, err := topo.conn.Channel()
	if err != nil {
		t.Fatalf("falha ao abrir canal 2: %v", err)
	}
	defer ch2.Close()

	msgs2, err := ch2.Consume(topo.queueName, "c2", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("falha ao consumir canal 2: %v", err)
	}

	select {
	case d2 := <-msgs2:
		if !d2.Redelivered {
			t.Fatalf("esperava que a mensagem reentregue tivesse Redelivered=true")
		}
		_ = d2.Ack(false)
		t.Log("Confirmação: Erro desconhecido classificado como Unknown e PRESERVADO na fila (sem DLQ automática, recuperado pós-reconexão).")
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout aguardando reentrega da mensagem após fechamento do canal")
	}
	_ = ch2.Cancel("c2", false)
}

// Helper mock para StreamProcessor
type mockStreamProcessor struct {
	processFunc func(ctx context.Context, ev stream.Event) error
}

func (m *mockStreamProcessor) Process(ctx context.Context, ev stream.Event) error {
	if m.processFunc != nil {
		return m.processFunc(ctx, ev)
	}
	return nil
}
