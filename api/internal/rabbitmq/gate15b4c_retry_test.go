package rabbitmq_test

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
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
// GATE 1.5B-4C: TESTES DE INTEGRAÇÃO COM RABBITMQ E POSTGRESQL REAIS
// -----------------------------------------------------------------------------

// 1. In-place retry com recuperação após falha transitória (ACK no final)
func TestGate15B4C_1_InPlaceRetry_TransientError_SuccessAfterBackoff(t *testing.T) {
	dbConn := setupGate15B3DB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	delID := uuid.New()
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b4c-retry1-%s", delID))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	// Interceptor no StreamProcessor para simular falha transitória na 1ª tentativa
	// e delegar para o pipeline real na 2ª tentativa
	validator := stream.NewSequenceValidator()
	proj := delivery.NewProjection(dbConn, "DeliveryProjection")
	rec := stream.NewGapRecovery(postgres.NewEventStore(db.New(dbConn)), validator, proj, nil)
	realStreamConsumer := stream.NewConsumer(validator, rec, proj, nil)

	attempts := 0
	flakyProcessor := &mockStreamProcessor{
		processFunc: func(ctx context.Context, ev stream.Event) error {
			attempts++
			if attempts == 1 {
				// Simula falha transitória de conexão / pool
				return driver.ErrBadConn
			}
			return realStreamConsumer.Process(ctx, ev)
		},
	}

	fastBackoff := rabbitmq.NewBackoffCalculator(10*time.Millisecond, 50*time.Millisecond, nil)
	rabbitConsumer := rabbitmq.NewConsumer(topo.ch, flakyProcessor, rabbitmq.WithConsumerBackoff(fastBackoff))

	ev := delivery.EventDeliveryCreated{
		ID:         "evt-15b4c-1",
		DeliveryID: delID,
		DriverID:   "driver-retry-1",
		Seq:        1,
		PickupLat:  -23.55,
		PickupLng:  -46.63,
		DropoffLat: -23.56,
		DropoffLng: -46.64,
	}
	p, _ := json.Marshal(ev)
	envBytes, _ := json.Marshal(map[string]interface{}{
		"event_id":        "evt-15b4c-1",
		"aggregate_id":    delID,
		"stream_position": int64(101),
		"event_type":      "DeliveryCreated",
		"payload":         json.RawMessage(p),
	})

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envBytes})
	msgs, _ := topo.ch.Consume(topo.queueName, "c1", false, false, false, false, nil)
	d := <-msgs

	err := rabbitConsumer.HandleDelivery(context.Background(), d)
	if err != nil {
		t.Fatalf("esperava sucesso após in-place retry, obteve: %v", err)
	}

	if attempts != 2 {
		t.Fatalf("esperava 2 tentativas de processamento (1 falha + 1 sucesso), obteve %d", attempts)
	}

	// Verificar persistência no banco real
	var count int
	err = dbConn.QueryRow("SELECT COUNT(*) FROM gate15b3_test.processed_events WHERE event_id = $1", "evt-15b4c-1").Scan(&count)
	if err != nil || count != 1 {
		t.Fatalf("esperava 1 registro em processed_events após retry bem-sucedido, obteve %d (err: %v)", count, err)
	}

	// Verificar DLQ zerada
	qDLQ, _ := topo.ch.QueueInspect(topo.dlqName)
	if qDLQ.Messages != 0 {
		t.Fatalf("esperava 0 mensagens na DLQ, obteve %d", qDLQ.Messages)
	}
	_ = topo.ch.Cancel("c1", false)
}

// 2. Falhas transitórias consecutivas com progressão de atraso
func TestGate15B4C_2_ConsecutiveTransientErrors_DelayProgression(t *testing.T) {
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b4c-prog-%s", uuid.New().String()))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	var mu sync.Mutex
	var delaysRecorded []time.Duration
	mockSleeper := func(ctx context.Context, d time.Duration) error {
		mu.Lock()
		delaysRecorded = append(delaysRecorded, d)
		mu.Unlock()
		return nil
	}

	attempts := 0
	mockProc := &mockStreamProcessor{
		processFunc: func(ctx context.Context, ev stream.Event) error {
			attempts++
			if attempts <= 3 {
				// Falha nas tentativas 1, 2, 3 (attempts 0, 1, 2 no loop)
				return fmt.Errorf("temporary lock timeout: %w", &pq.Error{
					Code:    "55P03",
					Message: "lock not available",
				})
			}
			return nil
		},
	}

	// Deterministic RNG retornando 100% do teto para testar progressão do teto
	deterministicBackoff := rabbitmq.NewBackoffCalculator(100*time.Millisecond, 5*time.Second, &deterministicRNG{fraction: 1.0})
	rabbitConsumer := rabbitmq.NewConsumer(topo.ch, mockProc,
		rabbitmq.WithConsumerBackoff(deterministicBackoff),
		rabbitmq.WithConsumerSleeper(mockSleeper),
	)

	delID := uuid.New()
	ev := delivery.EventDeliveryCreated{ID: "evt-15b4c-2", DeliveryID: delID, DriverID: "driver-1", Seq: 1, PickupLat: -23.55, PickupLng: -46.63, DropoffLat: -23.56, DropoffLng: -46.64}
	p, _ := json.Marshal(ev)
	envBytes, _ := json.Marshal(map[string]interface{}{"event_id": "evt-15b4c-2", "aggregate_id": delID, "stream_position": int64(102), "event_type": "DeliveryCreated", "payload": json.RawMessage(p)})

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envBytes})
	msgs, _ := topo.ch.Consume(topo.queueName, "c1", false, false, false, false, nil)
	d := <-msgs

	err := rabbitConsumer.HandleDelivery(context.Background(), d)
	if err != nil {
		t.Fatalf("esperava sucesso na 4ª tentativa, obteve: %v", err)
	}

	if attempts != 4 {
		t.Fatalf("esperava 4 tentativas, obteve %d", attempts)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(delaysRecorded) != 3 {
		t.Fatalf("esperava 3 esperas registradas, obteve %d", len(delaysRecorded))
	}

	// Tentativa 0: 100ms, Tentativa 1: 200ms, Tentativa 2: 400ms
	if delaysRecorded[0] != 100*time.Millisecond {
		t.Errorf("atraso 0 esperado 100ms, obteve %v", delaysRecorded[0])
	}
	if delaysRecorded[1] != 200*time.Millisecond {
		t.Errorf("atraso 1 esperado 200ms, obteve %v", delaysRecorded[1])
	}
	if delaysRecorded[2] != 400*time.Millisecond {
		t.Errorf("atraso 2 esperado 400ms, obteve %v", delaysRecorded[2])
	}
	_ = topo.ch.Cancel("c1", false)
}

// 3. Erro permanente: zero retentativas, encaminhamento imediato para DLQ via Reject(false)
func TestGate15B4C_3_PermanentError_ZeroRetries_ImmediateDLQ(t *testing.T) {
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b4c-perm-%s", uuid.New().String()))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	attempts := 0
	mockProc := &mockStreamProcessor{
		processFunc: func(ctx context.Context, ev stream.Event) error {
			attempts++
			return delivery.ErrCausalConflict
		},
	}

	rabbitConsumer := rabbitmq.NewConsumer(topo.ch, mockProc)

	delID := uuid.New()
	ev := delivery.EventDeliveryCreated{ID: "evt-15b4c-3", DeliveryID: delID, DriverID: "driver-1", Seq: 1, PickupLat: -23.55, PickupLng: -46.63, DropoffLat: -23.56, DropoffLng: -46.64}
	p, _ := json.Marshal(ev)
	envBytes, _ := json.Marshal(map[string]interface{}{"event_id": "evt-15b4c-3", "aggregate_id": delID, "stream_position": int64(103), "event_type": "DeliveryCreated", "payload": json.RawMessage(p)})

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envBytes})
	msgs, _ := topo.ch.Consume(topo.queueName, "c1", false, false, false, false, nil)
	d := <-msgs

	err := rabbitConsumer.HandleDelivery(context.Background(), d)
	if err == nil {
		t.Fatal("esperava erro por falha permanente, obteve nil")
	}

	if attempts != 1 {
		t.Fatalf("erro permanente não deve executar retentativas: esperava 1 tentativa, obteve %d", attempts)
	}

	// Confirmar que foi para DLQ
	dlqCount := waitForDLQCount(t, topo.ch, topo.dlqName, 1, 2*time.Second)
	if dlqCount != 1 {
		t.Fatalf("esperava 1 mensagem na DLQ, obteve %d", dlqCount)
	}
	_ = topo.ch.Cancel("c1", false)
}

// 4. Erro desconhecido: consumidor suspenso, mensagem mantida unacked, zero DLQ, novas entregas bloqueadas
func TestGate15B4C_4_UnknownError_ConsumerSuspended_DeliveryUnacked_NoDLQ(t *testing.T) {
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b4c-unk-%s", uuid.New().String()))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	attempts := 0
	mockProc := &mockStreamProcessor{
		processFunc: func(ctx context.Context, ev stream.Event) error {
			attempts++
			return errors.New("unclassified panic-like application condition")
		},
	}

	rabbitConsumer := rabbitmq.NewConsumer(topo.ch, mockProc)

	delID := uuid.New()
	ev := delivery.EventDeliveryCreated{ID: "evt-15b4c-4", DeliveryID: delID, DriverID: "driver-1", Seq: 1, PickupLat: -23.55, PickupLng: -46.63, DropoffLat: -23.56, DropoffLng: -46.64}
	p, _ := json.Marshal(ev)
	envBytes, _ := json.Marshal(map[string]interface{}{"event_id": "evt-15b4c-4", "aggregate_id": delID, "stream_position": int64(104), "event_type": "DeliveryCreated", "payload": json.RawMessage(p)})

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envBytes})
	msgs, _ := topo.ch.Consume(topo.queueName, "c1", false, false, false, false, nil)
	d := <-msgs

	err := rabbitConsumer.HandleDelivery(context.Background(), d)
	if err == nil {
		t.Fatal("esperava erro ao processar erro desconhecido, obteve nil")
	}

	if attempts != 1 {
		t.Fatalf("erro desconhecido não deve entrar em loop de retry: tentativas = %d", attempts)
	}

	if !rabbitConsumer.IsSuspended() {
		t.Fatal("esperava consumidor em estado SUSPENSO (IsSuspended == true)")
	}

	// DLQ deve permanecer zerada
	qDLQ, _ := topo.ch.QueueInspect(topo.dlqName)
	if qDLQ.Messages != 0 {
		t.Fatalf("erro desconhecido não deve ir para DLQ automaticamente; messages=%d", qDLQ.Messages)
	}

	// Tentar processar nova mensagem no mesmo consumidor suspenso: deve ser rejeitado imediatamente sem chamar target.Process
	dNext := amqp.Delivery{
		Body: envBytes,
	}
	errNext := rabbitConsumer.HandleDelivery(context.Background(), dNext)
	if errNext == nil {
		t.Fatal("esperava que entrega subsequente fosse rejeitada devido à suspensão do consumidor")
	}
	if attempts != 1 {
		t.Fatalf("target.Process não deveria ter sido invocado para consumidor suspenso; tentativas = %d", attempts)
	}
	_ = topo.ch.Cancel("c1", false)
}

// 5. Cancelamento durante o backoff: execução interrompida, mensagem preservada sem ACK
func TestGate15B4C_5_ContextCancellation_DuringBackoff_PreservedUnacked(t *testing.T) {
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b4c-cancel-%s", uuid.New().String()))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	ctx, cancel := context.WithCancel(context.Background())

	mockProc := &mockStreamProcessor{
		processFunc: func(c context.Context, ev stream.Event) error {
			// Cancela o contexto logo após a primeira falha transitória
			cancel()
			return driver.ErrBadConn
		},
	}

	rabbitConsumer := rabbitmq.NewConsumer(topo.ch, mockProc)

	delID := uuid.New()
	ev := delivery.EventDeliveryCreated{ID: "evt-15b4c-5", DeliveryID: delID, DriverID: "driver-1", Seq: 1, PickupLat: -23.55, PickupLng: -46.63, DropoffLat: -23.56, DropoffLng: -46.64}
	p, _ := json.Marshal(ev)
	envBytes, _ := json.Marshal(map[string]interface{}{"event_id": "evt-15b4c-5", "aggregate_id": delID, "stream_position": int64(105), "event_type": "DeliveryCreated", "payload": json.RawMessage(p)})

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envBytes})
	msgs, _ := topo.ch.Consume(topo.queueName, "c1", false, false, false, false, nil)
	d := <-msgs

	err := rabbitConsumer.HandleDelivery(ctx, d)
	if err == nil {
		t.Fatal("esperava erro decorrente do cancelamento do contexto")
	}

	// Zero na DLQ
	qDLQ, _ := topo.ch.QueueInspect(topo.dlqName)
	if qDLQ.Messages != 0 {
		t.Fatalf("cancelamento não deve enviar para DLQ; messages=%d", qDLQ.Messages)
	}

	// Fechar o canal para verificar que o broker recupera a mensagem unacked
	_ = topo.ch.Close()
	ch2, err := topo.conn.Channel()
	if err != nil {
		t.Fatalf("falha ao abrir novo canal: %v", err)
	}
	defer ch2.Close()

	msgs2, err := ch2.Consume(topo.queueName, "c2", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("falha ao consumir novo canal: %v", err)
	}

	select {
	case d2 := <-msgs2:
		if !d2.Redelivered {
			t.Fatalf("mensagem recuperada após cancelamento deveria ter Redelivered=true")
		}
		_ = d2.Ack(false)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout aguardando reentrega pelo broker pós-fechamento do canal")
	}
	_ = ch2.Cancel("c2", false)
}

// 6. Reconexão e reentrega: fechamento do canal devolve mensagem ao broker, reprocessada limpa
func TestGate15B4C_6_ChannelClose_Reconnect_RedeliveryProcessed(t *testing.T) {
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b4c-reconn-%s", uuid.New().String()))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	// 1ª fase: processamento falha com context.Canceled e deixa mensagem unacked
	procCanceled := &mockStreamProcessor{
		processFunc: func(ctx context.Context, ev stream.Event) error {
			return context.Canceled
		},
	}
	consumer1 := rabbitmq.NewConsumer(topo.ch, procCanceled)

	delID := uuid.New()
	ev := delivery.EventDeliveryCreated{ID: "evt-15b4c-6", DeliveryID: delID, DriverID: "driver-1", Seq: 1, PickupLat: -23.55, PickupLng: -46.63, DropoffLat: -23.56, DropoffLng: -46.64}
	p, _ := json.Marshal(ev)
	envBytes, _ := json.Marshal(map[string]interface{}{"event_id": "evt-15b4c-6", "aggregate_id": delID, "stream_position": int64(106), "event_type": "DeliveryCreated", "payload": json.RawMessage(p)})

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envBytes})
	msgs1, _ := topo.ch.Consume(topo.queueName, "c1", false, false, false, false, nil)
	d1 := <-msgs1

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = consumer1.HandleDelivery(canceledCtx, d1)

	// Fechar canal 1 simulando crash / restart da aplicação
	_ = topo.ch.Close()

	// 2ª fase: reconexão
	ch2, err := topo.conn.Channel()
	if err != nil {
		t.Fatalf("falha ao restabelecer canal: %v", err)
	}
	defer ch2.Close()

	processedSuccess := false
	procSuccess := &mockStreamProcessor{
		processFunc: func(ctx context.Context, ev stream.Event) error {
			processedSuccess = true
			return nil
		},
	}
	consumer2 := rabbitmq.NewConsumer(ch2, procSuccess)

	msgs2, err := ch2.Consume(topo.queueName, "c2", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("falha ao consumir no canal reconectado: %v", err)
	}

	select {
	case d2 := <-msgs2:
		if !d2.Redelivered {
			t.Fatalf("esperava Redelivered=true na mensagem reentregue após crash do canal")
		}
		err := consumer2.HandleDelivery(context.Background(), d2)
		if err != nil {
			t.Fatalf("falha ao reprocessar mensagem reentregue: %v", err)
		}
		if !processedSuccess {
			t.Fatal("mensagem não foi processada com sucesso pelo consumidor reconectado")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout aguardando mensagem reentregue após reconexão")
	}
	_ = ch2.Cancel("c2", false)
}

// 7. Redelivered = true não força DLQ e não afeta o algoritmo de backoff
func TestGate15B4C_7_RedeliveredTrue_DoesNotForceDLQ(t *testing.T) {
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b4c-redeliv-%s", uuid.New().String()))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	attempts := 0
	mockProc := &mockStreamProcessor{
		processFunc: func(ctx context.Context, ev stream.Event) error {
			attempts++
			if attempts == 1 {
				// 1ª tentativa falha com deadlock transitório
				return &pq.Error{Code: "40P01", Message: "deadlock detected"}
			}
			return nil
		},
	}

	fastBackoff := rabbitmq.NewBackoffCalculator(10*time.Millisecond, 50*time.Millisecond, nil)
	rabbitConsumer := rabbitmq.NewConsumer(topo.ch, mockProc, rabbitmq.WithConsumerBackoff(fastBackoff))

	delID := uuid.New()
	ev := delivery.EventDeliveryCreated{ID: "evt-15b4c-7", DeliveryID: delID, DriverID: "driver-1", Seq: 1, PickupLat: -23.55, PickupLng: -46.63, DropoffLat: -23.56, DropoffLng: -46.64}
	p, _ := json.Marshal(ev)
	envBytes, _ := json.Marshal(map[string]interface{}{"event_id": "evt-15b4c-7", "aggregate_id": delID, "stream_position": int64(107), "event_type": "DeliveryCreated", "payload": json.RawMessage(p)})

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envBytes})
	msgs, _ := topo.ch.Consume(topo.queueName, "c1", false, false, false, false, nil)
	d := <-msgs

	// Simula flag Redelivered = true vinda do broker
	d.Redelivered = true

	err := rabbitConsumer.HandleDelivery(context.Background(), d)
	if err != nil {
		t.Fatalf("esperava sucesso após in-place retry mesmo com Redelivered=true, obteve: %v", err)
	}

	if attempts != 2 {
		t.Fatalf("esperava 2 tentativas com in-place retry, obteve %d", attempts)
	}

	qDLQ, _ := topo.ch.QueueInspect(topo.dlqName)
	if qDLQ.Messages != 0 {
		t.Fatalf("Redelivered=true não deve forçar envio para DLQ; messages=%d", qDLQ.Messages)
	}
	_ = topo.ch.Cancel("c1", false)
}

// 8. Idempotência: reentrega de evento já persistido não causa erro nem mutação
func TestGate15B4C_8_Idempotency_DuplicateExactEvent_NoMutation(t *testing.T) {
	dbConn := setupGate15B3DB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	delID := uuid.New()
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b4c-idem-%s", delID))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	validator := stream.NewSequenceValidator()
	proj := delivery.NewProjection(dbConn, "DeliveryProjection")
	rec := stream.NewGapRecovery(postgres.NewEventStore(db.New(dbConn)), validator, proj, nil)
	streamConsumer := stream.NewConsumer(validator, rec, proj, nil)
	rabbitConsumer := rabbitmq.NewConsumer(topo.ch, streamConsumer)

	ev := delivery.EventDeliveryCreated{
		ID:         "evt-15b4c-8",
		DeliveryID: delID,
		DriverID:   "driver-idem",
		Seq:        1,
		PickupLat:  -23.55,
		PickupLng:  -46.63,
		DropoffLat: -23.56,
		DropoffLng: -46.64,
	}
	p, _ := json.Marshal(ev)
	envBytes, _ := json.Marshal(map[string]interface{}{
		"event_id":        "evt-15b4c-8",
		"aggregate_id":    delID,
		"stream_position": int64(108),
		"event_type":      "DeliveryCreated",
		"payload":         json.RawMessage(p),
	})

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envBytes})
	msgs, _ := topo.ch.Consume(topo.queueName, "c1", false, false, false, false, nil)
	d1 := <-msgs

	// 1ª entrega: processada com sucesso
	err := rabbitConsumer.HandleDelivery(context.Background(), d1)
	if err != nil {
		t.Fatalf("primeira entrega falhou: %v", err)
	}

	// 2ª entrega idêntica (ex: crash após commit antes do ACK original)
	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envBytes})
	d2 := <-msgs
	d2.Redelivered = true

	err = rabbitConsumer.HandleDelivery(context.Background(), d2)
	if err != nil {
		t.Fatalf("reentrega de evento idêntico deveria ter sucesso idempotente (ACK sem erro), obteve: %v", err)
	}

	// Verificar que exatamente 1 registro existe em processed_events
	var count int
	_ = dbConn.QueryRow("SELECT COUNT(*) FROM gate15b3_test.processed_events WHERE aggregate_id = $1", delID).Scan(&count)
	if count != 1 {
		t.Fatalf("esperava exatamente 1 registro em processed_events, obteve %d", count)
	}

	qDLQ, _ := topo.ch.QueueInspect(topo.dlqName)
	if qDLQ.Messages != 0 {
		t.Fatalf("duplicata idêntica não deve ir para DLQ; messages=%d", qDLQ.Messages)
	}
	_ = topo.ch.Cancel("c1", false)
}

// 9. Invariante QoS 1: broker não entrega 2ª mensagem enquanto a 1ª estiver unacked
func TestGate15B4C_9_QoS1_Invariant_Preserved(t *testing.T) {
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b4c-qos-%s", uuid.New().String()))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	// Garantir prefetch = 1 no canal
	err := topo.ch.Qos(1, 0, false)
	if err != nil {
		t.Fatalf("falha ao definir QoS 1: %v", err)
	}

	// Publicar 2 mensagens na fila
	env1, _ := json.Marshal(map[string]interface{}{"event_id": "evt-qos-1", "aggregate_id": uuid.New(), "stream_position": int64(1), "event_type": "DeliveryCreated", "payload": json.RawMessage(`{}`)})
	env2, _ := json.Marshal(map[string]interface{}{"event_id": "evt-qos-2", "aggregate_id": uuid.New(), "stream_position": int64(2), "event_type": "DeliveryCreated", "payload": json.RawMessage(`{}`)})

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: env1})
	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: env2})

	msgs, err := topo.ch.Consume(topo.queueName, "c_qos", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("falha ao consumir: %v", err)
	}

	// 1ª mensagem recebida
	d1 := <-msgs
	if len(d1.Body) == 0 {
		t.Fatal("esperava mensagem 1")
	}

	// Enquanto d1 NÃO for ackada/nackada, o canal com QoS 1 NUNCA deve receber a 2ª mensagem!
	select {
	case d2 := <-msgs:
		t.Fatalf("VIOLAÇÃO DE QoS 1: Segunda mensagem entregue antes do reconhecimento da primeira! body=%s", string(d2.Body))
	case <-time.After(300 * time.Millisecond):
		t.Log("Confirmação: Invariante QoS 1 preservada. Nenhuma segunda mensagem entregue enquanto a primeira permanece pendente.")
	}

	// Agora fazer ACK da primeira mensagem
	_ = d1.Ack(false)

	// Imediatamente após o ACK, a 2ª mensagem deve ser liberada pelo broker
	select {
	case d2 := <-msgs:
		_ = d2.Ack(false)
		t.Log("Confirmação: Segunda mensagem entregue com sucesso imediatamente após ACK da primeira.")
	case <-time.After(2 * time.Second):
		t.Fatal("segunda mensagem não foi entregue após ACK da primeira")
	}
	_ = topo.ch.Cancel("c_qos", false)
}
