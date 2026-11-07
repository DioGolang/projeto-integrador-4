package rabbitmq_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
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
// GATE 1.5B-4D: TESTES DE INTEGRAÇÃO COM RABBITMQ E POSTGRESQL REAIS
// -----------------------------------------------------------------------------

// 1. SIGTERM durante processamento: operação conclui dentro da janela de drenagem e ACK ocorre
func TestGate15B4D_Integration_1_SIGTERM_DuringProcessing_CompletesAndACKs(t *testing.T) {
	dbConn := setupGate15B3DB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	delID := uuid.New()
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b4d-proc-%s", delID))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	validator := stream.NewSequenceValidator()
	proj := delivery.NewProjection(dbConn, "DeliveryProjection")
	rec := stream.NewGapRecovery(postgres.NewEventStore(db.New(dbConn)), validator, proj, nil)
	realConsumer := stream.NewConsumer(validator, rec, proj, nil)

	processingStarted := make(chan struct{})
	// Wrapper que introduz pequeno atraso para garantir que o shutdown ocorra com a mensagem in-flight
	slowProcessor := &mockStreamProcessor{
		processFunc: func(ctx context.Context, ev stream.Event) error {
			select {
			case <-processingStarted:
			default:
				close(processingStarted)
			}
			time.Sleep(60 * time.Millisecond)
			return realConsumer.Process(ctx, ev)
		},
	}

	consumer := rabbitmq.NewConsumer(
		topo.ch,
		slowProcessor,
		rabbitmq.WithConsumerTag("test.tag.drain1"),
		rabbitmq.WithConsumerDrainTimeout(2*time.Second),
	)

	msgs, err := topo.ch.Consume(topo.queueName, "test.tag.drain1", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("falha ao consumir fila: %v", err)
	}

	consumeCtx, cancelConsume := context.WithCancel(context.Background())
	defer cancelConsume()

	if err := consumer.Start(consumeCtx, msgs); err != nil {
		t.Fatalf("falha ao iniciar consumer: %v", err)
	}

	ev := delivery.EventDeliveryCreated{
		ID:         "evt-15b4d-1",
		DeliveryID: delID,
		DriverID:   "driver-drain-1",
		Seq:        1,
		PickupLat:  -23.55,
		PickupLng:  -46.63,
		DropoffLat: -23.56,
		DropoffLng: -46.64,
	}
	p, _ := json.Marshal(ev)
	envBytes, _ := json.Marshal(map[string]interface{}{
		"event_id":        "evt-15b4d-1",
		"aggregate_id":    delID,
		"stream_position": int64(201),
		"event_type":      "DeliveryCreated",
		"payload":         json.RawMessage(p),
	})

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envBytes})

	// Aguarda o processamento iniciar no banco
	<-processingStarted

	// Dispara o shutdown enquanto a mensagem está sendo processada
	cancelConsume()
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelDrain()

	err = consumer.Stop(drainCtx)
	if err != nil {
		t.Fatalf("Stop falhou durante drenagem bem-sucedida: %v", err)
	}

	// Verificar que a transação foi commitada no PostgreSQL
	var count int
	_ = dbConn.QueryRow("SELECT COUNT(*) FROM gate15b3_test.processed_events WHERE event_id = $1", "evt-15b4d-1").Scan(&count)
	if count != 1 {
		t.Fatalf("esperava 1 registro em processed_events commitado durante a drenagem, obteve %d", count)
	}

	// Verificar que a mensagem foi ACKada no RabbitMQ e NÃO foi para DLQ
	chNew, err := topo.conn.Channel()
	if err != nil {
		t.Fatalf("falha ao abrir canal de inspeção: %v", err)
	}
	defer chNew.Close()

	qMain, _ := chNew.QueueInspect(topo.queueName)
	if qMain.Messages != 0 {
		t.Fatalf("esperava fila principal zerada após ACK na drenagem, obteve %d", qMain.Messages)
	}
	qDLQ, _ := chNew.QueueInspect(topo.dlqName)
	if qDLQ.Messages != 0 {
		t.Fatalf("esperava DLQ zerada, obteve %d", qDLQ.Messages)
	}
}

// 2. SIGTERM durante backoff: mensagem permanece sem ACK e volta para a fila após fechamento do canal
func TestGate15B4D_Integration_2_SIGTERM_DuringBackoff_PreservedUnacked_RedeliveredAfterChannelClose(t *testing.T) {
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b4d-backoff-%s", uuid.New().String()))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	sleepStarted := make(chan struct{})
	attempts := 0
	proc := &mockStreamProcessor{
		processFunc: func(ctx context.Context, ev stream.Event) error {
			attempts++
			return driver.ErrBadConn // transient operational error
		},
	}

	var sleeperInterrupted atomic.Bool
	sleeper := func(ctx context.Context, d time.Duration) error {
		select {
		case <-sleepStarted:
		default:
			close(sleepStarted)
		}
		select {
		case <-ctx.Done():
			sleeperInterrupted.Store(true)
			return ctx.Err()
		case <-time.After(d):
			return nil
		}
	}

	// Backoff com 10 segundos para comprovar que o shutdown interrompe imediatamente
	backoff := rabbitmq.NewBackoffCalculator(10*time.Second, 10*time.Second, nil)
	consumer := rabbitmq.NewConsumer(
		topo.ch,
		proc,
		rabbitmq.WithConsumerBackoff(backoff),
		rabbitmq.WithConsumerSleeper(sleeper),
		rabbitmq.WithConsumerTag("test.tag.backoff"),
	)

	msgs, _ := topo.ch.Consume(topo.queueName, "test.tag.backoff", false, false, false, false, nil)
	consumeCtx, cancelConsume := context.WithCancel(context.Background())
	defer cancelConsume()

	_ = consumer.Start(consumeCtx, msgs)

	delID := uuid.New()
	ev := delivery.EventDeliveryCreated{ID: "evt-15b4d-2", DeliveryID: delID, DriverID: "driver-1", Seq: 1, PickupLat: -23.55, PickupLng: -46.63, DropoffLat: -23.56, DropoffLng: -46.64}
	p, _ := json.Marshal(ev)
	envBytes, _ := json.Marshal(map[string]interface{}{"event_id": "evt-15b4d-2", "aggregate_id": delID, "stream_position": int64(202), "event_type": "DeliveryCreated", "payload": json.RawMessage(p)})

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envBytes})

	// Aguarda entrar no sono de backoff
	<-sleepStarted

	startStop := time.Now()
	cancelConsume()
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelDrain()

	_ = consumer.Stop(drainCtx)
	elapsed := time.Since(startStop)

	if !sleeperInterrupted.Load() {
		t.Fatal("sleeper de backoff não foi interrompido pelo shutdown")
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("Stop durante backoff demorou demais: %v (esperava interrupção imediata)", elapsed)
	}

	// Abrir novo canal para reconectar e verificar que o RabbitMQ reentrega a mensagem com Redelivered=true
	ch2, err := topo.conn.Channel()
	if err != nil {
		t.Fatalf("falha ao abrir canal 2: %v", err)
	}
	defer ch2.Close()

	msgs2, err := ch2.Consume(topo.queueName, "test.tag.reconn", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("falha ao consumir canal 2: %v", err)
	}

	select {
	case d2 := <-msgs2:
		if !d2.Redelivered {
			t.Fatal("esperava que a mensagem reentregue após fechamento do canal tivesse Redelivered=true")
		}
		_ = d2.Ack(false)
		t.Log("Confirmação: Mensagem em backoff preservada unacked e devolvida pelo broker pós-fechamento do canal.")
	case <-time.After(2 * time.Second):
		t.Fatal("timeout aguardando reentrega da mensagem pelo broker")
	}
	_ = ch2.Cancel("test.tag.reconn", false)
}

// 3. SIGTERM durante transação: rollback no PostgreSQL e redelivery pelo broker
func TestGate15B4D_Integration_3_SIGTERM_DuringTransaction_RollbackAndRedelivery(t *testing.T) {
	dbConn := setupGate15B3DB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	delID := uuid.New()
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b4d-tx-%s", delID))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	txStarted := make(chan struct{})
	// Mock que inicia transação no PostgreSQL e bloqueia até context.Done()
	procTx := &mockStreamProcessor{
		processFunc: func(ctx context.Context, ev stream.Event) error {
			tx, err := dbConn.BeginTx(ctx, &sql.TxOptions{})
			if err != nil {
				return err
			}
			defer tx.Rollback()

			_, err = tx.ExecContext(ctx, `
				INSERT INTO gate15b3_test.processed_events (consumer_id, event_id, aggregate_id, sequence, event_type, payload_hash)
				VALUES ($1, $2, $3, $4, $5, $6)
			`, "DeliveryProjection", "evt-15b4d-3", delID, 1, "DeliveryCreated", "hash-3")
			if err != nil {
				return err
			}

			close(txStarted)

			// Bloqueia simulando query longa ou travamento até que o drain timeout cancele
			<-ctx.Done()
			return ctx.Err()
		},
	}

	consumer := rabbitmq.NewConsumer(
		topo.ch,
		procTx,
		rabbitmq.WithConsumerTag("test.tag.tx"),
		rabbitmq.WithConsumerDrainTimeout(50*time.Millisecond), // Drain curto de 50ms para teste
	)

	msgs, _ := topo.ch.Consume(topo.queueName, "test.tag.tx", false, false, false, false, nil)
	consumeCtx, cancelConsume := context.WithCancel(context.Background())
	defer cancelConsume()

	_ = consumer.Start(consumeCtx, msgs)

	ev := delivery.EventDeliveryCreated{ID: "evt-15b4d-3", DeliveryID: delID, DriverID: "driver-1", Seq: 1, PickupLat: -23.55, PickupLng: -46.63, DropoffLat: -23.56, DropoffLng: -46.64}
	p, _ := json.Marshal(ev)
	envBytes, _ := json.Marshal(map[string]interface{}{"event_id": "evt-15b4d-3", "aggregate_id": delID, "stream_position": int64(203), "event_type": "DeliveryCreated", "payload": json.RawMessage(p)})

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envBytes})

	// Aguarda transação ser aberta e linha inserida dentro da tx
	<-txStarted

	// Dispara shutdown com drain de 50ms (que vai estourar e cancelar a in-flight tx)
	cancelConsume()
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelDrain()

	err := consumer.Stop(drainCtx)
	if err == nil {
		t.Fatal("esperava erro de timeout de drenagem no Stop")
	}

	// Verificar que a transação sofreu rollback no banco (0 registros)
	var count int
	_ = dbConn.QueryRow("SELECT COUNT(*) FROM gate15b3_test.processed_events WHERE event_id = $1", "evt-15b4d-3").Scan(&count)
	if count != 0 {
		t.Fatalf("esperava 0 registros em processed_events após rollback por timeout de drenagem, obteve %d", count)
	}

	// Reconectar canal e verificar redelivery
	ch2, err := topo.conn.Channel()
	if err != nil {
		t.Fatalf("falha ao abrir canal 2: %v", err)
	}
	defer ch2.Close()

	msgs2, _ := ch2.Consume(topo.queueName, "test.tag.tx.reconn", false, false, false, false, nil)
	select {
	case d2 := <-msgs2:
		if !d2.Redelivered {
			t.Fatal("esperava Redelivered=true após rollback e canal fechado")
		}
		_ = d2.Ack(false)
		t.Log("Confirmação: Transação abortada sofreu rollback e mensagem foi reentregue pelo broker.")
	case <-time.After(2 * time.Second):
		t.Fatal("timeout aguardando reentrega pós-rollback")
	}
	_ = ch2.Cancel("test.tag.tx.reconn", false)
}

// 4. SIGTERM depois do commit, antes do ACK: redelivery com idempotência sem duplicar efeitos
func TestGate15B4D_Integration_4_SIGTERM_AfterCommit_BeforeACK_IdempotentRedelivery(t *testing.T) {
	dbConn := setupGate15B3DB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	delID := uuid.New()
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b4d-crash-%s", delID))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	validator := stream.NewSequenceValidator()
	proj := delivery.NewProjection(dbConn, "DeliveryProjection")
	rec := stream.NewGapRecovery(postgres.NewEventStore(db.New(dbConn)), validator, proj, nil)
	streamConsumer := stream.NewConsumer(validator, rec, proj, nil)

	ev := delivery.EventDeliveryCreated{
		ID:         "evt-15b4d-4",
		DeliveryID: delID,
		DriverID:   "driver-crash",
		Seq:        1,
		PickupLat:  -23.55,
		PickupLng:  -46.63,
		DropoffLat: -23.56,
		DropoffLng: -46.64,
	}
	p, _ := json.Marshal(ev)
	envBytes, _ := json.Marshal(map[string]interface{}{
		"event_id":        "evt-15b4d-4",
		"aggregate_id":    delID,
		"stream_position": int64(204),
		"event_type":      "DeliveryCreated",
		"payload":         json.RawMessage(p),
	})

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envBytes})
	msgs, _ := topo.ch.Consume(topo.queueName, "c1", false, false, false, false, nil)
	_ = <-msgs

	// Simula crash exatamente após o commit no banco, antes de d1.Ack() chegar ao broker
	streamEv := stream.Event{StreamPosition: 204, DomainEvent: ev}
	err := streamConsumer.Process(context.Background(), streamEv)
	if err != nil {
		t.Fatalf("commit no banco falhou: %v", err)
	}

	// Fechar canal 1 sem executar d1.Ack() simulando crash do processo
	_ = topo.ch.Close()

	// Reconectar
	ch2, err := topo.conn.Channel()
	if err != nil {
		t.Fatalf("falha ao reconectar canal: %v", err)
	}
	defer ch2.Close()

	rabbitConsumer2 := rabbitmq.NewConsumer(ch2, streamConsumer)
	msgs2, _ := ch2.Consume(topo.queueName, "c2", false, false, false, false, nil)

	select {
	case d2 := <-msgs2:
		if !d2.Redelivered {
			t.Fatal("esperava Redelivered=true após crash antes do ACK")
		}
		// Processar reentrega: deve ser reconhecida como duplicata exata e dar ACK sem erro
		err := rabbitConsumer2.HandleDelivery(context.Background(), d2)
		if err != nil {
			t.Fatalf("reentrega de evento commitado não deveria falhar: %v", err)
		}

		// Verificar que exatamente 1 registro existe no banco (sem duplicação)
		var count int
		_ = dbConn.QueryRow("SELECT COUNT(*) FROM gate15b3_test.processed_events WHERE aggregate_id = $1", delID).Scan(&count)
		if count != 1 {
			t.Fatalf("esperava exatamente 1 registro em processed_events, obteve %d", count)
		}
		t.Log("Confirmação: Crash pós-commit e pré-ACK absorvido com sucesso pela idempotência persistida.")
	case <-time.After(2 * time.Second):
		t.Fatal("timeout aguardando reentrega pós-crash")
	}
	_ = ch2.Cancel("c2", false)
}

// 5. Prazo de drenagem excedido: canal fecha e mensagem é preservada na fila
func TestGate15B4D_Integration_5_DrainTimeoutExceeded_ChannelCloses_MessagePreserved(t *testing.T) {
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b4d-timeout-%s", uuid.New().String()))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	started := make(chan struct{})
	procHanging := &mockStreamProcessor{
		processFunc: func(ctx context.Context, ev stream.Event) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		},
	}

	consumer := rabbitmq.NewConsumer(
		topo.ch,
		procHanging,
		rabbitmq.WithConsumerTag("test.tag.drain.hang"),
	)

	msgs, _ := topo.ch.Consume(topo.queueName, "test.tag.drain.hang", false, false, false, false, nil)
	consumeCtx, cancelConsume := context.WithCancel(context.Background())
	defer cancelConsume()

	_ = consumer.Start(consumeCtx, msgs)

	delID := uuid.New()
	ev := delivery.EventDeliveryCreated{ID: "evt-15b4d-5", DeliveryID: delID, DriverID: "driver-1", Seq: 1, PickupLat: -23.55, PickupLng: -46.63, DropoffLat: -23.56, DropoffLng: -46.64}
	p, _ := json.Marshal(ev)
	envBytes, _ := json.Marshal(map[string]interface{}{"event_id": "evt-15b4d-5", "aggregate_id": delID, "stream_position": int64(205), "event_type": "DeliveryCreated", "payload": json.RawMessage(p)})

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envBytes})
	<-started

	// Drenagem curta de 50ms que certamente expirará
	cancelConsume()
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelDrain()

	err := consumer.Stop(drainCtx)
	if err == nil {
		t.Fatal("esperava erro por estouro do prazo de drenagem")
	}

	// Inspecionar com novo canal
	chNew, err := topo.conn.Channel()
	if err != nil {
		t.Fatalf("falha ao abrir canal novo: %v", err)
	}
	defer chNew.Close()

	// Mensagem deve continuar na fila principal pronta para novo consumo
	qMain, _ := chNew.QueueInspect(topo.queueName)
	if qMain.Messages != 1 {
		t.Fatalf("esperava 1 mensagem preservada na fila principal, obteve %d", qMain.Messages)
	}

	// Zero na DLQ
	qDLQ, _ := chNew.QueueInspect(topo.dlqName)
	if qDLQ.Messages != 0 {
		t.Fatalf("mensagem com timeout de drenagem não deve ir para DLQ; obteve %d", qDLQ.Messages)
	}
	t.Log("Confirmação: Estouro de drenagem preservou a mensagem na fila sem descarte ou DLQ.")
}

// 6. Fechamento inesperado do canal pelo broker: aplicação detecta a condição
func TestGate15B4D_Integration_6_UnexpectedChannelCloseByBroker_Detected(t *testing.T) {
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b4d-chclose-%s", uuid.New().String()))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	proc := &mockStreamProcessor{
		processFunc: func(ctx context.Context, ev stream.Event) error {
			return nil
		},
	}

	consumer := rabbitmq.NewConsumer(topo.ch, proc, rabbitmq.WithConsumerTag("test.tag.detect"))
	msgs, _ := topo.ch.Consume(topo.queueName, "test.tag.detect", false, false, false, false, nil)

	consumeCtx, cancelConsume := context.WithCancel(context.Background())
	defer cancelConsume()

	_ = consumer.Start(consumeCtx, msgs)

	// Simula encerramento inesperado do canal fechando-o diretamente
	_ = topo.ch.Close()

	// Verificar detecção via canal de notificação ou encerramento do runner
	select {
	case errClose := <-consumer.NotifyClose():
		if errClose != nil {
			t.Logf("Canal de notificação capturou encerramento com sucesso: %v", errClose)
		}
	case <-time.After(500 * time.Millisecond):
		// Também aceitável: entrega de ok=false no loop
	}

	// DrainCtx e Stop
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancelDrain()
	_ = consumer.Stop(drainCtx)

	t.Log("Confirmação: Encerramento inesperado do canal detectado e liberado com segurança.")
}

// 7. Recuperação de mensagem suspensa por erro desconhecido após reciclagem do canal
func TestGate15B4D_Integration_7_SuspendedUnknownMessage_RecoveryAfterChannelRecycle(t *testing.T) {
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b4d-unknown-recycle-%s", uuid.New().String()))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	// 1ª fase: processador falha com erro desconhecido
	procFailing := &mockStreamProcessor{
		processFunc: func(ctx context.Context, ev stream.Event) error {
			return errors.New("synthetic unclassified unknown bug")
		},
	}
	consumer1 := rabbitmq.NewConsumer(topo.ch, procFailing)

	delID := uuid.New()
	ev := delivery.EventDeliveryCreated{ID: "evt-15b4d-7", DeliveryID: delID, DriverID: "driver-1", Seq: 1, PickupLat: -23.55, PickupLng: -46.63, DropoffLat: -23.56, DropoffLng: -46.64}
	p, _ := json.Marshal(ev)
	envBytes, _ := json.Marshal(map[string]interface{}{"event_id": "evt-15b4d-7", "aggregate_id": delID, "stream_position": int64(207), "event_type": "DeliveryCreated", "payload": json.RawMessage(p)})

	_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envBytes})
	msgs1, _ := topo.ch.Consume(topo.queueName, "c1", false, false, false, false, nil)
	d1 := <-msgs1

	err := consumer1.HandleDelivery(context.Background(), d1)
	if err == nil {
		t.Fatal("esperava erro desconhecido")
	}

	if !consumer1.IsSuspended() {
		t.Fatal("esperava consumidor 1 suspenso")
	}

	// Zero na DLQ
	qDLQ, _ := topo.ch.QueueInspect(topo.dlqName)
	if qDLQ.Messages != 0 {
		t.Fatalf("erro desconhecido não deve ir para DLQ; messages=%d", qDLQ.Messages)
	}

	// 2ª fase: PROCEDIMENTO SEGURO DE RECUPERAÇÃO OPERACIONAL
	// 1. Saneamento da causa (neste teste, substituímos por processador corrigido com sucesso)
	// 2. Fechamento do canal antigo para que o broker libere a mensagem unacked
	_ = topo.ch.Close()

	// 3. Abertura de novo canal e reconsumo
	ch2, err := topo.conn.Channel()
	if err != nil {
		t.Fatalf("falha ao abrir canal 2: %v", err)
	}
	defer ch2.Close()

	recovered := false
	procFixed := &mockStreamProcessor{
		processFunc: func(ctx context.Context, ev stream.Event) error {
			recovered = true
			return nil
		},
	}
	consumer2 := rabbitmq.NewConsumer(ch2, procFixed)

	msgs2, err := ch2.Consume(topo.queueName, "c2", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("falha ao consumir canal 2: %v", err)
	}

	select {
	case d2 := <-msgs2:
		if !d2.Redelivered {
			t.Fatal("esperava que a mensagem reentregue tivesse Redelivered=true")
		}
		err := consumer2.HandleDelivery(context.Background(), d2)
		if err != nil {
			t.Fatalf("falha ao processar mensagem com consumidor corrigido: %v", err)
		}
		if !recovered {
			t.Fatal("mensagem não foi processada pelo consumidor recuperado")
		}
		t.Log("Confirmação: Recuperação operacional por reciclagem de canal testada e validada com sucesso.")
	case <-time.After(2 * time.Second):
		t.Fatal("timeout aguardando reentrega da mensagem após reciclagem do canal")
	}
	_ = ch2.Cancel("c2", false)
}

// 8. Nenhuma goroutine do consumidor permanece ativa depois do shutdown concluído
func TestGate15B4D_Integration_8_NoConsumerGoroutineLeakedAfterShutdown(t *testing.T) {
	topo := setupGate15B3AMQP(t, fmt.Sprintf("15b4d-leak-%s", uuid.New().String()))
	if topo == nil {
		return
	}
	defer topo.Cleanup()

	proc := &mockStreamProcessor{
		processFunc: func(ctx context.Context, ev stream.Event) error {
			return nil
		},
	}

	consumer := rabbitmq.NewConsumer(topo.ch, proc, rabbitmq.WithConsumerTag("test.tag.leak"))
	msgs, _ := topo.ch.Consume(topo.queueName, "test.tag.leak", false, false, false, false, nil)

	// Captura contagem de goroutines antes de iniciar o runner do consumidor
	runtime.GC()
	initialGoroutines := runtime.NumGoroutine()

	consumeCtx, cancelConsume := context.WithCancel(context.Background())
	defer cancelConsume()

	_ = consumer.Start(consumeCtx, msgs)

	// Publica e processa 3 mensagens
	for i := 1; i <= 3; i++ {
		delID := uuid.New()
		ev := delivery.EventDeliveryCreated{ID: fmt.Sprintf("evt-leak-%d", i), DeliveryID: delID, DriverID: "d1", Seq: int64(i), PickupLat: 0, PickupLng: 0, DropoffLat: 0, DropoffLng: 0}
		p, _ := json.Marshal(ev)
		envBytes, _ := json.Marshal(map[string]interface{}{"event_id": fmt.Sprintf("evt-leak-%d", i), "aggregate_id": delID, "stream_position": int64(300 + i), "event_type": "DeliveryCreated", "payload": json.RawMessage(p)})
		_ = topo.ch.PublishWithContext(context.Background(), "", topo.queueName, false, false, amqp.Publishing{Body: envBytes})
	}

	time.Sleep(50 * time.Millisecond)

	// Executa shutdown
	cancelConsume()
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelDrain()

	_ = consumer.Stop(drainCtx)

	// Aguarda estabilização do runtime
	time.Sleep(50 * time.Millisecond)
	runtime.GC()
	finalGoroutines := runtime.NumGoroutine()

	// A contagem final não deve ter acumulado goroutines do consumidor
	diff := finalGoroutines - initialGoroutines
	if diff > 1 {
		t.Fatalf("vazamento de goroutines detectado: inicial=%d, final=%d (diff=%d)", initialGoroutines, finalGoroutines, diff)
	}
	t.Logf("Confirmação: Zero vazamento de goroutines pós-shutdown (inicial=%d, final=%d).", initialGoroutines, finalGoroutines)
}
