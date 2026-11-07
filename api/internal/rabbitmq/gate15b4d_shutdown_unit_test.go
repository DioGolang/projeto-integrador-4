package rabbitmq_test

import (
	"context"
	"database/sql/driver"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/dvasconcellos/elo/api/internal/rabbitmq"
	"github.com/dvasconcellos/elo/api/internal/stream"
)

// -----------------------------------------------------------------------------
// GATE 1.5B-4D: TESTES UNITÁRIOS DE SHUTDOWN GRACIOSO E RESILIÊNCIA
// -----------------------------------------------------------------------------

// 1. Sinal de shutdown interrompe a recepção de novas mensagens
func TestGate15B4D_Unit_1_ShutdownSignal_StopsNewDeliveries(t *testing.T) {
	deliveries := make(chan amqp.Delivery, 5)

	var processedCount int32
	proc := &backoffMockStreamProcessor{
		processFunc: func(ctx context.Context, ev stream.Event) error {
			atomic.AddInt32(&processedCount, 1)
			return nil
		},
	}

	consumer := rabbitmq.NewConsumer(nil, proc)
	consumeCtx, cancelConsume := context.WithCancel(context.Background())
	defer cancelConsume()

	if err := consumer.Start(consumeCtx, deliveries); err != nil {
		t.Fatalf("falha ao iniciar consumidor: %v", err)
	}

	// 1ª mensagem entregue e processada
	ack1 := &backoffMockAcknowledger{}
	envBytes := []byte(`{"event_id":"evt-u1","aggregate_id":"00000000-0000-0000-0000-000000000001","stream_position":1,"event_type":"DeliveryCreated","payload":{"id":"evt-u1","delivery_id":"00000000-0000-0000-0000-000000000001","driver_id":"d1","seq":1,"pickup_lat":0,"pickup_lng":0,"dropoff_lat":0,"dropoff_lng":0}}`)
	deliveries <- amqp.Delivery{Acknowledger: ack1, Body: envBytes}

	time.Sleep(20 * time.Millisecond)
	if atomic.LoadInt32(&processedCount) != 1 {
		t.Fatalf("esperava 1 mensagem processada, obteve %d", atomic.LoadInt32(&processedCount))
	}

	// Executar shutdown
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancelDrain()
	cancelConsume() // Interrompe consumo

	if err := consumer.Stop(drainCtx); err != nil {
		t.Fatalf("falha no shutdown: %v", err)
	}

	// Tentar enviar 2ª mensagem após shutdown: não deve ser processada
	ack2 := &backoffMockAcknowledger{}
	deliveries <- amqp.Delivery{Acknowledger: ack2, Body: envBytes}
	time.Sleep(20 * time.Millisecond)

	if atomic.LoadInt32(&processedCount) != 1 {
		t.Fatalf("nenhuma nova entrega deve ser processada após shutdown; obteve %d", atomic.LoadInt32(&processedCount))
	}
	if ack2.ackCalled {
		t.Fatal("mensagem enviada após shutdown não deveria ter sido reconhecida")
	}
}

// 2. WaitGroup não sofre race entre Add e Wait sob alta concorrência
func TestGate15B4D_Unit_2_WaitGroup_NoRaceBetweenAddAndWait(t *testing.T) {
	proc := &backoffMockStreamProcessor{
		processFunc: func(ctx context.Context, ev stream.Event) error {
			time.Sleep(1 * time.Millisecond)
			return nil
		},
	}

	for iter := 0; iter < 10; iter++ {
		deliveries := make(chan amqp.Delivery, 50)
		consumer := rabbitmq.NewConsumer(nil, proc)
		consumeCtx, cancelConsume := context.WithCancel(context.Background())

		_ = consumer.Start(consumeCtx, deliveries)

		envBytes := []byte(`{"event_id":"evt-race","aggregate_id":"00000000-0000-0000-0000-000000000001","stream_position":1,"event_type":"DeliveryCreated","payload":{"id":"evt-race","delivery_id":"00000000-0000-0000-0000-000000000001","driver_id":"d1","seq":1,"pickup_lat":0,"pickup_lng":0,"dropoff_lat":0,"dropoff_lng":0}}`)

		var wgPublishers sync.WaitGroup
		// Spawna goroutines para encher o canal de entregas concorrentemente com o Stop
		for p := 0; p < 5; p++ {
			wgPublishers.Add(1)
			go func() {
				defer wgPublishers.Done()
				for i := 0; i < 10; i++ {
					deliveries <- amqp.Delivery{
						Acknowledger: &backoffMockAcknowledger{},
						Body:         envBytes,
					}
				}
			}()
		}

		// Dispara Stop em paralelo
		time.Sleep(2 * time.Millisecond)
		cancelConsume()
		drainCtx, cancelDrain := context.WithTimeout(context.Background(), 2*time.Second)
		_ = consumer.Stop(drainCtx)
		cancelDrain()

		wgPublishers.Wait()
		close(deliveries)
	}
}

// 3. Processamento em andamento pode concluir com sucesso durante a drenagem
func TestGate15B4D_Unit_3_InFlightProcessing_CompletesDuringDrain(t *testing.T) {
	deliveries := make(chan amqp.Delivery, 1)

	processingStarted := make(chan struct{})
	proc := &backoffMockStreamProcessor{
		processFunc: func(ctx context.Context, ev stream.Event) error {
			close(processingStarted)
			// Simula operação SQL em andamento que leva 40ms
			time.Sleep(40 * time.Millisecond)
			return nil
		},
	}

	consumer := rabbitmq.NewConsumer(nil, proc, rabbitmq.WithConsumerDrainTimeout(1*time.Second))
	consumeCtx, cancelConsume := context.WithCancel(context.Background())
	defer cancelConsume()

	_ = consumer.Start(consumeCtx, deliveries)

	ack := &backoffMockAcknowledger{}
	envBytes := []byte(`{"event_id":"evt-u3","aggregate_id":"00000000-0000-0000-0000-000000000001","stream_position":1,"event_type":"DeliveryCreated","payload":{"id":"evt-u3","delivery_id":"00000000-0000-0000-0000-000000000001","driver_id":"d1","seq":1,"pickup_lat":0,"pickup_lng":0,"dropoff_lat":0,"dropoff_lng":0}}`)
	deliveries <- amqp.Delivery{Acknowledger: ack, Body: envBytes}

	// Aguarda processamento iniciar
	<-processingStarted

	// Dispara shutdown enquanto o processamento está em curso
	startStop := time.Now()
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancelDrain()
	cancelConsume()

	err := consumer.Stop(drainCtx)
	stopDuration := time.Since(startStop)

	if err != nil {
		t.Fatalf("Stop não deveria retornar erro quando operação conclui na drenagem: %v", err)
	}

	if !ack.ackCalled {
		t.Fatal("esperava que a mensagem em drenagem concluísse e executasse ACK")
	}

	if stopDuration > 500*time.Millisecond {
		t.Errorf("Stop demorou excessivamente (%v), deveria liberar logo após os 40ms de processamento", stopDuration)
	}
}

// 4. Cancelamento depois dos 5 segundos (ou prazo configurado) interrompe a operação
func TestGate15B4D_Unit_4_DrainTimeout_CancelsInFlightOperation(t *testing.T) {
	deliveries := make(chan amqp.Delivery, 1)

	processingStarted := make(chan struct{})
	var operationCanceled bool
	proc := &backoffMockStreamProcessor{
		processFunc: func(ctx context.Context, ev stream.Event) error {
			close(processingStarted)
			// Operação bloqueada indefinidamente até cancelamento do contexto
			select {
			case <-ctx.Done():
				operationCanceled = true
				return ctx.Err()
			}
		},
	}

	consumer := rabbitmq.NewConsumer(nil, proc)
	consumeCtx, cancelConsume := context.WithCancel(context.Background())
	defer cancelConsume()

	_ = consumer.Start(consumeCtx, deliveries)

	ack := &backoffMockAcknowledger{}
	envBytes := []byte(`{"event_id":"evt-u4","aggregate_id":"00000000-0000-0000-0000-000000000001","stream_position":1,"event_type":"DeliveryCreated","payload":{"id":"evt-u4","delivery_id":"00000000-0000-0000-0000-000000000001","driver_id":"d1","seq":1,"pickup_lat":0,"pickup_lng":0,"dropoff_lat":0,"dropoff_lng":0}}`)
	deliveries <- amqp.Delivery{Acknowledger: ack, Body: envBytes}

	<-processingStarted

	// Dispara Stop com drain timeout curto de 50ms para o teste
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelDrain()
	cancelConsume()

	err := consumer.Stop(drainCtx)
	if err == nil {
		t.Fatal("esperava erro de timeout de drenagem no Stop, obteve nil")
	}

	if !operationCanceled {
		t.Fatal("esperava que o contexto da operação in-flight fosse cancelado pelo drain timeout")
	}

	// Nenhum ACK deve ter ocorrido
	if ack.ackCalled {
		t.Fatal("mensagem com operação cancelada por timeout de drenagem JAMAIS deve ser reconhecida (ACK)")
	}
	if ack.rejectCalled {
		t.Fatal("mensagem cancelada por timeout de drenagem NÃO deve ir para DLQ")
	}
}

// 5. Backoff é interrompido imediatamente pelo shutdown
func TestGate15B4D_Unit_5_Backoff_InterruptedImmediatelyByShutdown(t *testing.T) {
	deliveries := make(chan amqp.Delivery, 1)

	retryStarted := make(chan struct{})
	attempts := 0
	proc := &backoffMockStreamProcessor{
		processFunc: func(ctx context.Context, ev stream.Event) error {
			attempts++
			return driver.ErrBadConn // transient
		},
	}

	// Sleeper que avisa que entrou em sono longo (10 segundos)
	var sleepInterrupted atomic.Bool
	sleeper := func(ctx context.Context, d time.Duration) error {
		select {
		case <-retryStarted:
		default:
			close(retryStarted)
		}
		select {
		case <-ctx.Done():
			sleepInterrupted.Store(true)
			return ctx.Err()
		case <-time.After(d):
			return nil
		}
	}

	// Backoff fixo de 10s
	backoff := rabbitmq.NewBackoffCalculator(10*time.Second, 10*time.Second, nil)
	consumer := rabbitmq.NewConsumer(nil, proc,
		rabbitmq.WithConsumerBackoff(backoff),
		rabbitmq.WithConsumerSleeper(sleeper),
	)

	consumeCtx, cancelConsume := context.WithCancel(context.Background())
	defer cancelConsume()

	_ = consumer.Start(consumeCtx, deliveries)

	ack := &backoffMockAcknowledger{}
	envBytes := []byte(`{"event_id":"evt-u5","aggregate_id":"00000000-0000-0000-0000-000000000001","stream_position":1,"event_type":"DeliveryCreated","payload":{"id":"evt-u5","delivery_id":"00000000-0000-0000-0000-000000000001","driver_id":"d1","seq":1,"pickup_lat":0,"pickup_lng":0,"dropoff_lat":0,"dropoff_lng":0}}`)
	deliveries <- amqp.Delivery{Acknowledger: ack, Body: envBytes}

	// Aguarda entrar no sleeper de retry
	<-retryStarted

	startStop := time.Now()
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelDrain()
	cancelConsume()

	_ = consumer.Stop(drainCtx)
	elapsed := time.Since(startStop)

	if !sleepInterrupted.Load() {
		t.Fatal("esperava que o sleeper de backoff fosse interrompido pelo shutdown")
	}

	// O Stop deve ter finalizado imediatamente (em poucos milissegundos), sem esperar os 10s do sleeper
	if elapsed > 300*time.Millisecond {
		t.Fatalf("interrupção do sleeper demorou muito: %v (esperava interrupção imediata)", elapsed)
	}

	// Sem ACK, sem Reject
	if ack.ackCalled || ack.rejectCalled {
		t.Fatalf("mensagem em retry interrompida por shutdown não pode receber ACK nem Reject (ack=%v, reject=%v)", ack.ackCalled, ack.rejectCalled)
	}
}

// 6. Nenhum ACK ocorre depois de cancelamento de processamento incompleto
func TestGate15B4D_Unit_6_NoAckAfterCanceledIncompleteProcessing(t *testing.T) {
	ack := &backoffMockAcknowledger{}
	proc := &backoffMockStreamProcessor{
		processFunc: func(ctx context.Context, ev stream.Event) error {
			// Simula transação que foi cancelada externamente
			<-ctx.Done()
			return ctx.Err()
		},
	}

	consumer := rabbitmq.NewConsumer(nil, proc)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Já cancelado

	envBytes := []byte(`{"event_id":"evt-u6","aggregate_id":"00000000-0000-0000-0000-000000000001","stream_position":1,"event_type":"DeliveryCreated","payload":{"id":"evt-u6","delivery_id":"00000000-0000-0000-0000-000000000001","driver_id":"d1","seq":1,"pickup_lat":0,"pickup_lng":0,"dropoff_lat":0,"dropoff_lng":0}}`)
	d := amqp.Delivery{
		Acknowledger: ack,
		Body:         envBytes,
	}

	err := consumer.HandleDelivery(ctx, d)
	if err == nil {
		t.Fatal("esperava erro de contexto cancelado, obteve nil")
	}

	if ack.ackCalled {
		t.Fatal("VIOLAÇÃO CRÍTICA: ACK emitido após cancelamento de contexto!")
	}
	if ack.rejectCalled {
		t.Fatal("VIOLAÇÃO CRÍTICA: Reject (DLQ) emitido em contexto cancelado!")
	}
}
