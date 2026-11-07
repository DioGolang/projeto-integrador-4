package rabbitmq_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"math/rand"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/dvasconcellos/elo/api/internal/rabbitmq"
	"github.com/dvasconcellos/elo/api/internal/stream"
)

// deterministicRNG retorna sempre a fração especificada do teto n.
type deterministicRNG struct {
	fraction float64
}

func (d *deterministicRNG) Int63n(n int64) int64 {
	if n <= 1 {
		return 0
	}
	return int64(float64(n-1) * d.fraction)
}

func TestBackoff_Unit(t *testing.T) {
	initial := 100 * time.Millisecond
	maxDelay := 5 * time.Second

	// 1. Cálculo do backoff nos primeiros níveis (Ceilings determinísticos)
	t.Run("calculo_do_teto_nos_primeiros_niveis", func(t *testing.T) {
		b := rabbitmq.NewBackoffCalculator(initial, maxDelay, nil)

		expectedCeilings := []time.Duration{
			100 * time.Millisecond,  // attempt 0: 100ms * 2^0
			200 * time.Millisecond,  // attempt 1: 100ms * 2^1
			400 * time.Millisecond,  // attempt 2: 100ms * 2^2
			800 * time.Millisecond,  // attempt 3: 100ms * 2^3
			1600 * time.Millisecond, // attempt 4: 100ms * 2^4
			3200 * time.Millisecond, // attempt 5: 100ms * 2^5
			5000 * time.Millisecond, // attempt 6: 100ms * 2^6 = 6.4s -> capped at 5s
		}

		for attempt, expected := range expectedCeilings {
			ceiling := b.CalculateCeiling(attempt)
			if ceiling != expected {
				t.Errorf("attempt %d: esperado teto %v, obteve %v", attempt, expected, ceiling)
			}
		}
	})

	// 2. Aplicação do limite máximo
	t.Run("aplicacao_do_limite_maximo", func(t *testing.T) {
		b := rabbitmq.NewBackoffCalculator(initial, maxDelay, nil)

		for attempt := 6; attempt <= 40; attempt += 5 {
			ceiling := b.CalculateCeiling(attempt)
			if ceiling != maxDelay {
				t.Errorf("attempt %d: esperado teto capped em %v, obteve %v", attempt, maxDelay, ceiling)
			}
		}
	})

	// 3. Full jitter dentro dos limites esperados (0 <= delay <= ceiling)
	t.Run("full_jitter_dentro_dos_limites", func(t *testing.T) {
		rng := rand.New(rand.NewSource(42))
		b := rabbitmq.NewBackoffCalculator(initial, maxDelay, rng)

		for attempt := 0; attempt <= 10; attempt++ {
			ceiling := b.CalculateCeiling(attempt)
			for sample := 0; sample < 100; sample++ {
				delay := b.CalculateDelay(attempt)
				if delay < 0 || delay > ceiling {
					t.Fatalf("attempt %d: delay %v fora dos limites [0, %v]", attempt, delay, ceiling)
				}
			}
		}
	})

	// 4. Cancelamento durante a espera
	t.Run("cancelamento_durante_a_espera", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // cancelado previamente

		err := rabbitmq.DefaultSleeper(ctx, 10*time.Second)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("esperava erro context.Canceled, obteve: %v", err)
		}
	})

	// 5. Fonte aleatória determinística
	t.Run("fonte_aleatoria_deterministica", func(t *testing.T) {
		// Mock RNG retornando exatamente o teto máximo (fraction = 1.0)
		maxRNG := &deterministicRNG{fraction: 1.0}
		bMax := rabbitmq.NewBackoffCalculator(initial, maxDelay, maxRNG)

		if delay := bMax.CalculateDelay(0); delay != 100*time.Millisecond {
			t.Errorf("esperava delay exato de 100ms, obteve %v", delay)
		}
		if delay := bMax.CalculateDelay(1); delay != 200*time.Millisecond {
			t.Errorf("esperava delay exato de 200ms, obteve %v", delay)
		}

		// Mock RNG retornando zero (fraction = 0.0)
		zeroRNG := &deterministicRNG{fraction: 0.0}
		bZero := rabbitmq.NewBackoffCalculator(initial, maxDelay, zeroRNG)

		if delay := bZero.CalculateDelay(0); delay != 0 {
			t.Errorf("esperava delay exato de 0ms, obteve %v", delay)
		}
	})

	// 6. Reset e isolamento do estado de retry entre entregas
	t.Run("reset_e_isolamento_entre_entregas", func(t *testing.T) {
		var sleptDelays []time.Duration
		mockSleeper := func(ctx context.Context, d time.Duration) error {
			sleptDelays = append(sleptDelays, d)
			return nil
		}

		// Mock processor que falha 2 vezes e depois tem sucesso
		attemptsMsg1 := 0
		procMsg1 := &backoffMockStreamProcessor{
			processFunc: func(ctx context.Context, ev stream.Event) error {
				attemptsMsg1++
				if attemptsMsg1 <= 2 {
					return driver.ErrBadConn // transient
				}
				return nil
			},
		}

		bDeterministic := rabbitmq.NewBackoffCalculator(100*time.Millisecond, 5*time.Second, &deterministicRNG{fraction: 1.0})
		c := rabbitmq.NewConsumer(nil, procMsg1,
			rabbitmq.WithConsumerBackoff(bDeterministic),
			rabbitmq.WithConsumerSleeper(mockSleeper),
		)

		ack1 := &backoffMockAcknowledger{}
		envBytes := []byte(`{"event_id":"evt-1","aggregate_id":"00000000-0000-0000-0000-000000000001","stream_position":1,"event_type":"DeliveryCreated","payload":{"id":"evt-1","delivery_id":"00000000-0000-0000-0000-000000000001","driver_id":"d1","seq":1,"pickup_lat":0,"pickup_lng":0,"dropoff_lat":0,"dropoff_lng":0}}`)
		d1 := amqp.Delivery{
			Acknowledger: ack1,
			Body:         envBytes,
		}

		err := c.HandleDelivery(context.Background(), d1)
		if err != nil {
			t.Fatalf("primeira entrega falhou: %v", err)
		}
		if !ack1.ackCalled {
			t.Fatalf("esperava ACK na primeira entrega após recuperação")
		}
		if len(sleptDelays) != 2 {
			t.Fatalf("esperava 2 retentativas, obteve %d", len(sleptDelays))
		}
		// Tentativa 0 (teto 100ms), Tentativa 1 (teto 200ms)
		if sleptDelays[0] != 100*time.Millisecond || sleptDelays[1] != 200*time.Millisecond {
			t.Fatalf("delays inesperados na msg 1: %v", sleptDelays)
		}

		// Segunda entrega: deve reiniciar contagem de tentativa a partir de 0!
		sleptDelays = nil
		attemptsMsg2 := 0
		procMsg2 := &backoffMockStreamProcessor{
			processFunc: func(ctx context.Context, ev stream.Event) error {
				attemptsMsg2++
				if attemptsMsg2 == 1 {
					return driver.ErrBadConn // transient
				}
				return nil
			},
		}

		c2 := rabbitmq.NewConsumer(nil, procMsg2,
			rabbitmq.WithConsumerBackoff(bDeterministic),
			rabbitmq.WithConsumerSleeper(mockSleeper),
		)

		ack2 := &backoffMockAcknowledger{}
		envBytes2 := []byte(`{"event_id":"evt-2","aggregate_id":"00000000-0000-0000-0000-000000000002","stream_position":2,"event_type":"DeliveryCreated","payload":{"id":"evt-2","delivery_id":"00000000-0000-0000-0000-000000000002","driver_id":"d2","seq":1,"pickup_lat":0,"pickup_lng":0,"dropoff_lat":0,"dropoff_lng":0}}`)
		d2 := amqp.Delivery{
			Acknowledger: ack2,
			Body:         envBytes2,
		}

		err = c2.HandleDelivery(context.Background(), d2)
		if err != nil {
			t.Fatalf("segunda entrega falhou: %v", err)
		}
		if !ack2.ackCalled {
			t.Fatalf("esperava ACK na segunda entrega")
		}
		if len(sleptDelays) != 1 {
			t.Fatalf("esperava 1 retentativa na msg 2, obteve %d", len(sleptDelays))
		}
		// Deve ter começado exatamente em 100ms (attempt 0), comprovando isolamento total entre entregas!
		if sleptDelays[0] != 100*time.Millisecond {
			t.Fatalf("msg 2 não iniciou com attempt 0 (100ms): %v", sleptDelays[0])
		}
	})
}

// Helpers mock para unit test
type backoffMockAcknowledger struct {
	ackCalled         bool
	nackCalled        bool
	rejectCalled      bool
	lastRejectRequeue bool
	lastNackRequeue   bool
	nackCount         int
	rejectCount       int
}

func (m *backoffMockAcknowledger) Ack(tag uint64, multiple bool) error {
	m.ackCalled = true
	return nil
}

func (m *backoffMockAcknowledger) Nack(tag uint64, multiple bool, requeue bool) error {
	m.nackCalled = true
	m.lastNackRequeue = requeue
	m.nackCount++
	return nil
}

func (m *backoffMockAcknowledger) Reject(tag uint64, requeue bool) error {
	m.rejectCalled = true
	m.lastRejectRequeue = requeue
	m.rejectCount++
	return nil
}

type backoffMockStreamProcessor struct {
	processFunc func(ctx context.Context, ev stream.Event) error
}

func (m *backoffMockStreamProcessor) Process(ctx context.Context, ev stream.Event) error {
	if m.processFunc != nil {
		return m.processFunc(ctx, ev)
	}
	return nil
}
