package rabbitmq_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/dvasconcellos/elo/api/internal/db"
	"github.com/dvasconcellos/elo/api/internal/delivery"
	"github.com/dvasconcellos/elo/api/internal/postgres"
	"github.com/dvasconcellos/elo/api/internal/rabbitmq"
	"github.com/dvasconcellos/elo/api/internal/stream"
	"github.com/dvasconcellos/elo/api/pkg/logger"
)

func getCrashTestAMQPURL() string {
	if u := os.Getenv("TEST_AMQP_URL"); u != "" {
		return u
	}
	if u := os.Getenv("RABBITMQ_URL"); u != "" {
		return u
	}
	return "amqp://root:password@rabbitmq:5672/"
}

func getCrashTestDBDSN() string {
	d := os.Getenv("TEST_DB_DSN")
	if d == "" {
		d = "postgres://root:password@postgres:5432/logistics_test?sslmode=disable"
	}
	if !strings.Contains(d, "search_path") {
		if strings.Contains(d, "?") {
			d += "&search_path=worker_test,public"
		} else {
			d += "?search_path=worker_test,public"
		}
	}
	return d
}

func setupCrashTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbConn, err := sql.Open("postgres", getCrashTestDBDSN())
	if err != nil {
		t.Skipf("cannot connect to postgres: %v", err)
		return nil
	}
	if err := dbConn.Ping(); err != nil {
		t.Skipf("postgres ping failed: %v", err)
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = dbConn.ExecContext(ctx, `
		CREATE SCHEMA IF NOT EXISTS worker_test;
		SET search_path TO worker_test, public;

		DROP TABLE IF EXISTS worker_test.processed_events CASCADE;
		DROP TABLE IF EXISTS worker_test.delivery_read_model CASCADE;
		DROP TABLE IF EXISTS worker_test.projection_checkpoints CASCADE;
		DROP TABLE IF EXISTS worker_test.outbox_events CASCADE;
		DROP TYPE IF EXISTS worker_test.outbox_status CASCADE;

		CREATE TABLE worker_test.processed_events (
			consumer_id TEXT NOT NULL,
			event_id TEXT NOT NULL,
			processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			aggregate_id UUID NOT NULL,
			sequence BIGINT NOT NULL,
			event_type VARCHAR(100) NOT NULL,
			payload_hash VARCHAR(64) NOT NULL,
			PRIMARY KEY (consumer_id, event_id),
			CONSTRAINT uq_processed_events_consumer_agg_seq UNIQUE (consumer_id, aggregate_id, sequence)
		);

		CREATE TYPE worker_test.outbox_status AS ENUM ('PENDING', 'PROCESSED', 'FAILED');

		CREATE TABLE worker_test.delivery_read_model (
			delivery_id UUID PRIMARY KEY,
			driver_id TEXT NOT NULL,
			state TEXT NOT NULL,
			sequence BIGINT NOT NULL,
			pickup_lat DOUBLE PRECISION NOT NULL,
			pickup_lng DOUBLE PRECISION NOT NULL,
			dropoff_lat DOUBLE PRECISION NOT NULL,
			dropoff_lng DOUBLE PRECISION NOT NULL,
			active_route JSONB,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);

		CREATE TABLE worker_test.projection_checkpoints (
			projection_name TEXT PRIMARY KEY,
			last_sequence BIGINT NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);

		CREATE TABLE worker_test.outbox_events (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			aggregate_id UUID NOT NULL,
			aggregate_type VARCHAR(50) NOT NULL,
			event_type VARCHAR(100) NOT NULL,
			payload JSONB NOT NULL,
			status worker_test.outbox_status NOT NULL DEFAULT 'PENDING',
			stream_position BIGSERIAL,
			traceparent VARCHAR(255),
			tracestate VARCHAR(255),
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			processed_at TIMESTAMPTZ
		);
	`)
	if err != nil {
		dbConn.Close()
		t.Fatalf("failed to initialize schema worker_test: %v", err)
	}
	return dbConn
}

// TestGate15A2_CrashAfterSQLCommitBeforeACK implements scenario 1.5A-2:
// 1. Consumer 1 receives a message, commits the projection and checkpoint to PostgreSQL.
// 2. Consumer 1 dies abruptly (closes channel/connection) BEFORE sending basic.ack.
// 3. Broker redelivers the unacknowledged message to Consumer 2 with redelivered=true.
// 4. Consumer 2 boots cold (empty in-memory SequenceValidator) and handles the redelivered message.
// 5. Test verifies:
//    - Broker sets redelivered=true on redelivery.
//    - SQL read model remains unique and uncorrupted.
//    - Checkpoint does not advance or regress incorrectly.
//    - Consumer 2 sends basic.ack, draining the queue to 0.
//    - LiveHub DOES NOT emit duplicate SSE notifications for the already-persisted event.
func TestGate15A2_CrashAfterSQLCommitBeforeACK(t *testing.T) {
	amqpURL := getCrashTestAMQPURL()
	dbConn := setupCrashTestDB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	// Connection for test setup
	adminConn, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Skipf("cannot connect to rabbitmq: %v", err)
		return
	}
	defer adminConn.Close()

	adminCh, err := adminConn.Channel()
	if err != nil {
		t.Fatalf("failed to open admin channel: %v", err)
	}
	defer adminCh.Close()

	testDeliveryID := uuid.New()
	testQueueName := fmt.Sprintf("test.gate15a2.%s", testDeliveryID.String())

	// Declare durable queue
	_, err = adminCh.QueueDeclare(testQueueName, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to declare queue: %v", err)
	}
	defer adminCh.QueueDelete(testQueueName, false, false, false)

	streamPos := int64(100501)
	payloadBytes, _ := json.Marshal(delivery.EventDeliveryCreated{
		ID:         "ev-gate15a2-01",
		DeliveryID: testDeliveryID,
		DriverID:   "driver-ponto-b",
		Seq:        1,
		PickupLat:  -23.5505,
		PickupLng:  -46.6333,
		DropoffLat: -23.5590,
		DropoffLng: -46.6400,
	})

	envBytes, _ := json.Marshal(map[string]interface{}{
		"aggregate_id":    testDeliveryID,
		"stream_position": streamPos,
		"event_type":      "DeliveryCreated",
		"payload":         json.RawMessage(payloadBytes),
	})

	// Publish initial message into the test queue
	err = adminCh.PublishWithContext(context.Background(), "", testQueueName, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         envBytes,
	})
	if err != nil {
		t.Fatalf("failed to publish initial message: %v", err)
	}

	// Setup LiveHub to observe external side effects
	liveHub := stream.NewMemoryLiveHub()
	hubSub, hubUnsub := liveHub.Subscribe(context.Background(), testDeliveryID)
	defer hubUnsub()

	// -------------------------------------------------------------------------
	// FASE 1: Consumer 1 processa, commita no PostgreSQL, mas sofre CRASH antes do ACK
	// -------------------------------------------------------------------------
	t.Log("FASE 1: Consumer 1 iniciando...")
	conn1, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatalf("failed to dial conn1: %v", err)
	}
	ch1, err := conn1.Channel()
	if err != nil {
		t.Fatalf("failed to open ch1: %v", err)
	}

	msgs1, err := ch1.Consume(testQueueName, "consumer-1", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to start consume on ch1: %v", err)
	}

	select {
	case d1 := <-msgs1:
		if d1.Redelivered {
			t.Fatalf("expected d1.Redelivered == false on initial delivery, got true")
		}
		t.Log("Consumer 1 recebeu mensagem. Executando commit no PostgreSQL...")

		// Executa projeção real no banco (simulando a transação concluída com sucesso)
		proj1 := delivery.NewProjection(dbConn, "DeliveryProjection")
		var createdEv delivery.EventDeliveryCreated
		_ = json.Unmarshal(payloadBytes, &createdEv)
		applied, err := proj1.Apply(context.Background(), createdEv, streamPos)
		if err != nil {
			t.Fatalf("proj1.Apply failed: %v", err)
		}
		if !applied {
			t.Fatalf("expected proj1.Apply applied==true on first delivery, got false")
		}

		// Simula notificação da 1ª aplicação bem-sucedida
		liveHub.Publish(stream.Event{
			StreamPosition: streamPos,
			DomainEvent:    createdEv,
		})

		// Consome a notificação legítima inicial da 1ª aplicação
		select {
		case initialNotification := <-hubSub:
			if initialNotification.StreamPosition != streamPos {
				t.Fatalf("unexpected initial notification stream position: %d", initialNotification.StreamPosition)
			}
			t.Log("Notificação legítima da 1ª aplicação recebida no LiveHub.")
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for initial notification on LiveHub")
		}

		// PONTO B: Crash imediato fechando conexão TCP sem chamar d1.Ack()
		t.Log("CRASH: Fechando conexão TCP do Consumer 1 sem chamar d1.Ack()...")
		_ = ch1.Close()
		_ = conn1.Close()

	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for delivery on consumer 1")
	}

	// Verifica estado no banco após Fase 1
	var countPhase1 int
	var seqPhase1 int64
	var statePhase1 string
	err = dbConn.QueryRow("SELECT count(*), max(sequence), max(state) FROM worker_test.delivery_read_model WHERE delivery_id = $1", testDeliveryID).
		Scan(&countPhase1, &seqPhase1, &statePhase1)
	if err != nil || countPhase1 != 1 || seqPhase1 != 1 || statePhase1 != "CREATED" {
		t.Fatalf("Phase 1 DB inconsistent: count=%d, seq=%d, state=%s (err: %v)", countPhase1, seqPhase1, statePhase1, err)
	}

	var cpPhase1 int64
	err = dbConn.QueryRow("SELECT last_sequence FROM worker_test.projection_checkpoints WHERE projection_name = 'DeliveryProjection'").Scan(&cpPhase1)
	if err != nil || cpPhase1 != streamPos {
		t.Fatalf("Phase 1 checkpoint inconsistent: cp=%d (expected %d, err: %v)", cpPhase1, streamPos, err)
	}
	t.Logf("Fase 1 confirmada no DB: read model seq=%d, checkpoint=%d", seqPhase1, cpPhase1)

	// -------------------------------------------------------------------------
	// FASE 2 & 3: Broker re-entrega mensagem não confirmada para Consumer 2
	// -------------------------------------------------------------------------
	t.Log("FASE 2: Conectando Consumer 2 (simulando processo novo pós-restart)...")
	conn2, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatalf("failed to dial conn2: %v", err)
	}
	defer conn2.Close()

	ch2, err := conn2.Channel()
	if err != nil {
		t.Fatalf("failed to open ch2: %v", err)
	}
	defer ch2.Close()

	msgs2, err := ch2.Consume(testQueueName, "consumer-2", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to start consume on ch2: %v", err)
	}

	select {
	case d2 := <-msgs2:
		// CRITÉRIO 1: O broker precisa ter marcado redelivered = true
		if !d2.Redelivered {
			t.Fatalf("CRITÉRIO 1 FALHOU: broker não marcou redelivered=true na reentrega após crash!")
		}
		t.Logf("CRITÉRIO 1 APROVADO: Mensagem reentregue pelo broker com redelivered=%v", d2.Redelivered)

		// Consumer 2 inicia do zero (validador em memória vazio)
		coldValidator := stream.NewSequenceValidator()
		queries := db.New(dbConn)
		eventStore := postgres.NewEventStore(queries)
		proj2 := delivery.NewProjection(dbConn, "DeliveryProjection")
		recovery := stream.NewGapRecovery(eventStore, coldValidator, proj2, liveHub)
		streamConsumer := stream.NewConsumer(coldValidator, recovery, proj2, liveHub)

		testLogger := logger.NewZapLogger("test-consumer-2", false)
		rabbitConsumer2 := rabbitmq.NewConsumer(ch2, streamConsumer, rabbitmq.WithConsumerLogger(testLogger))

		// Processa a mensagem reentregue
		t.Log("Consumer 2 processando a mensagem reentregue...")
		if err := rabbitConsumer2.HandleDelivery(context.Background(), d2); err != nil {
			t.Fatalf("Consumer 2 falhou ao processar reentrega: %v", err)
		}
		t.Log("Consumer 2 processou e enviou ACK com sucesso.")

	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for broker redelivery on consumer 2")
	}

	// -------------------------------------------------------------------------
	// FASE 4: Asserções de Consistência e Idempotência
	// -------------------------------------------------------------------------

	// CRITÉRIO 2: Read Model no PostgreSQL sem duplicidade nem regressão
	var countFinal int
	var seqFinal int64
	var stateFinal string
	err = dbConn.QueryRow("SELECT count(*), max(sequence), max(state) FROM worker_test.delivery_read_model WHERE delivery_id = $1", testDeliveryID).
		Scan(&countFinal, &seqFinal, &stateFinal)
	if err != nil || countFinal != 1 || seqFinal != 1 || stateFinal != "CREATED" {
		t.Fatalf("CRITÉRIO 2 FALHOU: read model corrompido pós-reentrega: count=%d, seq=%d, state=%s", countFinal, seqFinal, stateFinal)
	}
	t.Logf("CRITÉRIO 2 APROVADO: Read Model preservado sem duplicatas físicas (count=%d, seq=%d)", countFinal, seqFinal)

	// CRITÉRIO 3: Checkpoint não avança nem regride
	var cpFinal int64
	err = dbConn.QueryRow("SELECT last_sequence FROM worker_test.projection_checkpoints WHERE projection_name = 'DeliveryProjection'").Scan(&cpFinal)
	if err != nil || cpFinal != streamPos {
		t.Fatalf("CRITÉRIO 3 FALHOU: checkpoint alterado indevidamente na reentrega: cp=%d (esperado %d)", cpFinal, streamPos)
	}
	t.Logf("CRITÉRIO 3 APROVADO: Checkpoint preservado estritamente em %d", cpFinal)

	// CRITÉRIO 4: Fila zerada no RabbitMQ
	qStatus, err := adminCh.QueueInspect(testQueueName)
	if err != nil {
		t.Fatalf("falha ao inspecionar fila: %v", err)
	}
	if qStatus.Messages != 0 {
		t.Fatalf("CRITÉRIO 4 FALHOU: fila ainda possui %d mensagens após ACK do Consumer 2", qStatus.Messages)
	}
	t.Log("CRITÉRIO 4 APROVADO: Fila drenada e zerada após ACK.")

	// CRITÉRIO 5 (EIXO 2): LiveHub NÃO pode emitir notificação duplicada na reentrega!
	select {
	case dup := <-hubSub:
		t.Fatalf("CRITÉRIO 5 FALHOU (VIOLAÇÃO DE IDEMPOTÊNCIA DO LIVEHUB): "+
			"LiveHub emitiu notificação duplicada na reentrega de evento já persistido! (seq=%d, stream_pos=%d)",
			dup.DomainEvent.Sequence(), dup.StreamPosition)
	case <-time.After(500 * time.Millisecond):
		t.Log("CRITÉRIO 5 APROVADO: Zero notificações duplicadas emitidas no LiveHub após reentrega.")
	}

	t.Log("=== TESTE 1.5A-2 (PONTO B: CRASH PÓS-COMMIT PRÉ-ACK) CONCLUÍDO COM SUCESSO ===")
}

// crashPreCommitTarget simula um target que inicia uma transação real no PostgreSQL,
// executa operações de escrita no buffer local da transação, e sofre um crash fatal do processo
// antes de chamar tx.Commit(), severando a conexão TCP com o RabbitMQ e com o PostgreSQL sem ACK.
type crashPreCommitTarget struct {
	db           *sql.DB
	ch           *amqp.Channel
	conn         *amqp.Connection
	observedInTx bool
}

func (p *crashPreCommitTarget) Apply(ctx context.Context, ev delivery.DomainEvent, streamPosition int64) (bool, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	q := db.New(tx)
	if e, ok := ev.(delivery.EventDeliveryCreated); ok {
		// Executa escrita preliminar dentro do escopo da transação não comitada
		_, err = q.InsertDeliveryReadModel(ctx, db.InsertDeliveryReadModelParams{
			DeliveryID: e.DeliveryID,
			DriverID:   e.DriverID,
			State:      "CREATED",
			Sequence:   e.Seq,
			PickupLat:  e.PickupLat,
			PickupLng:  e.PickupLng,
			DropoffLat: e.DropoffLat,
			DropoffLng: e.DropoffLng,
		})
		if err != nil {
			return false, err
		}

		// Evidência de que a escrita esteve temporariamente visível dentro da transação tx
		var countInTx int
		_ = tx.QueryRowContext(ctx, "SELECT count(*) FROM worker_test.delivery_read_model WHERE delivery_id = $1", e.DeliveryID).Scan(&countInTx)
		if countInTx == 1 {
			p.observedInTx = true
		}
	}

	// CRASH FATAL PRÉ-COMMIT:
	// A transação NÃO é comitada (tx.Commit() nunca é alcançado).
	// O canal e a conexão TCP com o broker AMQP são fechados abruptamente sem envio de basic.ack ou basic.nack.
	_ = p.ch.Close()
	_ = p.conn.Close()

	// Retorna erro simulando a falha catastrófica da transação
	return false, errors.New("fatal process crash during SQL transaction before commit")
}

// TestGate15A1_CrashBeforeSQLCommit implementa o cenário 1.5A-1 (Ponto A):
// 1. Mensagem recebida pelo Consumer 1.
// 2. Transação SQL é aberta, dados são escritos no buffer da transação, mas o processo sofre crash ANTES de tx.Commit().
// 3. Verificações pré-restart:
//    - Critério 1: Transação sofreu rollback, zero alterações parciais persistidas no PostgreSQL.
//    - Critério 2: Checkpoint da projeção não avançou (0 registros).
//    - Critério 3: LiveHub não emitiu nenhuma notificação prematura durante a tentativa que falhou.
// 4. Broker re-entrega a mensagem para Consumer 2 com flag redelivered=true (Critério 4: Mensagem recuperável no RabbitMQ).
// 5. Consumer 2 (novo processo) processa com sucesso:
//    - Critério 5: Evento é aplicado com integridade no PostgreSQL pós-restart (count=1, seq=1).
//    - Critério 6: Checkpoint avança para a posição correspondente.
//    - Critério 7: Fila drenada e zerada após ACK do Consumer 2.
//    - Critério 8: LiveHub emite EXATAMENTE UMA notificação legítima em todo o ciclo de vida.
func TestGate15A1_CrashBeforeSQLCommit(t *testing.T) {
	amqpURL := getCrashTestAMQPURL()
	dbConn := setupCrashTestDB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	// Conexão administrativa para setup do teste
	adminConn, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Skipf("cannot connect to rabbitmq: %v", err)
		return
	}
	defer adminConn.Close()

	adminCh, err := adminConn.Channel()
	if err != nil {
		t.Fatalf("failed to open admin channel: %v", err)
	}
	defer adminCh.Close()

	testDeliveryID := uuid.New()
	testQueueName := fmt.Sprintf("test.gate15a1.%s", testDeliveryID.String())

	// Declara fila durável para o teste 1.5A-1
	_, err = adminCh.QueueDeclare(testQueueName, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to declare queue: %v", err)
	}
	defer adminCh.QueueDelete(testQueueName, false, false, false)

	streamPos := int64(100601)
	payloadBytes, _ := json.Marshal(delivery.EventDeliveryCreated{
		ID:         "ev-gate15a1-01",
		DeliveryID: testDeliveryID,
		DriverID:   "driver-ponto-a",
		Seq:        1,
		PickupLat:  -23.5505,
		PickupLng:  -46.6333,
		DropoffLat: -23.5590,
		DropoffLng: -46.6400,
	})

	envBytes, _ := json.Marshal(map[string]interface{}{
		"aggregate_id":    testDeliveryID,
		"stream_position": streamPos,
		"event_type":      "DeliveryCreated",
		"payload":         json.RawMessage(payloadBytes),
	})

	// Publica a mensagem inicial no RabbitMQ com persistência
	err = adminCh.PublishWithContext(context.Background(), "", testQueueName, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         envBytes,
	})
	if err != nil {
		t.Fatalf("failed to publish initial message: %v", err)
	}

	// Setup do LiveHub para rastrear efeitos colaterais ao longo de todo o ciclo
	liveHub := stream.NewMemoryLiveHub()
	hubSub, hubUnsub := liveHub.Subscribe(context.Background(), testDeliveryID)
	defer hubUnsub()

	// -------------------------------------------------------------------------
	// FASE 1: Consumer 1 consome a mensagem e sofre crash durante a transação pré-commit
	// -------------------------------------------------------------------------
	t.Log("FASE 1: Consumer 1 iniciando com crash pré-commit configurado...")
	conn1, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatalf("failed to dial conn1: %v", err)
	}

	ch1, err := conn1.Channel()
	if err != nil {
		_ = conn1.Close()
		t.Fatalf("failed to open ch1: %v", err)
	}

	msgs1, err := ch1.Consume(testQueueName, "consumer-1", false, false, false, false, nil)
	if err != nil {
		_ = ch1.Close()
		_ = conn1.Close()
		t.Fatalf("failed to start consume on ch1: %v", err)
	}

	crashTarget := &crashPreCommitTarget{
		db:   dbConn,
		ch:   ch1,
		conn: conn1,
	}

	validator1 := stream.NewSequenceValidator()
	queries1 := db.New(dbConn)
	eventStore1 := postgres.NewEventStore(queries1)
	recovery1 := stream.NewGapRecovery(eventStore1, validator1, crashTarget, liveHub)
	streamConsumer1 := stream.NewConsumer(validator1, recovery1, crashTarget, liveHub)

	testLogger1 := logger.NewZapLogger("test-consumer-1-crash", false)
	rabbitConsumer1 := rabbitmq.NewConsumer(ch1, streamConsumer1, rabbitmq.WithConsumerLogger(testLogger1))

	select {
	case d1 := <-msgs1:
		if d1.Redelivered {
			t.Fatalf("esperado d1.Redelivered == false na entrega inicial, obtido true")
		}
		t.Log("Consumer 1 recebeu mensagem. Executando processamento que sofrerá crash pré-commit...")

		// Executa HandleDelivery: entrará em crashPreCommitTarget.Apply, executará escrita no buffer tx,
		// fechará TCP com o broker e comitará Rollback.
		handleErr := rabbitConsumer1.HandleDelivery(context.Background(), d1)
		if handleErr == nil {
			t.Fatal("esperava erro de crash no HandleDelivery do Consumer 1, obtido nil")
		}
		t.Logf("Consumer 1 sofreu crash conforme planejado: %v", handleErr)

		if !crashTarget.observedInTx {
			t.Fatal("escrita no buffer da transação não foi observada antes do crash")
		}
		t.Log("Evidência confirmada: dados estiveram presentes no buffer isolado de tx antes do crash.")

	case <-time.After(5 * time.Second):
		t.Fatal("timeout esperando entrega inicial no Consumer 1")
	}

	// -------------------------------------------------------------------------
	// AVALIAÇÃO INTERMEDIÁRIA: Invariantes no PostgreSQL e LiveHub pós-crash
	// -------------------------------------------------------------------------
	t.Log("Verificando invariantes de rollback e silêncio pós-crash...")

	// CRITÉRIO 1: A transação não deixa alterações parciais (rollback limpo)
	var countPostCrash int
	err = dbConn.QueryRow("SELECT count(*) FROM worker_test.delivery_read_model WHERE delivery_id = $1", testDeliveryID).Scan(&countPostCrash)
	if err != nil || countPostCrash != 0 {
		t.Fatalf("CRITÉRIO 1 FALHOU: transação deixou alterações parciais no PostgreSQL! count=%d (err: %v)", countPostCrash, err)
	}
	t.Log("CRITÉRIO 1 APROVADO: Transação sofreu rollback limpo, zero linhas parciais no PostgreSQL.")

	// CRITÉRIO 2: O checkpoint não avança
	var cpCount int
	err = dbConn.QueryRow("SELECT count(*) FROM worker_test.projection_checkpoints WHERE projection_name = 'DeliveryProjection'").Scan(&cpCount)
	if err != nil || cpCount != 0 {
		t.Fatalf("CRITÉRIO 2 FALHOU: checkpoint gravado indevidamente antes do commit! count=%d (err: %v)", cpCount, err)
	}
	t.Log("CRITÉRIO 2 APROVADO: Checkpoint da projeção não avançou (0 registros gravados no PostgreSQL).")

	// CRITÉRIO 3: LiveHub não emitiu notificação na falha pré-commit
	select {
	case notif := <-hubSub:
		t.Fatalf("CRITÉRIO 3 FALHOU: LiveHub emitiu notificação prematura na falha pré-commit: %+v", notif)
	default:
		t.Log("CRITÉRIO 3 APROVADO: LiveHub permaneceu estritamente em silêncio durante o crash pré-commit.")
	}

	// -------------------------------------------------------------------------
	// FASE 2: Broker re-entrega mensagem não confirmada para Consumer 2
	// -------------------------------------------------------------------------
	t.Log("FASE 2: Conectando Consumer 2 (simulando processo novo pós-restart)...")
	conn2, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatalf("failed to dial conn2: %v", err)
	}
	defer conn2.Close()

	ch2, err := conn2.Channel()
	if err != nil {
		t.Fatalf("failed to open ch2: %v", err)
	}
	defer ch2.Close()

	msgs2, err := ch2.Consume(testQueueName, "consumer-2", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to start consume on ch2: %v", err)
	}

	select {
	case d2 := <-msgs2:
		// CRITÉRIO 4: A mensagem permanece recuperável no RabbitMQ com redelivered=true
		if !d2.Redelivered {
			t.Fatalf("CRITÉRIO 4 FALHOU: broker não marcou redelivered=true na reentrega após crash pré-commit!")
		}
		t.Logf("CRITÉRIO 4 APROVADO: Mensagem permaneceu 100%% recuperável e foi reentregue pelo broker com redelivered=%v", d2.Redelivered)

		// Consumer 2 inicia do zero com pipeline normal de produção
		coldValidator := stream.NewSequenceValidator()
		queries2 := db.New(dbConn)
		eventStore2 := postgres.NewEventStore(queries2)
		proj2 := delivery.NewProjection(dbConn, "DeliveryProjection")
		recovery2 := stream.NewGapRecovery(eventStore2, coldValidator, proj2, liveHub)
		streamConsumer2 := stream.NewConsumer(coldValidator, recovery2, proj2, liveHub)

		testLogger2 := logger.NewZapLogger("test-consumer-2", false)
		rabbitConsumer2 := rabbitmq.NewConsumer(ch2, streamConsumer2, rabbitmq.WithConsumerLogger(testLogger2))

		// Processa a mensagem reentregue
		t.Log("Consumer 2 processando a mensagem reentregue com projeção de produção...")
		if err := rabbitConsumer2.HandleDelivery(context.Background(), d2); err != nil {
			t.Fatalf("Consumer 2 falhou ao processar reentrega: %v", err)
		}
		t.Log("Consumer 2 processou e enviou ACK com sucesso.")

	case <-time.After(5 * time.Second):
		t.Fatal("timeout esperando broker redelivery no Consumer 2")
	}

	// -------------------------------------------------------------------------
	// FASE 3: Asserções Finais de Consistência, Persistência e Notificação Única
	// -------------------------------------------------------------------------

	// CRITÉRIO 5: Após o restart, o evento é aplicado com integridade no PostgreSQL
	var countFinal int
	var seqFinal int64
	var stateFinal string
	err = dbConn.QueryRow("SELECT count(*), max(sequence), max(state) FROM worker_test.delivery_read_model WHERE delivery_id = $1", testDeliveryID).
		Scan(&countFinal, &seqFinal, &stateFinal)
	if err != nil || countFinal != 1 || seqFinal != 1 || stateFinal != "CREATED" {
		t.Fatalf("CRITÉRIO 5 FALHOU: read model não foi devidamente persistido pós-restart: count=%d, seq=%d, state=%s (err: %v)",
			countFinal, seqFinal, stateFinal, err)
	}
	t.Logf("CRITÉRIO 5 APROVADO: Evento aplicado com integridade pós-restart (count=%d, seq=%d, state=%s).", countFinal, seqFinal, stateFinal)

	// CRITÉRIO 6: Checkpoint atualizado com precisão
	var cpFinal int64
	err = dbConn.QueryRow("SELECT last_sequence FROM worker_test.projection_checkpoints WHERE projection_name = 'DeliveryProjection'").Scan(&cpFinal)
	if err != nil || cpFinal != streamPos {
		t.Fatalf("CRITÉRIO 6 FALHOU: checkpoint inconsistente pós-restart: cp=%d (esperado %d, err: %v)", cpFinal, streamPos, err)
	}
	t.Logf("CRITÉRIO 6 APROVADO: Checkpoint atualizado com precisão para %d.", cpFinal)

	// CRITÉRIO 7: Fila RabbitMQ drenada e zerada após ACK
	qStatus, err := adminCh.QueueInspect(testQueueName)
	if err != nil {
		t.Fatalf("falha ao inspecionar fila: %v", err)
	}
	if qStatus.Messages != 0 {
		t.Fatalf("CRITÉRIO 7 FALHOU: fila ainda possui %d mensagens após ACK do Consumer 2", qStatus.Messages)
	}
	t.Log("CRITÉRIO 7 APROVADO: Fila drenada e zerada após ACK do Consumer 2.")

	// CRITÉRIO 8: LiveHub emite EXATAMENTE UMA notificação legítima em todo o ciclo de vida
	select {
	case notif := <-hubSub:
		if notif.StreamPosition != streamPos || notif.DomainEvent.Sequence() != 1 {
			t.Fatalf("CRITÉRIO 8 FALHOU: notificação com payload incorreto: pos=%d, seq=%d", notif.StreamPosition, notif.DomainEvent.Sequence())
		}
		t.Logf("Notificação legítima recebida no LiveHub: stream_pos=%d, seq=%d", notif.StreamPosition, notif.DomainEvent.Sequence())
	case <-time.After(2 * time.Second):
		t.Fatal("CRITÉRIO 8 FALHOU: timeout esperando notificação legítima no LiveHub pós-restart")
	}

	// Garante ausência total de notificações extras ou duplicadas
	select {
	case extra := <-hubSub:
		t.Fatalf("CRITÉRIO 8 FALHOU: LiveHub emitiu notificação duplicada/inesperada! %+v", extra)
	case <-time.After(500 * time.Millisecond):
		t.Log("CRITÉRIO 8 APROVADO: LiveHub emitiu EXATAMENTE UMA notificação em todo o ciclo (crash pré-commit + restart com sucesso).")
	}

	t.Log("=== TESTE 1.5A-1 (PONTO A: CRASH PRÉ-COMMIT COM RECUPERAÇÃO) CONCLUÍDO COM SUCESSO ===")
}

func publishCrashTestEnvelope(t *testing.T, dbConn *sql.DB, ch *amqp.Channel, queueName string, aggID uuid.UUID, streamPos int64, eventType string, eventPayload interface{}) {
	t.Helper()
	payloadBytes, err := json.Marshal(eventPayload)
	if err != nil {
		t.Fatalf("failed to marshal event payload: %v", err)
	}

	if dbConn != nil {
		_, _ = dbConn.Exec(`
			INSERT INTO worker_test.outbox_events (aggregate_id, aggregate_type, event_type, payload, stream_position)
			VALUES ($1, 'delivery', $2, $3, $4)
			ON CONFLICT DO NOTHING
		`, aggID, eventType, payloadBytes, streamPos)
	}

	envBytes, err := json.Marshal(map[string]interface{}{
		"aggregate_id":    aggID,
		"stream_position": streamPos,
		"event_type":      eventType,
		"payload":         json.RawMessage(payloadBytes),
	})
	if err != nil {
		t.Fatalf("failed to marshal envelope: %v", err)
	}
	err = ch.PublishWithContext(context.Background(), "", queueName, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         envBytes,
	})
	if err != nil {
		t.Fatalf("failed to publish crash test event: %v", err)
	}
}

// TestGate15A3a_FailureBeforeACKReachesBroker implementa o cenário 1.5A-3a:
// 1. Mensagem 1 (Seq 1) processada e comitada no PostgreSQL; notificação 1 emitida no LiveHub.
// 2. Interrupção abrupta da conexão ANTES de o basic.ack chegar ao broker (conexão fecha sem ACK processado).
// 3. Broker RabbitMQ detecta fechamento do canal sem ACK e re-enfileira a mensagem 1.
// 4. Consumer 2 (novo processo) recebe a mensagem reentregue com redelivered=true.
// 5. Projeção reconhece duplicata (ON CONFLICT DO NOTHING, applied=false) e LiveHub suprime nova notificação.
// 6. Consumer 2 confirma o evento 1 com basic.ack.
// 7. Evento subsequente (Seq 2: DriverArrivedAtPickup) é publicado, consumido e aplicado com sucesso (Critério 6).
// 8. Asserções finais:
//    - Read model: exatamente 1 entrega, estado AT_PICKUP, seq=2.
//    - Checkpoint: avança para a posição da mensagem 2.
//    - Fila RabbitMQ: drenada e zerada.
//    - LiveHub: exatamente 2 notificações emitidas ao todo (1 por evento legítimo).
func TestGate15A3a_FailureBeforeACKReachesBroker(t *testing.T) {
	amqpURL := getCrashTestAMQPURL()
	dbConn := setupCrashTestDB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	adminConn, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Skipf("cannot connect to rabbitmq: %v", err)
		return
	}
	defer adminConn.Close()

	adminCh, err := adminConn.Channel()
	if err != nil {
		t.Fatalf("failed to open admin channel: %v", err)
	}
	defer adminCh.Close()

	testDeliveryID := uuid.New()
	testQueueName := fmt.Sprintf("test.gate15a3a.%s", testDeliveryID.String())

	_, err = adminCh.QueueDeclare(testQueueName, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to declare queue: %v", err)
	}
	defer adminCh.QueueDelete(testQueueName, false, false, false)

	streamPos1 := int64(100701)
	ev1 := delivery.EventDeliveryCreated{
		ID:         "ev-gate15a3a-01",
		DeliveryID: testDeliveryID,
		DriverID:   "driver-ponto-c1",
		Seq:        1,
		PickupLat:  -23.5505,
		PickupLng:  -46.6333,
		DropoffLat: -23.5590,
		DropoffLng: -46.6400,
	}
	publishCrashTestEnvelope(t, dbConn, adminCh, testQueueName, testDeliveryID, streamPos1, "DeliveryCreated", ev1)

	liveHub := stream.NewMemoryLiveHub()
	hubSub, hubUnsub := liveHub.Subscribe(context.Background(), testDeliveryID)
	defer hubUnsub()

	// -------------------------------------------------------------------------
	// FASE 1: Consumer 1 processa, comita no PostgreSQL, notifica LiveHub, mas cai antes do ACK
	// -------------------------------------------------------------------------
	t.Log("FASE 1: Consumer 1 processando Evento 1...")
	conn1, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatalf("failed to dial conn1: %v", err)
	}
	ch1, err := conn1.Channel()
	if err != nil {
		_ = conn1.Close()
		t.Fatalf("failed to open ch1: %v", err)
	}

	msgs1, err := ch1.Consume(testQueueName, "consumer-1", false, false, false, false, nil)
	if err != nil {
		_ = ch1.Close()
		_ = conn1.Close()
		t.Fatalf("failed to start consume on ch1: %v", err)
	}

	select {
	case d1 := <-msgs1:
		if d1.Redelivered {
			t.Fatalf("esperado d1.Redelivered == false na entrega inicial")
		}

		proj1 := delivery.NewProjection(dbConn, "DeliveryProjection")
		applied, err := proj1.Apply(context.Background(), ev1, streamPos1)
		if err != nil || !applied {
			t.Fatalf("proj1.Apply falhou: applied=%v, err=%v", applied, err)
		}

		// Notificação legítima da 1ª aplicação
		liveHub.Publish(stream.Event{StreamPosition: streamPos1, DomainEvent: ev1})

		select {
		case notif1 := <-hubSub:
			if notif1.StreamPosition != streamPos1 || notif1.DomainEvent.Sequence() != 1 {
				t.Fatalf("notificação 1 incorreta: %+v", notif1)
			}
			t.Log("Notificação legítima do Evento 1 recebida no LiveHub.")
		case <-time.After(2 * time.Second):
			t.Fatal("timeout esperando notificação 1 no LiveHub")
		}

		// INTERRUPÇÃO DA CONEXÃO ANTES DE O ACK CHEGAR AO BROKER:
		t.Log("INTERRUPÇÃO: Fechando conexão TCP sem enviar ACK ao broker...")
		_ = ch1.Close()
		_ = conn1.Close()

	case <-time.After(5 * time.Second):
		t.Fatal("timeout esperando entrega inicial no Consumer 1")
	}

	// -------------------------------------------------------------------------
	// FASE 2: Consumer 2 conecta; Broker re-entrega mensagem 1 (redelivered=true)
	// -------------------------------------------------------------------------
	t.Log("FASE 2: Conectando Consumer 2 pós-interrupção...")
	conn2, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatalf("failed to dial conn2: %v", err)
	}
	defer conn2.Close()

	ch2, err := conn2.Channel()
	if err != nil {
		t.Fatalf("failed to open ch2: %v", err)
	}
	defer ch2.Close()

	msgs2, err := ch2.Consume(testQueueName, "consumer-2", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to start consume on ch2: %v", err)
	}

	coldValidator := stream.NewSequenceValidator()
	queries2 := db.New(dbConn)
	eventStore2 := postgres.NewEventStore(queries2)
	proj2 := delivery.NewProjection(dbConn, "DeliveryProjection")
	recovery2 := stream.NewGapRecovery(eventStore2, coldValidator, proj2, liveHub)
	streamConsumer2 := stream.NewConsumer(coldValidator, recovery2, proj2, liveHub)

	testLogger2 := logger.NewZapLogger("test-consumer-2", false)
	rabbitConsumer2 := rabbitmq.NewConsumer(ch2, streamConsumer2, rabbitmq.WithConsumerLogger(testLogger2))

	select {
	case d2 := <-msgs2:
		// CRITÉRIO 4 (1.5A-3a): Broker re-entregou a mensagem não confirmada
		if !d2.Redelivered {
			t.Fatalf("CRITÉRIO 4 FALHOU (1.5A-3a): broker não marcou redelivered=true na reentrega após ACK não recebido!")
		}
		t.Logf("CRITÉRIO 4 APROVADO (1.5A-3a): Mensagem reentregue pelo broker com redelivered=%v", d2.Redelivered)

		// Processa a reentrega: deve ser reconhecida como duplicata e confirmada com ACK
		if err := rabbitConsumer2.HandleDelivery(context.Background(), d2); err != nil {
			t.Fatalf("Consumer 2 falhou ao processar reentrega: %v", err)
		}
		t.Log("Consumer 2 processou duplicata com sucesso e enviou ACK.")

	case <-time.After(5 * time.Second):
		t.Fatal("timeout esperando broker redelivery no Consumer 2")
	}

	// CRITÉRIO 5: LiveHub NÃO publicou notificação duplicada na reentrega
	select {
	case dup := <-hubSub:
		t.Fatalf("CRITÉRIO 5 FALHOU: LiveHub emitiu notificação duplicada para Evento 1 na reentrega! %+v", dup)
	case <-time.After(500 * time.Millisecond):
		t.Log("CRITÉRIO 5 APROVADO: Zero notificações duplicadas emitidas no LiveHub na reentrega.")
	}

	// -------------------------------------------------------------------------
	// FASE 3: Publicação e processamento de mensagem posterior (Critério 6)
	// -------------------------------------------------------------------------
	t.Log("FASE 3: Publicando evento posterior (Seq 2: DriverArrivedAtPickup)...")
	streamPos2 := int64(100702)
	ev2 := delivery.EventDriverArrivedAtPickup{
		ID:         "ev-gate15a3a-02",
		DeliveryID: testDeliveryID,
		Seq:        2,
	}
	publishCrashTestEnvelope(t, dbConn, adminCh, testQueueName, testDeliveryID, streamPos2, "DriverArrivedAtPickup", ev2)

	select {
	case d3 := <-msgs2:
		if d3.Redelivered {
			t.Fatalf("esperado d3.Redelivered == false para evento novo 2")
		}
		if err := rabbitConsumer2.HandleDelivery(context.Background(), d3); err != nil {
			t.Fatalf("Consumer 2 falhou ao processar evento posterior (Seq 2): %v", err)
		}
		t.Log("Consumer 2 processou evento subsequente (Seq 2) com sucesso.")

	case <-time.After(5 * time.Second):
		t.Fatal("timeout esperando evento subsequente no Consumer 2")
	}

	// CRITÉRIO 6 & 3: Notificação legítima do Evento 2 recebida no LiveHub
	select {
	case notif2 := <-hubSub:
		if notif2.StreamPosition != streamPos2 || notif2.DomainEvent.Sequence() != 2 {
			t.Fatalf("notificação 2 incorreta: pos=%d, seq=%d", notif2.StreamPosition, notif2.DomainEvent.Sequence())
		}
		t.Logf("CRITÉRIO 6 APROVADO: Evento subsequente (Seq 2) produziu notificação legítima: stream_pos=%d, seq=%d",
			notif2.StreamPosition, notif2.DomainEvent.Sequence())
	case <-time.After(2 * time.Second):
		t.Fatal("timeout esperando notificação 2 no LiveHub")
	}

	// Nenhuma outra notificação no LiveHub
	select {
	case extra := <-hubSub:
		t.Fatalf("notificação inesperada no LiveHub: %+v", extra)
	case <-time.After(500 * time.Millisecond):
		t.Log("Total de notificações no LiveHub verificado: EXATAMENTE 2 (1 por evento legítimo).")
	}

	// -------------------------------------------------------------------------
	// FASE 4: Asserções Finais no PostgreSQL e Broker
	// -------------------------------------------------------------------------
	// CRITÉRIO 1: Read Model atualizado para estado do evento 2 sem duplicatas físicas
	var countFinal int
	var seqFinal int64
	var stateFinal string
	err = dbConn.QueryRow("SELECT count(*), max(sequence), max(state) FROM worker_test.delivery_read_model WHERE delivery_id = $1", testDeliveryID).
		Scan(&countFinal, &seqFinal, &stateFinal)
	if err != nil || countFinal != 1 || seqFinal != 2 || stateFinal != "AT_PICKUP" {
		t.Fatalf("CRITÉRIO 1 FALHOU: read model corrompido pós-1.5A-3a: count=%d, seq=%d, state=%s", countFinal, seqFinal, stateFinal)
	}
	t.Logf("CRITÉRIO 1 APROVADO: Read model preservado com estado atualizado (count=%d, seq=%d, state=%s).", countFinal, seqFinal, stateFinal)

	// CRITÉRIO 2: Checkpoint avançado para a posição da mensagem 2
	var cpFinal int64
	err = dbConn.QueryRow("SELECT last_sequence FROM worker_test.projection_checkpoints WHERE projection_name = 'DeliveryProjection'").Scan(&cpFinal)
	if err != nil || cpFinal != streamPos2 {
		t.Fatalf("CRITÉRIO 2 FALHOU: checkpoint inconsistente: cp=%d (esperado %d)", cpFinal, streamPos2)
	}
	t.Logf("CRITÉRIO 2 APROVADO: Checkpoint avançado consistentemente para %d.", cpFinal)

	// Fila zerada no broker
	qStatus, err := adminCh.QueueInspect(testQueueName)
	if err != nil || qStatus.Messages != 0 {
		t.Fatalf("fila não foi zerada: messages=%d (err: %v)", qStatus.Messages, err)
	}
	t.Log("Fila RabbitMQ drenada e zerada com sucesso.")

	t.Log("=== TESTE 1.5A-3a (INTERRUPÇÃO PRÉ-ACK COM REDELIVERY E SEQUÊNCIA SUBSEQUENTE) APROVADO ===")
}

// TestGate15A3b_FailureAfterACKProcessedByBroker implementa o cenário 1.5A-3b:
// 1. Mensagem 1 (Seq 1) processada, comitada no PostgreSQL e confirmada com basic.ack.
// 2. Confirmação explícita por observação direta do estado do broker de que o ACK foi processado (fila zerada).
// 3. Interrupção na conexão do Consumer 1 imediatamente após o ACK ser processado.
// 4. Consumer 2 conecta; Critério 4 (1.5A-3b): Mensagem 1 NÃO é reentregue pelo broker (foi definitivamente removida).
// 5. Evento subsequente (Seq 2: DriverArrivedAtPickup) é publicado, consumido e aplicado com sucesso (Critério 6).
// 6. Asserções finais:
//    - Read model: exatamente 1 entrega, estado AT_PICKUP, seq=2.
//    - Checkpoint: avança para a posição da mensagem 2.
//    - LiveHub: exatamente 2 notificações emitidas ao todo (1 por evento legítimo).
func TestGate15A3b_FailureAfterACKProcessedByBroker(t *testing.T) {
	amqpURL := getCrashTestAMQPURL()
	dbConn := setupCrashTestDB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	adminConn, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Skipf("cannot connect to rabbitmq: %v", err)
		return
	}
	defer adminConn.Close()

	adminCh, err := adminConn.Channel()
	if err != nil {
		t.Fatalf("failed to open admin channel: %v", err)
	}
	defer adminCh.Close()

	testDeliveryID := uuid.New()
	testQueueName := fmt.Sprintf("test.gate15a3b.%s", testDeliveryID.String())

	_, err = adminCh.QueueDeclare(testQueueName, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to declare queue: %v", err)
	}
	defer adminCh.QueueDelete(testQueueName, false, false, false)

	streamPos1 := int64(100801)
	ev1 := delivery.EventDeliveryCreated{
		ID:         "ev-gate15a3b-01",
		DeliveryID: testDeliveryID,
		DriverID:   "driver-ponto-c2",
		Seq:        1,
		PickupLat:  -23.5505,
		PickupLng:  -46.6333,
		DropoffLat: -23.5590,
		DropoffLng: -46.6400,
	}
	publishCrashTestEnvelope(t, dbConn, adminCh, testQueueName, testDeliveryID, streamPos1, "DeliveryCreated", ev1)

	liveHub := stream.NewMemoryLiveHub()
	hubSub, hubUnsub := liveHub.Subscribe(context.Background(), testDeliveryID)
	defer hubUnsub()

	// -------------------------------------------------------------------------
	// FASE 1: Consumer 1 processa Evento 1, envia ACK e confirma processamento no broker
	// -------------------------------------------------------------------------
	t.Log("FASE 1: Consumer 1 processando e enviando ACK...")
	conn1, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatalf("failed to dial conn1: %v", err)
	}

	ch1, err := conn1.Channel()
	if err != nil {
		_ = conn1.Close()
		t.Fatalf("failed to open ch1: %v", err)
	}

	msgs1, err := ch1.Consume(testQueueName, "consumer-1", false, false, false, false, nil)
	if err != nil {
		_ = ch1.Close()
		_ = conn1.Close()
		t.Fatalf("failed to start consume on ch1: %v", err)
	}

	coldValidator1 := stream.NewSequenceValidator()
	queries1 := db.New(dbConn)
	eventStore1 := postgres.NewEventStore(queries1)
	proj1 := delivery.NewProjection(dbConn, "DeliveryProjection")
	recovery1 := stream.NewGapRecovery(eventStore1, coldValidator1, proj1, liveHub)
	streamConsumer1 := stream.NewConsumer(coldValidator1, recovery1, proj1, liveHub)
	rabbitConsumer1 := rabbitmq.NewConsumer(ch1, streamConsumer1, rabbitmq.WithConsumerLogger(logger.NewZapLogger("test-c1", false)))

	select {
	case d1 := <-msgs1:
		if d1.Redelivered {
			t.Fatalf("esperado d1.Redelivered == false na entrega inicial")
		}

		if err := rabbitConsumer1.HandleDelivery(context.Background(), d1); err != nil {
			t.Fatalf("HandleDelivery falhou no Consumer 1: %v", err)
		}
		t.Log("Consumer 1 processou e enviou ACK com sucesso.")

	case <-time.After(5 * time.Second):
		t.Fatal("timeout esperando entrega inicial no Consumer 1")
	}

	// Confirma que a notificação 1 foi emitida no LiveHub
	select {
	case notif1 := <-hubSub:
		if notif1.StreamPosition != streamPos1 || notif1.DomainEvent.Sequence() != 1 {
			t.Fatalf("notificação 1 incorreta: %+v", notif1)
		}
		t.Log("Notificação legítima do Evento 1 recebida no LiveHub.")
	case <-time.After(2 * time.Second):
		t.Fatal("timeout esperando notificação 1 no LiveHub")
	}

	// CONFIRMAÇÃO METODOLÓGICA NO BROKER:
	// Observa o estado da fila no broker até confirmar que a mensagem foi removida pós-ACK
	ackProcessed := false
	for i := 0; i < 50; i++ {
		qStatus, err := adminCh.QueueInspect(testQueueName)
		if err == nil && qStatus.Messages == 0 {
			ackProcessed = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ackProcessed {
		t.Fatal("broker não confirmou remoção da mensagem após ACK")
	}
	t.Log("Confirmação metodológica obtida: Broker removeu a mensagem após processar o ACK.")

	// -------------------------------------------------------------------------
	// FASE 2: Interrupção na conexão do Consumer 1 DEPOIS do ACK processado
	// -------------------------------------------------------------------------
	t.Log("FASE 2: Simulando interrupção na conexão após ACK ser processado pelo broker...")
	_ = ch1.Close()
	_ = conn1.Close()

	// -------------------------------------------------------------------------
	// FASE 3: Consumer 2 conecta; Critério 4 (1.5A-3b): Mensagem NÃO é reentregue
	// -------------------------------------------------------------------------
	t.Log("FASE 3: Conectando Consumer 2 pós-interrupção...")
	conn2, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatalf("failed to dial conn2: %v", err)
	}
	defer conn2.Close()

	ch2, err := conn2.Channel()
	if err != nil {
		t.Fatalf("failed to open ch2: %v", err)
	}
	defer ch2.Close()

	msgs2, err := ch2.Consume(testQueueName, "consumer-2", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to start consume on ch2: %v", err)
	}

	// Aguarda janela de 500ms para provar ausência de redelivery
	select {
	case dUnexpected := <-msgs2:
		t.Fatalf("CRITÉRIO 4 FALHOU (1.5A-3b): Mensagem confirmada foi indevidamente reentregue pelo broker! %+v", dUnexpected)
	case <-time.After(500 * time.Millisecond):
		t.Log("CRITÉRIO 4 APROVADO (1.5A-3b): Mensagem confirmada foi definitivamente removida, zero redeliveries.")
	}

	// -------------------------------------------------------------------------
	// FASE 4: Processamento de mensagem posterior (Critério 6)
	// -------------------------------------------------------------------------
	t.Log("FASE 4: Publicando evento subsequente (Seq 2: DriverArrivedAtPickup)...")
	streamPos2 := int64(100802)
	ev2 := delivery.EventDriverArrivedAtPickup{
		ID:         "ev-gate15a3b-02",
		DeliveryID: testDeliveryID,
		Seq:        2,
	}
	publishCrashTestEnvelope(t, dbConn, adminCh, testQueueName, testDeliveryID, streamPos2, "DriverArrivedAtPickup", ev2)

	coldValidator2 := stream.NewSequenceValidator()
	queries2 := db.New(dbConn)
	eventStore2 := postgres.NewEventStore(queries2)
	proj2 := delivery.NewProjection(dbConn, "DeliveryProjection")
	recovery2 := stream.NewGapRecovery(eventStore2, coldValidator2, proj2, liveHub)
	streamConsumer2 := stream.NewConsumer(coldValidator2, recovery2, proj2, liveHub)
	rabbitConsumer2 := rabbitmq.NewConsumer(ch2, streamConsumer2, rabbitmq.WithConsumerLogger(logger.NewZapLogger("test-c2", false)))

	select {
	case d2 := <-msgs2:
		if d2.Redelivered {
			t.Fatalf("esperado d2.Redelivered == false para evento posterior novo")
		}
		if err := rabbitConsumer2.HandleDelivery(context.Background(), d2); err != nil {
			t.Fatalf("Consumer 2 falhou ao processar evento posterior (Seq 2): %v", err)
		}
		t.Log("Consumer 2 processou evento subsequente (Seq 2) com sucesso.")

	case <-time.After(5 * time.Second):
		t.Fatal("timeout esperando evento subsequente no Consumer 2")
	}

	// CRITÉRIO 6 & 3: Notificação legítima do Evento 2 recebida no LiveHub
	select {
	case notif2 := <-hubSub:
		if notif2.StreamPosition != streamPos2 || notif2.DomainEvent.Sequence() != 2 {
			t.Fatalf("notificação 2 incorreta: pos=%d, seq=%d", notif2.StreamPosition, notif2.DomainEvent.Sequence())
		}
		t.Logf("CRITÉRIO 6 APROVADO: Evento subsequente (Seq 2) produziu notificação legítima: stream_pos=%d, seq=%d",
			notif2.StreamPosition, notif2.DomainEvent.Sequence())
	case <-time.After(2 * time.Second):
		t.Fatal("timeout esperando notificação 2 no LiveHub")
	}

	// Nenhuma outra notificação no LiveHub
	select {
	case extra := <-hubSub:
		t.Fatalf("notificação inesperada no LiveHub: %+v", extra)
	case <-time.After(500 * time.Millisecond):
		t.Log("Total de notificações no LiveHub verificado: EXATAMENTE 2 (1 por evento legítimo).")
	}

	// -------------------------------------------------------------------------
	// FASE 5: Asserções Finais no PostgreSQL e Broker
	// -------------------------------------------------------------------------
	// CRITÉRIO 1: Read Model atualizado para estado do evento 2 sem duplicatas físicas
	var countFinal int
	var seqFinal int64
	var stateFinal string
	err = dbConn.QueryRow("SELECT count(*), max(sequence), max(state) FROM worker_test.delivery_read_model WHERE delivery_id = $1", testDeliveryID).
		Scan(&countFinal, &seqFinal, &stateFinal)
	if err != nil || countFinal != 1 || seqFinal != 2 || stateFinal != "AT_PICKUP" {
		t.Fatalf("CRITÉRIO 1 FALHOU: read model corrompido pós-1.5A-3b: count=%d, seq=%d, state=%s", countFinal, seqFinal, stateFinal)
	}
	t.Logf("CRITÉRIO 1 APROVADO: Read model preservado com estado atualizado (count=%d, seq=%d, state=%s).", countFinal, seqFinal, stateFinal)

	// CRITÉRIO 2: Checkpoint avançado para a posição da mensagem 2
	var cpFinal int64
	err = dbConn.QueryRow("SELECT last_sequence FROM worker_test.projection_checkpoints WHERE projection_name = 'DeliveryProjection'").Scan(&cpFinal)
	if err != nil || cpFinal != streamPos2 {
		t.Fatalf("CRITÉRIO 2 FALHOU: checkpoint inconsistente: cp=%d (esperado %d)", cpFinal, streamPos2)
	}
	t.Logf("CRITÉRIO 2 APROVADO: Checkpoint avançado consistentemente para %d.", cpFinal)

	// Fila zerada no broker
	qStatus, err := adminCh.QueueInspect(testQueueName)
	if err != nil || qStatus.Messages != 0 {
		t.Fatalf("fila não foi zerada: messages=%d (err: %v)", qStatus.Messages, err)
	}
	t.Log("Fila RabbitMQ drenada e zerada com sucesso.")

	t.Log("=== TESTE 1.5A-3b (INTERRUPÇÃO PÓS-ACK PROCESSADO COM REMOÇÃO DEFINITIVA) APROVADO ===")
}

// TestGate15A4_ColdRestartWithPersistedCheckpoint implementa o cenário 1.5A-4:
// 1. Publicar e processar eventos de um agregado até sequence=3 (DeliveryCreated, DriverArrivedAtPickup, DriverEnRouteToDropoff).
// 2. Confirmar que o read model (sequence=3, state=EN_ROUTE_TO_DROPOFF) e o checkpoint global (stream_position=100903) foram persistidos.
// 3. Encerrar completamente o Consumidor 1 (fechando canal e conexão TCP).
// 4. Criar o Consumidor 2 com SequenceValidator completamente vazio (sem memória do agregado).
// 5. Validar que o checkpoint persistido está intacto no PostgreSQL (proj2.GetCheckpoint == 100903).
// 6. Publicar uma duplicata antiga (sequence=2, stream_position=100902):
//    - O Consumidor 2 reconhece a duplicata através da reconciliação com o estado persistido;
//    - Não há alteração no read model (não regride nem duplica);
//    - Não há alteração ou regressão no checkpoint;
//    - O LiveHub NÃO emite notificação para o evento duplicado;
//    - A mensagem é confirmada com basic.ack.
// 7. Publicar o próximo evento legítimo (sequence=4: DeliveryCompleted, stream_position=100904):
//    - O Consumidor 2 aplica com sucesso o evento no read model (sequence=4, state=DELIVERED);
//    - O checkpoint global avança para 100904;
//    - O LiveHub emite EXATAMENTE uma notificação para o novo evento legítimo;
//    - A mensagem é confirmada com basic.ack.
// 8. Asserções finais de integridade:
//    - Read model: exatamente 1 registro, sequence=4, state=DELIVERED.
//    - Checkpoint: exatamente 100904.
//    - LiveHub: exatamente 4 notificações ao todo (ev1, ev2, ev3, ev4 - zero para a duplicata).
//    - Fila RabbitMQ: completamente drenada e zerada.
func TestGate15A4_ColdRestartWithPersistedCheckpoint(t *testing.T) {
	amqpURL := getCrashTestAMQPURL()
	dbConn := setupCrashTestDB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	adminConn, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Skipf("cannot connect to rabbitmq: %v", err)
		return
	}
	defer adminConn.Close()

	adminCh, err := adminConn.Channel()
	if err != nil {
		t.Fatalf("failed to open admin channel: %v", err)
	}
	defer adminCh.Close()

	testDeliveryID := uuid.New()
	testQueueName := fmt.Sprintf("test.gate15a4.%s", testDeliveryID.String())

	_, err = adminCh.QueueDeclare(testQueueName, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to declare queue: %v", err)
	}
	defer adminCh.QueueDelete(testQueueName, false, false, false)

	liveHub := stream.NewMemoryLiveHub()
	hubSub, hubUnsub := liveHub.Subscribe(context.Background(), testDeliveryID)
	defer hubUnsub()

	// -------------------------------------------------------------------------
	// FASE 1: Publicar e processar eventos até sequence=3 no Consumer 1
	// -------------------------------------------------------------------------
	t.Log("FASE 1: Publicando e processando eventos 1, 2 e 3 com Consumer 1...")

	conn1, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatalf("failed to dial conn1: %v", err)
	}
	ch1, err := conn1.Channel()
	if err != nil {
		_ = conn1.Close()
		t.Fatalf("failed to open ch1: %v", err)
	}

	msgs1, err := ch1.Consume(testQueueName, "consumer-1", false, false, false, false, nil)
	if err != nil {
		_ = ch1.Close()
		_ = conn1.Close()
		t.Fatalf("failed to start consume on ch1: %v", err)
	}

	validator1 := stream.NewSequenceValidator()
	queries1 := db.New(dbConn)
	eventStore1 := postgres.NewEventStore(queries1)
	proj1 := delivery.NewProjection(dbConn, "DeliveryProjection")
	recovery1 := stream.NewGapRecovery(eventStore1, validator1, proj1, liveHub)
	streamConsumer1 := stream.NewConsumer(validator1, recovery1, proj1, liveHub)
	rabbitConsumer1 := rabbitmq.NewConsumer(ch1, streamConsumer1, rabbitmq.WithConsumerLogger(logger.NewZapLogger("test-c1-phase1", false)))

	// Evento 1 (Seq 1): DeliveryCreated
	streamPos1 := int64(100901)
	ev1 := delivery.EventDeliveryCreated{
		ID:         "ev-gate15a4-01",
		DeliveryID: testDeliveryID,
		DriverID:   "driver-gate15a4",
		Seq:        1,
		PickupLat:  -23.5505,
		PickupLng:  -46.6333,
		DropoffLat: -23.5590,
		DropoffLng: -46.6400,
	}
	publishCrashTestEnvelope(t, dbConn, adminCh, testQueueName, testDeliveryID, streamPos1, "DeliveryCreated", ev1)

	select {
	case d1 := <-msgs1:
		if err := rabbitConsumer1.HandleDelivery(context.Background(), d1); err != nil {
			t.Fatalf("Consumer 1 falhou no evento 1: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout esperando d1")
	}

	// Notificação 1 no LiveHub
	select {
	case n1 := <-hubSub:
		if n1.StreamPosition != streamPos1 || n1.DomainEvent.Sequence() != 1 {
			t.Fatalf("notificação 1 incorreta: %+v", n1)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout esperando notificação 1")
	}

	// Evento 2 (Seq 2): DriverArrivedAtPickup
	streamPos2 := int64(100902)
	ev2 := delivery.EventDriverArrivedAtPickup{
		ID:         "ev-gate15a4-02",
		DeliveryID: testDeliveryID,
		Seq:        2,
	}
	publishCrashTestEnvelope(t, dbConn, adminCh, testQueueName, testDeliveryID, streamPos2, "DriverArrivedAtPickup", ev2)

	select {
	case d2 := <-msgs1:
		if err := rabbitConsumer1.HandleDelivery(context.Background(), d2); err != nil {
			t.Fatalf("Consumer 1 falhou no evento 2: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout esperando d2")
	}

	// Notificação 2 no LiveHub
	select {
	case n2 := <-hubSub:
		if n2.StreamPosition != streamPos2 || n2.DomainEvent.Sequence() != 2 {
			t.Fatalf("notificação 2 incorreta: %+v", n2)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout esperando notificação 2")
	}

	// Evento 3 (Seq 3): DriverEnRouteToDropoff
	streamPos3 := int64(100903)
	ev3 := delivery.EventDriverEnRouteToDropoff{
		ID:         "ev-gate15a4-03",
		DeliveryID: testDeliveryID,
		Seq:        3,
	}
	publishCrashTestEnvelope(t, dbConn, adminCh, testQueueName, testDeliveryID, streamPos3, "DriverEnRouteToDropoff", ev3)

	select {
	case d3 := <-msgs1:
		if err := rabbitConsumer1.HandleDelivery(context.Background(), d3); err != nil {
			t.Fatalf("Consumer 1 falhou no evento 3: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout esperando d3")
	}

	// Notificação 3 no LiveHub
	select {
	case n3 := <-hubSub:
		if n3.StreamPosition != streamPos3 || n3.DomainEvent.Sequence() != 3 {
			t.Fatalf("notificação 3 incorreta: %+v", n3)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout esperando notificação 3")
	}

	// -------------------------------------------------------------------------
	// FASE 2: Validação do Estado Inicial Persistido e Encerramento do Consumer 1
	// -------------------------------------------------------------------------
	t.Log("FASE 2: Validando persistência inicial e encerrando Consumer 1...")

	// CRITÉRIO DE ESTADO INICIAL: Read model em sequence 3 e state EN_ROUTE_TO_DROPOFF
	var initCount int
	var initSeq int64
	var initState string
	err = dbConn.QueryRow("SELECT count(*), max(sequence), max(state) FROM worker_test.delivery_read_model WHERE delivery_id = $1", testDeliveryID).
		Scan(&initCount, &initSeq, &initState)
	if err != nil || initCount != 1 || initSeq != 3 || initState != "EN_ROUTE_TO_DROPOFF" {
		t.Fatalf("Estado inicial inválido no DB: count=%d, seq=%d, state=%s (err: %v)", initCount, initSeq, initState, err)
	}
	t.Logf("Estado inicial confirmado no read model: seq=%d, state=%s", initSeq, initState)

	// Checkpoint global no PostgreSQL em streamPos3 (100903)
	ckptInit, err := proj1.GetCheckpoint(context.Background())
	if err != nil || ckptInit != streamPos3 {
		t.Fatalf("Checkpoint inicial inconsistente: ckpt=%d (esperado %d, err: %v)", ckptInit, streamPos3, err)
	}
	t.Logf("Checkpoint inicial confirmado no PostgreSQL: %d", ckptInit)

	// Encerrar completamente o Consumer 1
	_ = ch1.Close()
	_ = conn1.Close()
	t.Log("Consumer 1 encerrado completamente.")

	// -------------------------------------------------------------------------
	// FASE 3: Inicialização a frio do Consumer 2 com SequenceValidator vazio
	// -------------------------------------------------------------------------
	t.Log("FASE 3: Inicializando Consumer 2 a frio (SequenceValidator vazio)...")

	conn2, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatalf("failed to dial conn2: %v", err)
	}
	defer conn2.Close()

	ch2, err := conn2.Channel()
	if err != nil {
		t.Fatalf("failed to open ch2: %v", err)
	}
	defer ch2.Close()

	msgs2, err := ch2.Consume(testQueueName, "consumer-2", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to start consume on ch2: %v", err)
	}

	coldValidator := stream.NewSequenceValidator()
	// Confirma que o validador em memória está estritamente vazio para o agregado
	if _, ok := coldValidator.Last(testDeliveryID); ok {
		t.Fatal("CRITÉRIO DE RESTART FALHOU: coldValidator não está vazio!")
	}
	t.Log("CRITÉRIO DE RESTART APROVADO: Consumer 2 iniciando com SequenceValidator em memória 100% vazio.")

	queries2 := db.New(dbConn)
	eventStore2 := postgres.NewEventStore(queries2)
	proj2 := delivery.NewProjection(dbConn, "DeliveryProjection")

	// CRITÉRIO DE CHECKPOINT: Carregado corretamente do PostgreSQL
	ckptLoaded, err := proj2.GetCheckpoint(context.Background())
	if err != nil || ckptLoaded != streamPos3 {
		t.Fatalf("CRITÉRIO DE CHECKPOINT FALHOU: checkpoint não recuperado do PostgreSQL: ckpt=%d, esperado=%d (err: %v)",
			ckptLoaded, streamPos3, err)
	}
	t.Logf("CRITÉRIO DE CHECKPOINT APROVADO: Checkpoint persistido recuperado com sucesso do PostgreSQL: %d", ckptLoaded)

	recovery2 := stream.NewGapRecovery(eventStore2, coldValidator, proj2, liveHub)
	streamConsumer2 := stream.NewConsumer(coldValidator, recovery2, proj2, liveHub)
	rabbitConsumer2 := rabbitmq.NewConsumer(ch2, streamConsumer2, rabbitmq.WithConsumerLogger(logger.NewZapLogger("test-c2-cold", false)))

	// -------------------------------------------------------------------------
	// FASE 4: Publicar duplicata antiga (sequence=2) e verificar deduplicação
	// -------------------------------------------------------------------------
	t.Log("FASE 4: Publicando duplicata antiga (sequence=2)...")
	// Não reinserimos na outbox para não alterar o histórico; apenas publicamos novamente na fila AMQP
	envBytesDup, err := json.Marshal(map[string]interface{}{
		"aggregate_id":    testDeliveryID,
		"stream_position": streamPos2,
		"event_type":      "DriverArrivedAtPickup",
		"payload":         json.RawMessage(`{"id":"ev-gate15a4-02","delivery_id":"` + testDeliveryID.String() + `","seq":2}`),
	})
	if err != nil {
		t.Fatalf("failed to marshal duplicate envelope: %v", err)
	}
	err = adminCh.PublishWithContext(context.Background(), "", testQueueName, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         envBytesDup,
	})
	if err != nil {
		t.Fatalf("failed to publish duplicate event: %v", err)
	}

	select {
	case dDup := <-msgs2:
		t.Log("Consumer 2 recebeu a duplicata antiga (Seq 2). Processando com reconciliação a frio...")
		if err := rabbitConsumer2.HandleDelivery(context.Background(), dDup); err != nil {
			t.Fatalf("Consumer 2 falhou ao tratar duplicata antiga: %v", err)
		}
		t.Log("Consumer 2 processou duplicata antiga com sucesso e enviou basic.ack.")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout esperando duplicata no Consumer 2")
	}

	// CRITÉRIO DE EVENTO ANTIGO / BANCO: Sem regressão de estado ou checkpoint
	var midSeq int64
	var midState string
	err = dbConn.QueryRow("SELECT sequence, state FROM worker_test.delivery_read_model WHERE delivery_id = $1", testDeliveryID).
		Scan(&midSeq, &midState)
	if err != nil || midSeq != 3 || midState != "EN_ROUTE_TO_DROPOFF" {
		t.Fatalf("CRITÉRIO DE EVENTO ANTIGO FALHOU: read model sofreu regressão após duplicata! seq=%d, state=%s", midSeq, midState)
	}
	t.Log("CRITÉRIO DE EVENTO ANTIGO APROVADO: Read model preservado sem regressão (seq=3, state=EN_ROUTE_TO_DROPOFF).")

	ckptMid, err := proj2.GetCheckpoint(context.Background())
	if err != nil || ckptMid != streamPos3 {
		t.Fatalf("CRITÉRIO DE CHECKPOINT FALHOU: checkpoint sofreu regressão após duplicata: %d (esperado %d)", ckptMid, streamPos3)
	}
	t.Log("CRITÉRIO DE CHECKPOINT APROVADO: Checkpoint preservado sem regressão (100903).")

	// CRITÉRIO DE LIVEHUB: Nenhuma notificação emitida para a duplicata
	select {
	case notifDup := <-hubSub:
		t.Fatalf("CRITÉRIO DE LIVEHUB FALHOU: notificação emitida indevidamente para duplicata: %+v", notifDup)
	case <-time.After(500 * time.Millisecond):
		t.Log("CRITÉRIO DE LIVEHUB APROVADO: Zero notificações emitidas para a duplicata antiga.")
	}

	// -------------------------------------------------------------------------
	// FASE 5: Publicar o próximo evento legítimo (sequence=4)
	// -------------------------------------------------------------------------
	t.Log("FASE 5: Publicando próximo evento legítimo (sequence=4: DeliveryCompleted)...")
	streamPos4 := int64(100904)
	ev4 := delivery.EventDeliveryCompleted{
		ID:         "ev-gate15a4-04",
		DeliveryID: testDeliveryID,
		Seq:        4,
	}
	publishCrashTestEnvelope(t, dbConn, adminCh, testQueueName, testDeliveryID, streamPos4, "DeliveryCompleted", ev4)

	select {
	case d4 := <-msgs2:
		t.Log("Consumer 2 recebeu evento subsequente (Seq 4). Processando...")
		if err := rabbitConsumer2.HandleDelivery(context.Background(), d4); err != nil {
			t.Fatalf("Consumer 2 falhou ao processar evento subsequente (Seq 4): %v", err)
		}
		t.Log("Consumer 2 processou evento subsequente com sucesso e enviou basic.ack.")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout esperando d4 no Consumer 2")
	}

	// CRITÉRIO DE EVENTO SEGUINTE / LIVEHUB: Notificação legítima para sequence 4
	select {
	case n4 := <-hubSub:
		if n4.StreamPosition != streamPos4 || n4.DomainEvent.Sequence() != 4 {
			t.Fatalf("CRITÉRIO DE EVENTO SEGUINTE FALHOU: notificação incorreta: pos=%d, seq=%d", n4.StreamPosition, n4.DomainEvent.Sequence())
		}
		t.Logf("CRITÉRIO DE EVENTO SEGUINTE APROVADO: Notificação legítima para Sequence 4 recebida no LiveHub: stream_pos=%d, seq=%d",
			n4.StreamPosition, n4.DomainEvent.Sequence())
	case <-time.After(2 * time.Second):
		t.Fatal("timeout esperando notificação 4 no LiveHub")
	}

	// Nenhuma outra notificação residual
	select {
	case extra := <-hubSub:
		t.Fatalf("notificação espúria residual no LiveHub: %+v", extra)
	case <-time.After(500 * time.Millisecond):
		t.Log("Total de notificações no LiveHub verificado: EXATAMENTE 4 (ev1, ev2, ev3, ev4 - zero para a duplicata).")
	}

	// -------------------------------------------------------------------------
	// FASE 6: Asserções Finais de Consistência
	// -------------------------------------------------------------------------
	t.Log("FASE 6: Asserções finais de integridade no PostgreSQL e Broker...")

	// CRITÉRIO DE BANCO: Read model atualizado para sequence 4 com unicidade estrita
	var finalCount int
	var finalSeq int64
	var finalState string
	err = dbConn.QueryRow("SELECT count(*), max(sequence), max(state) FROM worker_test.delivery_read_model WHERE delivery_id = $1", testDeliveryID).
		Scan(&finalCount, &finalSeq, &finalState)
	if err != nil || finalCount != 1 || finalSeq != 4 || finalState != "DELIVERED" {
		t.Fatalf("CRITÉRIO DE BANCO FALHOU: estado final inconsistente: count=%d, seq=%d, state=%s (err: %v)", finalCount, finalSeq, finalState, err)
	}
	t.Logf("CRITÉRIO DE BANCO APROVADO: Read model em sequence 4, state=DELIVERED (count=1).")

	// CRITÉRIO DE CHECKPOINT: Checkpoint global avançado para streamPos4 (100904)
	ckptFinal, err := proj2.GetCheckpoint(context.Background())
	if err != nil || ckptFinal != streamPos4 {
		t.Fatalf("CRITÉRIO DE CHECKPOINT FALHOU: checkpoint final incorreto: %d (esperado %d)", ckptFinal, streamPos4)
	}
	t.Logf("CRITÉRIO DE CHECKPOINT APROVADO: Checkpoint avançado para %d sem saltos ou inconsistências.", ckptFinal)

	// CRITÉRIO DE RABBITMQ: Fila completamente drenada e zerada
	qStatus, err := adminCh.QueueInspect(testQueueName)
	if err != nil || qStatus.Messages != 0 {
		t.Fatalf("CRITÉRIO DE RABBITMQ FALHOU: fila ainda possui mensagens: %d (err: %v)", qStatus.Messages, err)
	}
	t.Log("CRITÉRIO DE RABBITMQ APROVADO: Fila drenada e zerada após todos os ACKs (messages=0).")

	t.Log("=== TESTE 1.5A-4 (RESTART A FRIO COM CHECKPOINT PERSISTIDO) CONCLUÍDO COM SUCESSO ===")
}

// TestGate15A5_HeterogeneousBatchCrashAndReconciliation implementa o cenário 1.5A-5:
// Valida a resiliência do consumidor sob um lote heterogêneo intercalando dois agregados independentes (A e B),
// sofrendo uma interrupção determinística no meio do lote (após 4 mensagens confirmadas),
// e retomando com um consumidor novo a frio até a reconciliação completa.
//
// Ordem de Publicação dos 10 Eventos:
//  1: Agregado A, Seq 1 (DeliveryCreated, stream_pos=101001)
//  2: Agregado B, Seq 1 (DeliveryCreated, stream_pos=101002)
//  3: Agregado A, Seq 2 (DriverArrivedAtPickup, stream_pos=101003)
//  4: Agregado B, Seq 2 (DriverArrivedAtPickup, stream_pos=101004)
//  --- PONTO DETERMINÍSTICO DE INTERRUPÇÃO (Consumer 1 fecha conexão) ---
//  5: Agregado A, Seq 3 (DeliveryRouteAssigned, stream_pos=101005)
//  6: Agregado B, Seq 3 (DeliveryRouteAssigned, stream_pos=101006)
//  7: Agregado A, Seq 4 (DriverEnRouteToDropoff, stream_pos=101007)
//  8: Agregado B, Seq 4 (DriverEnRouteToDropoff, stream_pos=101008)
//  9: Agregado A, Seq 5 (DeliveryCompleted, stream_pos=101009)
// 10: Agregado B, Seq 5 (DeliveryCompleted, stream_pos=101010)
//
// Cuidados metodológicos atendidos:
// A. Não confundir ACK com aplicação: Cada mensagem tem sua transação PostgreSQL confirmada antes do basic.ack.
// B. Não confundir posição global com sequência causal: Read models de A e B e o checkpoint global são verificados separadamente.
// C. Não presumir ordem global de notificação: Ordem causal interna [1, 2, 3, 4, 5] validada estritamente por agregado em subA e subB.
// D. Reconciliação total: Fila vazia, read models em sequence 5, checkpoint em 101010 e exatamente 10 notificações no LiveHub.
func TestGate15A5_HeterogeneousBatchCrashAndReconciliation(t *testing.T) {
	amqpURL := getCrashTestAMQPURL()
	dbConn := setupCrashTestDB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	adminConn, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Skipf("cannot connect to rabbitmq: %v", err)
		return
	}
	defer adminConn.Close()

	adminCh, err := adminConn.Channel()
	if err != nil {
		t.Fatalf("failed to open admin channel: %v", err)
	}
	defer adminCh.Close()

	aggA := uuid.New()
	aggB := uuid.New()
	testQueueName := fmt.Sprintf("test.gate15a5.%s", uuid.New().String())

	_, err = adminCh.QueueDeclare(testQueueName, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to declare queue: %v", err)
	}
	defer adminCh.QueueDelete(testQueueName, false, false, false)

	// LiveHub e assinaturas independentes por agregado
	liveHub := stream.NewMemoryLiveHub()
	subA, unsubA := liveHub.Subscribe(context.Background(), aggA)
	defer unsubA()
	subB, unsubB := liveHub.Subscribe(context.Background(), aggB)
	defer unsubB()

	// -------------------------------------------------------------------------
	// FASE 1: Publicar os 10 eventos intercalados de A e B
	// -------------------------------------------------------------------------
	t.Log("FASE 1: Publicando lote heterogêneo de 10 eventos (5 para Agregado A, 5 para Agregado B)...")

	type eventDef struct {
		aggID     uuid.UUID
		streamPos int64
		eventType string
		payload   interface{}
	}

	eventsBatch := []eventDef{
		{
			aggID:     aggA,
			streamPos: 101001,
			eventType: "DeliveryCreated",
			payload: delivery.EventDeliveryCreated{
				ID:         "ev-gate15a5-a01",
				DeliveryID: aggA,
				DriverID:   "driver-a",
				Seq:        1,
				PickupLat:  -23.5505,
				PickupLng:  -46.6333,
				DropoffLat: -23.5590,
				DropoffLng: -46.6400,
			},
		},
		{
			aggID:     aggB,
			streamPos: 101002,
			eventType: "DeliveryCreated",
			payload: delivery.EventDeliveryCreated{
				ID:         "ev-gate15a5-b01",
				DeliveryID: aggB,
				DriverID:   "driver-b",
				Seq:        1,
				PickupLat:  -23.5700,
				PickupLng:  -46.6500,
				DropoffLat: -23.5800,
				DropoffLng: -46.6600,
			},
		},
		{
			aggID:     aggA,
			streamPos: 101003,
			eventType: "DriverArrivedAtPickup",
			payload: delivery.EventDriverArrivedAtPickup{
				ID:         "ev-gate15a5-a02",
				DeliveryID: aggA,
				Seq:        2,
			},
		},
		{
			aggID:     aggB,
			streamPos: 101004,
			eventType: "DriverArrivedAtPickup",
			payload: delivery.EventDriverArrivedAtPickup{
				ID:         "ev-gate15a5-b02",
				DeliveryID: aggB,
				Seq:        2,
			},
		},
		{
			aggID:     aggA,
			streamPos: 101005,
			eventType: "DeliveryRouteAssigned",
			payload: delivery.EventDeliveryRouteAssigned{
				ID:         "ev-gate15a5-a03",
				DeliveryID: aggA,
				Seq:        3,
				Route: delivery.RouteGeometryPayload{
					RouteID:             "route-a-rev1",
					Revision:            1,
					Leg:                 "pickup",
					DistanceMeters:      1200,
					EstimatedDurationMs: 90000,
				},
			},
		},
		{
			aggID:     aggB,
			streamPos: 101006,
			eventType: "DeliveryRouteAssigned",
			payload: delivery.EventDeliveryRouteAssigned{
				ID:         "ev-gate15a5-b03",
				DeliveryID: aggB,
				Seq:        3,
				Route: delivery.RouteGeometryPayload{
					RouteID:             "route-b-rev1",
					Revision:            1,
					Leg:                 "pickup",
					DistanceMeters:      2400,
					EstimatedDurationMs: 180000,
				},
			},
		},
		{
			aggID:     aggA,
			streamPos: 101007,
			eventType: "DriverEnRouteToDropoff",
			payload: delivery.EventDriverEnRouteToDropoff{
				ID:         "ev-gate15a5-a04",
				DeliveryID: aggA,
				Seq:        4,
				Lat:        -23.5550,
				Lng:        -46.6350,
			},
		},
		{
			aggID:     aggB,
			streamPos: 101008,
			eventType: "DriverEnRouteToDropoff",
			payload: delivery.EventDriverEnRouteToDropoff{
				ID:         "ev-gate15a5-b04",
				DeliveryID: aggB,
				Seq:        4,
				Lat:        -23.5750,
				Lng:        -46.6550,
			},
		},
		{
			aggID:     aggA,
			streamPos: 101009,
			eventType: "DeliveryCompleted",
			payload: delivery.EventDeliveryCompleted{
				ID:         "ev-gate15a5-a05",
				DeliveryID: aggA,
				Seq:        5,
			},
		},
		{
			aggID:     aggB,
			streamPos: 101010,
			eventType: "DeliveryCompleted",
			payload: delivery.EventDeliveryCompleted{
				ID:         "ev-gate15a5-b05",
				DeliveryID: aggB,
				Seq:        5,
			},
		},
	}

	for _, e := range eventsBatch {
		publishCrashTestEnvelope(t, dbConn, adminCh, testQueueName, e.aggID, e.streamPos, e.eventType, e.payload)
	}

	// -------------------------------------------------------------------------
	// FASE 2: Iniciar Consumer 1 e processar exatamente 4 mensagens
	// -------------------------------------------------------------------------
	t.Log("FASE 2: Consumer 1 iniciando e consumindo exatamente 4 mensagens com QoS=1...")

	conn1, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatalf("failed to dial conn1: %v", err)
	}
	ch1, err := conn1.Channel()
	if err != nil {
		_ = conn1.Close()
		t.Fatalf("failed to open ch1: %v", err)
	}

	if err := ch1.Qos(1, 0, false); err != nil {
		_ = ch1.Close()
		_ = conn1.Close()
		t.Fatalf("failed to set QoS=1 on ch1: %v", err)
	}

	msgs1, err := ch1.Consume(testQueueName, "consumer-1", false, false, false, false, nil)
	if err != nil {
		_ = ch1.Close()
		_ = conn1.Close()
		t.Fatalf("failed to start consume on ch1: %v", err)
	}

	validator1 := stream.NewSequenceValidator()
	queries1 := db.New(dbConn)
	eventStore1 := postgres.NewEventStore(queries1)
	proj1 := delivery.NewProjection(dbConn, "DeliveryProjection")
	recovery1 := stream.NewGapRecovery(eventStore1, validator1, proj1, liveHub)
	streamConsumer1 := stream.NewConsumer(validator1, recovery1, proj1, liveHub)
	rabbitConsumer1 := rabbitmq.NewConsumer(ch1, streamConsumer1, rabbitmq.WithConsumerLogger(logger.NewZapLogger("test-c1-phase2", false)))

	var notifsA []stream.Event
	var notifsB []stream.Event

	for i := 1; i <= 4; i++ {
		select {
		case d := <-msgs1:
			if err := rabbitConsumer1.HandleDelivery(context.Background(), d); err != nil {
				t.Fatalf("Consumer 1 falhou na mensagem %d: %v", i, err)
			}
			t.Logf("Consumer 1 processou mensagem %d com sucesso.", i)
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout esperando mensagem %d no Consumer 1", i)
		}
	}

	// Coleta as 4 notificações legítimas da Fase 2 (2 de A e 2 de B)
	for i := 0; i < 2; i++ {
		select {
		case nA := <-subA:
			notifsA = append(notifsA, nA)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout esperando notificação para A na Fase 2")
		}
		select {
		case nB := <-subB:
			notifsB = append(notifsB, nB)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout esperando notificação para B na Fase 2")
		}
	}

	if len(notifsA) != 2 || len(notifsB) != 2 {
		t.Fatalf("notificações intermediárias incorretas: A=%d, B=%d", len(notifsA), len(notifsB))
	}
	t.Log("CRITÉRIO DE PROCESSAMENTO INICIAL APROVADO: 4 mensagens confirmadas e comitadas.")

	// -------------------------------------------------------------------------
	// FASE 3: Interrupção determinística e verificação de estado intermediário
	// -------------------------------------------------------------------------
	t.Log("FASE 3: Interrupção determinística do Consumer 1 (fechando TCP imediatamente)...")
	_ = ch1.Close()
	_ = conn1.Close()
	t.Log("Consumer 1 encerrado.")

	// CRITÉRIO DE INTERRUPÇÃO: Mensagens não confirmadas permanecem recuperáveis na fila
	qStatusMid, err := adminCh.QueueInspect(testQueueName)
	if err != nil || qStatusMid.Messages != 6 {
		t.Fatalf("CRITÉRIO DE INTERRUPÇÃO FALHOU: esperado 6 mensagens restantes no broker, obtido %d (err: %v)",
			qStatusMid.Messages, err)
	}
	t.Logf("CRITÉRIO DE INTERRUPÇÃO APROVADO: Fila mantém exatamente 6 mensagens não confirmadas recuperáveis (%d).", qStatusMid.Messages)

	// Validação do estado no PostgreSQL no ponto de interrupção
	var seqMidA, seqMidB int64
	var stateMidA, stateMidB string
	err = dbConn.QueryRow("SELECT sequence, state FROM worker_test.delivery_read_model WHERE delivery_id = $1", aggA).
		Scan(&seqMidA, &stateMidA)
	if err != nil || seqMidA != 2 || stateMidA != "AT_PICKUP" {
		t.Fatalf("Estado intermediário incorreto para A: seq=%d, state=%s (err: %v)", seqMidA, stateMidA, err)
	}

	err = dbConn.QueryRow("SELECT sequence, state FROM worker_test.delivery_read_model WHERE delivery_id = $1", aggB).
		Scan(&seqMidB, &stateMidB)
	if err != nil || seqMidB != 2 || stateMidB != "AT_PICKUP" {
		t.Fatalf("Estado intermediário incorreto para B: seq=%d, state=%s (err: %v)", seqMidB, stateMidB, err)
	}
	t.Logf("Estado intermediário confirmado no PostgreSQL: Agregado A=(seq=%d, state=%s), Agregado B=(seq=%d, state=%s)",
		seqMidA, stateMidA, seqMidB, stateMidB)

	ckptMid, err := proj1.GetCheckpoint(context.Background())
	if err != nil || ckptMid != 101004 {
		t.Fatalf("Checkpoint intermediário inconsistente: %d (esperado 101004)", ckptMid)
	}
	t.Logf("Checkpoint global intermediário confirmado no PostgreSQL: %d", ckptMid)

	// -------------------------------------------------------------------------
	// FASE 4: Inicializar Consumer 2 a frio e processar as mensagens restantes
	// -------------------------------------------------------------------------
	t.Log("FASE 4: Inicializando Consumer 2 com estado em memória vazio para processar as 6 mensagens restantes...")

	conn2, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatalf("failed to dial conn2: %v", err)
	}
	defer conn2.Close()

	ch2, err := conn2.Channel()
	if err != nil {
		t.Fatalf("failed to open ch2: %v", err)
	}
	defer ch2.Close()

	if err := ch2.Qos(1, 0, false); err != nil {
		t.Fatalf("failed to set QoS=1 on ch2: %v", err)
	}

	msgs2, err := ch2.Consume(testQueueName, "consumer-2", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to start consume on ch2: %v", err)
	}

	coldValidator := stream.NewSequenceValidator()
	// CRITÉRIO DE RESTART: Novo consumidor inicia com estado em memória vazio
	if _, ok := coldValidator.Last(aggA); ok {
		t.Fatal("CRITÉRIO DE RESTART FALHOU: coldValidator contém estado em memória para Agregado A")
	}
	if _, ok := coldValidator.Last(aggB); ok {
		t.Fatal("CRITÉRIO DE RESTART FALHOU: coldValidator contém estado em memória para Agregado B")
	}
	t.Log("CRITÉRIO DE RESTART APROVADO: Consumer 2 iniciado com SequenceValidator 100% vazio.")

	queries2 := db.New(dbConn)
	eventStore2 := postgres.NewEventStore(queries2)
	proj2 := delivery.NewProjection(dbConn, "DeliveryProjection")
	recovery2 := stream.NewGapRecovery(eventStore2, coldValidator, proj2, liveHub)
	streamConsumer2 := stream.NewConsumer(coldValidator, recovery2, proj2, liveHub)
	rabbitConsumer2 := rabbitmq.NewConsumer(ch2, streamConsumer2, rabbitmq.WithConsumerLogger(logger.NewZapLogger("test-c2-phase4", false)))

	for i := 5; i <= 10; i++ {
		select {
		case d := <-msgs2:
			if err := rabbitConsumer2.HandleDelivery(context.Background(), d); err != nil {
				t.Fatalf("Consumer 2 falhou ao tratar mensagem %d: %v", i, err)
			}
			t.Logf("Consumer 2 tratou mensagem %d com sucesso.", i)
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout esperando mensagem %d no Consumer 2", i)
		}
	}

	// -------------------------------------------------------------------------
	// FASE 5: Reconciliação Total e Validação dos Critérios de Aprovação
	// -------------------------------------------------------------------------
	t.Log("FASE 5: Executando reconciliação final de estado, checkpoint, fila e notificações...")

	// Drenar notificações restantes do LiveHub para Agregado A (esperado até 5)
	for len(notifsA) < 5 {
		select {
		case nA := <-subA:
			notifsA = append(notifsA, nA)
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout esperando notificações restantes para A (obtidas %d de 5)", len(notifsA))
		}
	}

	// Drenar notificações restantes do LiveHub para Agregado B (esperado até 5)
	for len(notifsB) < 5 {
		select {
		case nB := <-subB:
			notifsB = append(notifsB, nB)
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout esperando notificações restantes para B (obtidas %d de 5)", len(notifsB))
		}
	}

	// CRITÉRIO DE LIVEHUB & DUPLICATAS: Nenhuma notificação duplicada/residual
	select {
	case extraA := <-subA:
		t.Fatalf("CRITÉRIO DE DUPLICATAS FALHOU: Notificação extra recebida para Agregado A: %+v", extraA)
	case extraB := <-subB:
		t.Fatalf("CRITÉRIO DE DUPLICATAS FALHOU: Notificação extra recebida para Agregado B: %+v", extraB)
	case <-time.After(500 * time.Millisecond):
		t.Log("CRITÉRIO DE DUPLICATAS APROVADO: Zero notificações duplicadas após drenagem do lote.")
	}

	// Validação da ordem causal estrita em A [1, 2, 3, 4, 5]
	for idx, n := range notifsA {
		expectedSeq := int64(idx + 1)
		if n.DomainEvent.Sequence() != expectedSeq {
			t.Fatalf("Ordem causal violada para Agregado A na posição %d: esperado seq %d, obtido %d",
				idx, expectedSeq, n.DomainEvent.Sequence())
		}
	}

	// Validação da ordem causal estrita em B [1, 2, 3, 4, 5]
	for idx, n := range notifsB {
		expectedSeq := int64(idx + 1)
		if n.DomainEvent.Sequence() != expectedSeq {
			t.Fatalf("Ordem causal violada para Agregado B na posição %d: esperado seq %d, obtido %d",
				idx, expectedSeq, n.DomainEvent.Sequence())
		}
	}
	t.Logf("CRITÉRIO DE LIVEHUB APROVADO: Exatamente 10 notificações emitidas (5 para A, 5 para B) preservando causalidade estrita.")

	// CRITÉRIO DE AGREGADO A: Read model termina em sequence 5, state DELIVERED
	var countA int
	var finalSeqA int64
	var finalStateA string
	err = dbConn.QueryRow("SELECT count(*), max(sequence), max(state) FROM worker_test.delivery_read_model WHERE delivery_id = $1", aggA).
		Scan(&countA, &finalSeqA, &finalStateA)
	if err != nil || countA != 1 || finalSeqA != 5 || finalStateA != "DELIVERED" {
		t.Fatalf("CRITÉRIO AGREGADO A FALHOU: count=%d, seq=%d, state=%s (err: %v)", countA, finalSeqA, finalStateA, err)
	}
	t.Logf("CRITÉRIO AGREGADO A APROVADO: Read model em sequence 5, state=DELIVERED (count=1).")

	// CRITÉRIO DE AGREGADO B: Read model termina em sequence 5, state DELIVERED
	var countB int
	var finalSeqB int64
	var finalStateB string
	err = dbConn.QueryRow("SELECT count(*), max(sequence), max(state) FROM worker_test.delivery_read_model WHERE delivery_id = $1", aggB).
		Scan(&countB, &finalSeqB, &finalStateB)
	if err != nil || countB != 1 || finalSeqB != 5 || finalStateB != "DELIVERED" {
		t.Fatalf("CRITÉRIO AGREGADO B FALHOU: count=%d, seq=%d, state=%s (err: %v)", countB, finalSeqB, finalStateB, err)
	}
	t.Logf("CRITÉRIO AGREGADO B APROVADO: Read model em sequence 5, state=DELIVERED (count=1).")

	// CRITÉRIO DE CHECKPOINT: Avança monotonicamente até 101010
	ckptFinal, err := proj2.GetCheckpoint(context.Background())
	if err != nil || ckptFinal != 101010 {
		t.Fatalf("CRITÉRIO DE CHECKPOINT FALHOU: checkpoint final incorreto: %d (esperado 101010)", ckptFinal)
	}
	t.Logf("CRITÉRIO DE CHECKPOINT APROVADO: Checkpoint final avançado monotonicamente para %d sem saltos.", ckptFinal)

	// CRITÉRIO DE RABBITMQ: Fila completamente drenada (messages=0)
	qStatusFinal, err := adminCh.QueueInspect(testQueueName)
	if err != nil || qStatusFinal.Messages != 0 {
		t.Fatalf("CRITÉRIO DE RABBITMQ FALHOU: fila ainda possui %d mensagens após conclusão do lote", qStatusFinal.Messages)
	}
	t.Log("CRITÉRIO DE RABBITMQ APROVADO: Fila drenada e zerada (messages=0).")

	t.Log("=== TESTE 1.5A-5 (LOTE HETEROGÊNEO COM CRASH E RECONCILIAÇÃO) CONCLUÍDO COM SUCESSO ===")
}



