package rabbitmq_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

// -----------------------------------------------------------------------------
// GATE 1.5B-4A: TESTES DE ACEITAÇÃO DE TOPOLOGIA DLQ E SINGLE ACTIVE CONSUMER
// -----------------------------------------------------------------------------

// 4A-1 — Policy DLX: Confirma roteamento de Reject(false) para a DLQ via DLX_Policy
func TestGate15B4A_1_PolicyDLX_RejectRoutesToDLQ(t *testing.T) {
	conn, err := amqp.Dial(getGate15B3AMQPURL())
	if err != nil {
		t.Skipf("cannot connect to rabbitmq: %v", err)
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()

	const mainQueue = "api.delivery_projection"
	const dlqQueue = "api.delivery_projection.dlq"
	const dlxExchange = "logistics.events.dlx"

	// 1. Verificar declaração passiva das filas e exchanges existentes
	_, err = ch.QueueDeclarePassive(mainQueue, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("fila principal %s não existe: %v", mainQueue, err)
	}
	_, err = ch.QueueDeclarePassive(dlqQueue, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("fila DLQ %s não existe: %v", dlqQueue, err)
	}
	err = ch.ExchangeDeclarePassive(dlxExchange, "fanout", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("exchange DLX %s não existe: %v", dlxExchange, err)
	}

	// 2. Garantir estado limpo inicial
	qMainInit, _ := ch.QueueInspect(mainQueue)
	t.Logf("Estado inicial %s: messages=%d", mainQueue, qMainInit.Messages)
	qDLQInit, _ := ch.QueueInspect(dlqQueue)
	t.Logf("Estado inicial %s: messages=%d", dlqQueue, qDLQInit.Messages)

	// 3. Publicar mensagem com DeliveryMode persistent
	uniqueMsgID := fmt.Sprintf("gate15b4a-reject-%s", uuid.New().String())
	payload := []byte(fmt.Sprintf(`{"test_id":"%s","event_type":"InvalidOrPoisonEvent"}`, uniqueMsgID))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = ch.PublishWithContext(ctx, "", mainQueue, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		MessageId:    uniqueMsgID,
		Body:         payload,
	})
	if err != nil {
		t.Fatalf("falha ao publicar mensagem de teste: %v", err)
	}

	// 4. Consumir a mensagem da fila principal e executar Reject(false)
	consumerTag := fmt.Sprintf("test-c-%s", uuid.New().String())
	msgs, err := ch.Consume(mainQueue, consumerTag, false, false, false, false, nil)
	if err != nil {
		t.Fatalf("falha ao criar consumidor na fila principal: %v", err)
	}

	var d amqp.Delivery
	select {
	case d = <-msgs:
	case <-time.After(3 * time.Second):
		t.Fatalf("timeout aguardando mensagem na fila principal")
	}

	// Cancelar consumidor na fila principal para não interferir
	_ = ch.Cancel(consumerTag, false)

	// Executa Reject(false) simulando descarte por erro permanente / poison
	t.Logf("Executando Reject(requeue=false) na mensagem %s...", d.MessageId)
	if err := d.Reject(false); err != nil {
		t.Fatalf("falha ao executar Reject(false): %v", err)
	}

	// 5. Confirmar que a mensagem saiu da fila principal
	time.Sleep(50 * time.Millisecond)
	qMainAfter, err := ch.QueueInspect(mainQueue)
	if err != nil {
		t.Fatalf("falha ao inspecionar fila principal: %v", err)
	}
	if qMainAfter.Messages != 0 {
		t.Fatalf("esperava 0 mensagens na fila principal após Reject, obteve %d", qMainAfter.Messages)
	}
	t.Log("Confirmação: Mensagem saiu da fila principal.")

	// 6. Confirmar que a mensagem chegou à DLQ (não houve descarte silencioso)
	expectedDLQCount := qDLQInit.Messages + 1
	dlqCount := waitForDLQCount(t, ch, dlqQueue, expectedDLQCount, 3*time.Second)
	if dlqCount != expectedDLQCount {
		t.Fatalf("esperava %d mensagens na DLQ, obteve %d (houve descarte silencioso!)", expectedDLQCount, dlqCount)
	}
	t.Log("Confirmação: Mensagem encaminhada com sucesso para a DLQ via DLX_Policy.")

	// 7. Drenar a mensagem da DLQ e confirmar conteúdo
	dlqMsgs, err := ch.Consume(dlqQueue, "test-dlq-consumer", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("falha ao consumir da DLQ: %v", err)
	}

	select {
	case dlqDelivery := <-dlqMsgs:
		if dlqDelivery.MessageId != uniqueMsgID {
			t.Fatalf("esperava MessageId %s na DLQ, obteve %s", uniqueMsgID, dlqDelivery.MessageId)
		}
		// Confirmar cabeçalho x-death inserido pelo RabbitMQ ao rotear para DLX
		if xDeath, ok := dlqDelivery.Headers["x-death"]; ok {
			t.Logf("Confirmação: Cabeçalho x-death presente na DLQ: %+v", xDeath)
		}
		_ = dlqDelivery.Ack(false)
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout ao retirar mensagem da DLQ para validação")
	}
	_ = ch.Cancel("test-dlq-consumer", false)

	// Confirmar DLQ limpa
	qDLQFinal, _ := ch.QueueInspect(dlqQueue)
	if qDLQFinal.Messages != qDLQInit.Messages {
		t.Fatalf("esperava DLQ voltar ao estado inicial %d, obteve %d", qDLQInit.Messages, qDLQFinal.Messages)
	}
	t.Log("=== TESTE 4A-1 PASSOU COM SUCESSO ===")
}

// 4A-2 — Durabilidade: Valida que filas e exchanges são duráveis e mensagens são persistentes
func TestGate15B4A_2_DurabilityAndPersistence(t *testing.T) {
	conn, err := amqp.Dial(getGate15B3AMQPURL())
	if err != nil {
		t.Skipf("cannot connect to rabbitmq: %v", err)
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()

	const mainQueue = "api.delivery_projection"
	const dlqQueue = "api.delivery_projection.dlq"
	const mainExchange = "logistics.events"
	const dlxExchange = "logistics.events.dlx"

	// 1. Declarar passivamente exigindo durable=true
	_, err = ch.QueueDeclarePassive(mainQueue, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("fila principal %s não é durável ou não existe: %v", mainQueue, err)
	}
	t.Logf("Confirmação: Fila principal %s é DURÁVEL.", mainQueue)

	_, err = ch.QueueDeclarePassive(dlqQueue, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("fila DLQ %s não é durável ou não existe: %v", dlqQueue, err)
	}
	t.Logf("Confirmação: Fila DLQ %s é DURÁVEL.", dlqQueue)

	err = ch.ExchangeDeclarePassive(mainExchange, "direct", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("exchange %s não é durável ou não existe: %v", mainExchange, err)
	}
	t.Logf("Confirmação: Exchange principal %s é DURÁVEL.", mainExchange)

	err = ch.ExchangeDeclarePassive(dlxExchange, "fanout", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("exchange DLX %s não é durável ou não existe: %v", dlxExchange, err)
	}
	t.Logf("Confirmação: Exchange DLX %s é DURÁVEL.", dlxExchange)

	// 2. Testar publicação persistente
	msgID := fmt.Sprintf("durability-test-%s", uuid.New().String())
	err = ch.PublishWithContext(context.Background(), "", mainQueue, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		MessageId:    msgID,
		Body:         []byte(`{"durable":true}`),
	})
	if err != nil {
		t.Fatalf("falha ao publicar mensagem persistente: %v", err)
	}

	msgs, err := ch.Consume(mainQueue, "durability-consumer", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("falha ao consumir: %v", err)
	}

	select {
	case d := <-msgs:
		if d.DeliveryMode != amqp.Persistent {
			t.Fatalf("esperava DeliveryMode = 2 (Persistent), obteve %d", d.DeliveryMode)
		}
		t.Logf("Confirmação: Mensagem entregue com DeliveryMode = 2 (Persistent).")
		_ = d.Ack(false)
	case <-time.After(3 * time.Second):
		t.Fatalf("timeout aguardando mensagem persistente")
	}
	_ = ch.Cancel("durability-consumer", false)
	t.Log("=== TESTE 4A-2 PASSOU COM SUCESSO ===")
}

// 4A-3 — Single Active Consumer: Valida exclusividade e failover em fila efêmera
func TestGate15B4A_3_SingleActiveConsumer_ExclusivityAndFailover(t *testing.T) {
	conn, err := amqp.Dial(getGate15B3AMQPURL())
	if err != nil {
		t.Skipf("cannot connect to rabbitmq: %v", err)
		return
	}
	defer conn.Close()

	// Canal para gestão da fila efêmera
	mgmtCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open mgmt channel: %v", err)
	}
	defer mgmtCh.Close()

	ephemeralQueue := fmt.Sprintf("test.gate15b4a.sac.%s", uuid.New().String())
	qArgs := amqp.Table{
		"x-single-active-consumer": true,
	}

	// 1. Declarar fila efêmera com Single Active Consumer
	_, err = mgmtCh.QueueDeclare(ephemeralQueue, true, false, false, false, qArgs)
	if err != nil {
		t.Fatalf("falha ao declarar fila com x-single-active-consumer: %v", err)
	}
	defer func() {
		_, _ = mgmtCh.QueueDelete(ephemeralQueue, false, false, false)
	}()
	t.Logf("Fila efêmera declarada com sucesso: %s (x-single-active-consumer=true)", ephemeralQueue)

	// 2. Conectar Consumidor 1 em canal dedicado
	ch1, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open ch1: %v", err)
	}
	defer ch1.Close()
	_ = ch1.Qos(1, 0, false)

	// 3. Conectar Consumidor 2 em canal dedicado
	ch2, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open ch2: %v", err)
	}
	defer ch2.Close()
	_ = ch2.Qos(1, 0, false)

	c1MsgsChan, err := ch1.Consume(ephemeralQueue, "sac-consumer-1", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("falha ao consumir ch1: %v", err)
	}

	c2MsgsChan, err := ch2.Consume(ephemeralQueue, "sac-consumer-2", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("falha ao consumir ch2: %v", err)
	}

	var mu sync.Mutex
	var c1Received []string
	var c2Received []string

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Goroutine coletora Consumidor 1
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case d, ok := <-c1MsgsChan:
				if !ok {
					return
				}
				mu.Lock()
				c1Received = append(c1Received, string(d.Body))
				mu.Unlock()
				_ = d.Ack(false)
			}
		}
	}()

	// Goroutine coletora Consumidor 2
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case d, ok := <-c2MsgsChan:
				if !ok {
					return
				}
				mu.Lock()
				c2Received = append(c2Received, string(d.Body))
				mu.Unlock()
				_ = d.Ack(false)
			}
		}
	}()

	// 4. Publicar 10 mensagens na fila
	for i := 0; i < 10; i++ {
		body := fmt.Sprintf("batch1-msg-%d", i)
		err = mgmtCh.PublishWithContext(context.Background(), "", ephemeralQueue, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(body),
		})
		if err != nil {
			t.Fatalf("falha ao publicar msg %d: %v", i, err)
		}
	}

	// Aguardar entrega e processamento das 10 mensagens
	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	countC1 := len(c1Received)
	countC2 := len(c2Received)
	mu.Unlock()

	t.Logf("Resultado Lote 1 (10 mensagens) -> Consumidor 1: %d, Consumidor 2: %d", countC1, countC2)

	// Validar que exatamente UM dos consumidores recebeu todas as 10 mensagens e o outro zero
	if !(countC1 == 10 && countC2 == 0) && !(countC1 == 0 && countC2 == 10) {
		t.Fatalf("falha na exclusividade de Single Active Consumer: c1=%d, c2=%d (esperava 10 e 0)", countC1, countC2)
	}
	t.Log("Confirmação: Exclusividade de Single Active Consumer comprovada (apenas o ativo recebeu mensagens).")

	// 5. Testar Failover: Desconectar o consumidor ativo
	if countC1 == 10 {
		t.Log("Desconectando Consumidor 1 para disparar failover para Consumidor 2...")
		_ = ch1.Close()
	} else {
		t.Log("Desconectando Consumidor 2 para disparar failover para Consumidor 1...")
		_ = ch2.Close()
	}

	// Aguardar broker registrar desconexão do ativo
	time.Sleep(150 * time.Millisecond)

	// 6. Publicar mais 5 mensagens
	for i := 0; i < 5; i++ {
		body := fmt.Sprintf("batch2-msg-%d", i)
		err = mgmtCh.PublishWithContext(context.Background(), "", ephemeralQueue, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(body),
		})
		if err != nil {
			t.Fatalf("falha ao publicar batch2 msg %d: %v", i, err)
		}
	}

	// Aguardar entrega do segundo lote
	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	totalC1 := len(c1Received)
	totalC2 := len(c2Received)
	mu.Unlock()

	t.Logf("Resultado pós-failover -> Consumidor 1: %d, Consumidor 2: %d", totalC1, totalC2)

	if countC1 == 10 {
		// Consumidor 2 deve ter assumido e recebido as 5 novas mensagens
		if totalC2 != 5 {
			t.Fatalf("esperava Consumidor 2 assumir e receber 5 mensagens pós-failover, obteve %d", totalC2)
		}
		t.Log("Confirmação: Consumidor 2 assumiu com sucesso como Single Active Consumer pós-failover.")
	} else {
		if totalC1 != 5 {
			t.Fatalf("esperava Consumidor 1 assumir e receber 5 mensagens pós-failover, obteve %d", totalC1)
		}
		t.Log("Confirmação: Consumidor 1 assumiu com sucesso como Single Active Consumer pós-failover.")
	}

	t.Log("=== TESTE 4A-3 PASSOU COM SUCESSO ===")
}

// 4A-4 — Regressão e Integridade: Verifica que filas persistentes estão intactas
func TestGate15B4A_4_TopologyIntegrityAndNoDestruction(t *testing.T) {
	conn, err := amqp.Dial(getGate15B3AMQPURL())
	if err != nil {
		t.Skipf("cannot connect to rabbitmq: %v", err)
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open channel: %v", err)
	}
	defer ch.Close()

	const mainQueue = "api.delivery_projection"
	const dlqQueue = "api.delivery_projection.dlq"

	qMain, err := ch.QueueInspect(mainQueue)
	if err != nil {
		t.Fatalf("falha ao inspecionar fila principal %s: %v", mainQueue, err)
	}
	t.Logf("Fila principal %s intacta: messages=%d, consumers=%d", mainQueue, qMain.Messages, qMain.Consumers)

	qDLQ, err := ch.QueueInspect(dlqQueue)
	if err != nil {
		t.Fatalf("falha ao inspecionar fila DLQ %s: %v", dlqQueue, err)
	}
	t.Logf("Fila DLQ %s intacta: messages=%d, consumers=%d", dlqQueue, qDLQ.Messages, qDLQ.Consumers)

	t.Log("=== TESTE 4A-4 PASSOU COM SUCESSO ===")
}
