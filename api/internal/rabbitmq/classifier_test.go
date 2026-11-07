package rabbitmq_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lib/pq"

	"github.com/dvasconcellos/elo/api/internal/delivery"
	"github.com/dvasconcellos/elo/api/internal/rabbitmq"
	"github.com/dvasconcellos/elo/api/internal/stream"
)

// -----------------------------------------------------------------------------
// GATE 1.5B-4B: TESTES UNITÁRIOS DO CLASSIFICADOR EXPLÍCITO DE ERROS
// -----------------------------------------------------------------------------

func TestClassifyError_MandatoryRules(t *testing.T) {
	bgCtx := context.Background()

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel() // cancelado imediatamente

	timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer timeoutCancel()
	time.Sleep(5 * time.Millisecond) // expirado

	tests := []struct {
		name             string
		err              error
		ctx              context.Context
		expectedCategory rabbitmq.ErrorCategory
	}{
		// 1. Identity conflict → permanent
		{
			name:             "1. Identity conflict",
			err:              delivery.ErrIdentityConflict,
			ctx:              bgCtx,
			expectedCategory: rabbitmq.CategoryPermanent,
		},
		// 2. Causal conflict → permanent
		{
			name:             "2. Causal conflict domain sentinel",
			err:              delivery.ErrCausalConflict,
			ctx:              bgCtx,
			expectedCategory: rabbitmq.CategoryPermanent,
		},
		{
			name: "2b. Causal conflict via pq 23505 with exact constraint",
			err: &pq.Error{
				Code:       "23505",
				Constraint: "uq_processed_events_consumer_agg_seq",
			},
			ctx:              bgCtx,
			expectedCategory: rabbitmq.CategoryPermanent,
		},
		// 3. Outra constraint 23505 → unknown
		{
			name: "3. Outra constraint 23505 (delivery_read_model_pkey)",
			err: &pq.Error{
				Code:       "23505",
				Constraint: "delivery_read_model_pkey",
			},
			ctx:              bgCtx,
			expectedCategory: rabbitmq.CategoryUnknown,
		},
		{
			name: "3b. Outra constraint 23505 (processed_events_pkey)",
			err: &pq.Error{
				Code:       "23505",
				Constraint: "processed_events_pkey",
			},
			ctx:              bgCtx,
			expectedCategory: rabbitmq.CategoryUnknown,
		},
		// 4. Unrecoverable gap → permanent
		{
			name:             "4. Unrecoverable gap",
			err:              stream.ErrUnrecoverableGap,
			ctx:              bgCtx,
			expectedCategory: rabbitmq.CategoryPermanent,
		},
		// 5. Poison message → permanent
		{
			name:             "5. Poison message",
			err:              rabbitmq.ErrPoisonMessage,
			ctx:              bgCtx,
			expectedCategory: rabbitmq.CategoryPermanent,
		},
		// 6. Falha de conexão PostgreSQL → transient
		{
			name: "6a. Falha de rede ECONNREFUSED",
			err: &net.OpError{
				Op:  "dial",
				Net: "tcp",
				Err: syscall.ECONNREFUSED,
			},
			ctx:              bgCtx,
			expectedCategory: rabbitmq.CategoryTransient,
		},
		{
			name:             "6b. driver.ErrBadConn",
			err:              driver.ErrBadConn,
			ctx:              bgCtx,
			expectedCategory: rabbitmq.CategoryTransient,
		},
		{
			name:             "6c. sql.ErrConnDone",
			err:              sql.ErrConnDone,
			ctx:              bgCtx,
			expectedCategory: rabbitmq.CategoryTransient,
		},
		{
			name: "6d. pq connection failure 08006",
			err: &pq.Error{
				Code: "08006",
			},
			ctx:              bgCtx,
			expectedCategory: rabbitmq.CategoryTransient,
		},
		{
			name: "6e. pq recovery 57P03",
			err: &pq.Error{
				Code: "57P03",
			},
			ctx:              bgCtx,
			expectedCategory: rabbitmq.CategoryTransient,
		},
		// 7. Deadlock 40P01 → transient
		{
			name: "7. Deadlock 40P01 (pq)",
			err: &pq.Error{
				Code: "40P01",
			},
			ctx:              bgCtx,
			expectedCategory: rabbitmq.CategoryTransient,
		},
		{
			name: "7b. Deadlock 40P01 (pgconn)",
			err: &pgconn.PgError{
				Code: "40P01",
			},
			ctx:              bgCtx,
			expectedCategory: rabbitmq.CategoryTransient,
		},
		// 8. Serialization failure 40001 → transient
		{
			name: "8. Serialization failure 40001 (pq)",
			err: &pq.Error{
				Code: "40001",
			},
			ctx:              bgCtx,
			expectedCategory: rabbitmq.CategoryTransient,
		},
		{
			name: "8b. Serialization failure 40001 (pgconn)",
			err: &pgconn.PgError{
				Code: "40001",
			},
			ctx:              bgCtx,
			expectedCategory: rabbitmq.CategoryTransient,
		},
		// 9. Lock timeout 55P03 → transient
		{
			name: "9. Lock timeout 55P03",
			err: &pq.Error{
				Code: "55P03",
			},
			ctx:              bgCtx,
			expectedCategory: rabbitmq.CategoryTransient,
		},
		// 10. Statement timeout 57014 → transient
		{
			name: "10. Statement timeout 57014 com background context",
			err: &pq.Error{
				Code:    "57014",
				Message: "canceling statement due to statement timeout",
			},
			ctx:              bgCtx,
			expectedCategory: rabbitmq.CategoryTransient,
		},
		{
			name:             "10b. Context deadline exceeded",
			err:              context.DeadlineExceeded,
			ctx:              timeoutCtx,
			expectedCategory: rabbitmq.CategoryTransient,
		},
		// 11. Cancelamento de shutdown → contextual, sem descarte
		{
			name:             "11a. Context canceled explícito no erro",
			err:              context.Canceled,
			ctx:              bgCtx,
			expectedCategory: rabbitmq.CategoryContextCanceled,
		},
		{
			name: "11b. 57014 decorrente de context canceled",
			err: &pq.Error{
				Code:    "57014",
				Message: "canceling statement due to user request",
			},
			ctx:              canceledCtx,
			expectedCategory: rabbitmq.CategoryContextCanceled,
		},
		{
			name:             "11c. Context cancelado pelo ctx.Err()",
			err:              errors.New("db error while canceling"),
			ctx:              canceledCtx,
			expectedCategory: rabbitmq.CategoryContextCanceled,
		},
		// 12. Erro desconhecido → unknown
		{
			name:             "12. Erro genérico/desconhecido",
			err:              errors.New("unexpected synthetic calculation panic"),
			ctx:              bgCtx,
			expectedCategory: rabbitmq.CategoryUnknown,
		},
		// 13. Wrapping com %w → classificação preservada
		{
			name:             "13a. Deep wrap de ErrIdentityConflict",
			err:              fmt.Errorf("layer 3: %w", fmt.Errorf("layer 2: %w", fmt.Errorf("layer 1: %w", delivery.ErrIdentityConflict))),
			ctx:              bgCtx,
			expectedCategory: rabbitmq.CategoryPermanent,
		},
		{
			name: "13b. Deep wrap de pq.Error deadlock 40P01",
			err:  fmt.Errorf("execute tx: %w", fmt.Errorf("query delivery: %w", &pq.Error{Code: "40P01"})),
			ctx:  bgCtx,
			expectedCategory: rabbitmq.CategoryTransient,
		},
		{
			name: "13c. Deep wrap de non-causal 23505",
			err:  fmt.Errorf("insert read model: %w", &pq.Error{Code: "23505", Constraint: "delivery_read_model_pkey"}),
			ctx:  bgCtx,
			expectedCategory: rabbitmq.CategoryUnknown,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := rabbitmq.ClassifyError(tc.err, tc.ctx)
			if res.Category != tc.expectedCategory {
				t.Fatalf("esperava categoria %q, obteve %q (motivo: %s)", tc.expectedCategory, res.Category, res.Reason)
			}
			if res.Reason == "" {
				t.Fatalf("esperava motivo não-vazio para classificação")
			}
		})
	}
}
