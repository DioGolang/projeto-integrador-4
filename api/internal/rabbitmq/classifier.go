package rabbitmq

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lib/pq"

	"github.com/dvasconcellos/elo/api/internal/delivery"
	"github.com/dvasconcellos/elo/api/internal/stream"
)

// ErrorCategory representa a classificação de severidade e recuperabilidade de um erro.
type ErrorCategory string

const (
	// CategoryPermanent representa um erro conhecido e irrecuperável para a mensagem corrente (DLQ imediata).
	CategoryPermanent ErrorCategory = "permanent"

	// CategoryTransient representa uma falha operacional potencialmente recuperável (requeue / retry).
	CategoryTransient ErrorCategory = "transient"

	// CategoryContextCanceled representa interrupção por cancelamento de contexto ou shutdown (não descartar).
	CategoryContextCanceled ErrorCategory = "context_canceled"

	// CategoryUnknown representa um erro não classificado; nunca deve sofrer descarte ou DLQ automática.
	CategoryUnknown ErrorCategory = "unknown"
)

// CausalConstraintName é o nome exato da restrição única no PostgreSQL que protege a ordem causal.
const CausalConstraintName = "uq_processed_events_consumer_agg_seq"

// Classification contém a categoria de erro e a justificativa técnica correspondente.
type Classification struct {
	Category ErrorCategory
	Reason   string
}

// ClassifyError avalia um erro e seu contexto de execução para determinar se ele é permanente,
// transitório, decorrente de cancelamento/shutdown ou desconhecido.
// Preserva a inspeção de erros encapsulados com %w em qualquer profundidade.
func ClassifyError(err error, ctx context.Context) Classification {
	if err == nil {
		return Classification{Category: CategoryUnknown, Reason: "nil error"}
	}

	// 1. Context Cancellation / Shutdown (Regra 8 e 11)
	// Deve ser verificado prioritariamente para não confundir shutdown com erro de consulta.
	if errors.Is(err, context.Canceled) || (ctx != nil && ctx.Err() == context.Canceled) {
		return Classification{
			Category: CategoryContextCanceled,
			Reason:   "operation interrupted by context cancellation or application shutdown",
		}
	}

	// 2. Context Deadline Exceeded (Regra 7 e 8)
	if errors.Is(err, context.DeadlineExceeded) || (ctx != nil && ctx.Err() == context.DeadlineExceeded) {
		return Classification{
			Category: CategoryTransient,
			Reason:   "operation timed out (context deadline exceeded)",
		}
	}

	// 3. Erros Permanentes Conhecidos de Domínio, Stream e Envelope (Regras 1, 2, 3 e 4)
	if errors.Is(err, delivery.ErrIdentityConflict) {
		return Classification{
			Category: CategoryPermanent,
			Reason:   "identity conflict: event_id already registered with different payload fingerprint",
		}
	}
	if errors.Is(err, delivery.ErrCausalConflict) {
		return Classification{
			Category: CategoryPermanent,
			Reason:   "causal conflict: sequence ordinal already occupied by another event",
		}
	}
	if errors.Is(err, stream.ErrUnrecoverableGap) {
		return Classification{
			Category: CategoryPermanent,
			Reason:   "unrecoverable gap in aggregate sequence stream",
		}
	}
	if errors.Is(err, ErrPoisonMessage) {
		return Classification{
			Category: CategoryPermanent,
			Reason:   "poison message: envelope or domain payload validation failure",
		}
	}

	// 4. Erros Específicos do PostgreSQL via lib/pq e pgx/pgconn (Regras 5, 6, 7 e 8)
	var (
		pgCode       string
		pgConstraint string
		isPgErr      bool
	)

	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		pgCode = string(pqErr.Code)
		pgConstraint = pqErr.Constraint
		isPgErr = true
	}

	var pgconnErr *pgconn.PgError
	if !isPgErr && errors.As(err, &pgconnErr) {
		pgCode = pgconnErr.Code
		pgConstraint = pgconnErr.ConstraintName
		isPgErr = true
	}

	if isPgErr {
		switch pgCode {
		case "23505": // unique_violation
			// Regra 5: Somente causal quando a constraint for exatamente uq_processed_events_consumer_agg_seq
			if pgConstraint == CausalConstraintName {
				return Classification{
					Category: CategoryPermanent,
					Reason:   "causal conflict: unique constraint " + CausalConstraintName + " violated",
				}
			}
			// Regra 6: Outras violações 23505 são Unknown, sem DLQ automática
			return Classification{
				Category: CategoryUnknown,
				Reason:   "non-causal unique constraint violation: " + pgConstraint,
			}

		case "57014": // query_canceled (Regra 8)
			// Distinguir statement timeout de cancelamento por shutdown
			if (ctx != nil && ctx.Err() == context.Canceled) || errors.Is(err, context.Canceled) {
				return Classification{
					Category: CategoryContextCanceled,
					Reason:   "query canceled by client shutdown/cancellation",
				}
			}
			return Classification{
				Category: CategoryTransient,
				Reason:   "query canceled due to statement/execution timeout",
			}

		// Concorrência e Transação (Regra 7)
		case "40P01": // deadlock_detected
			return Classification{
				Category: CategoryTransient,
				Reason:   "database deadlock detected (40P01)",
			}
		case "40001": // serialization_failure
			return Classification{
				Category: CategoryTransient,
				Reason:   "database serialization failure (40001)",
			}
		case "55P03": // lock_not_available
			return Classification{
				Category: CategoryTransient,
				Reason:   "database lock not available / lock timeout (55P03)",
			}

		// Shutdown do Servidor e Falhas de Conexão SQLSTATE (Regra 7)
		case "57P01": // admin_shutdown
			return Classification{
				Category: CategoryTransient,
				Reason:   "postgresql admin shutdown in progress (57P01)",
			}
		case "57P02": // crash_shutdown
			return Classification{
				Category: CategoryTransient,
				Reason:   "postgresql crash shutdown in progress (57P02)",
			}
		case "57P03": // cannot_connect_now
			return Classification{
				Category: CategoryTransient,
				Reason:   "postgresql recovery in progress / cannot connect now (57P03)",
			}
		case "08006": // connection_failure
			return Classification{
				Category: CategoryTransient,
				Reason:   "postgresql connection failure (08006)",
			}
		case "08001": // sqlclient_unable_to_establish_sqlconnection
			return Classification{
				Category: CategoryTransient,
				Reason:   "unable to establish postgresql connection (08001)",
			}
		case "08000", "08003", "08004":
			return Classification{
				Category: CategoryTransient,
				Reason:   "postgresql connection exception: " + pgCode,
			}
		}
	}

	// 5. Driver de Banco de Dados e Conexões Desfeitas (Regra 7)
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, sql.ErrConnDone) {
		return Classification{
			Category: CategoryTransient,
			Reason:   "bad or expired database connection",
		}
	}

	// Falhas de rede direta (Syscall ECONNREFUSED, ECONNRESET, EPIPE)
	if errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) {
		return Classification{
			Category: CategoryTransient,
			Reason:   "network socket error (connection refused / reset / broken pipe)",
		}
	}

	var netOpErr *net.OpError
	if errors.As(err, &netOpErr) {
		return Classification{
			Category: CategoryTransient,
			Reason:   fmt.Sprintf("network operational error during database I/O: %v", netOpErr),
		}
	}

	var syscallErr *os.SyscallError
	if errors.As(err, &syscallErr) {
		if errors.Is(syscallErr.Err, syscall.ECONNREFUSED) ||
			errors.Is(syscallErr.Err, syscall.ECONNRESET) ||
			errors.Is(syscallErr.Err, syscall.EPIPE) {
			return Classification{
				Category: CategoryTransient,
				Reason:   fmt.Sprintf("network syscall error: %v", syscallErr),
			}
		}
	}

	// 6. Erro Desconhecido / Não Classificado (Regra 9 e 12)
	return Classification{
		Category: CategoryUnknown,
		Reason:   "unclassified error: " + err.Error(),
	}
}
