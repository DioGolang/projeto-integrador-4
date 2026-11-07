package rabbitmq

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

// RandomSource abstrai o gerador de números pseudoaleatórios para testes determinísticos.
type RandomSource interface {
	Int63n(n int64) int64
}

// Sleeper define a função de pausa ciente de cancelamento de contexto.
type Sleeper func(ctx context.Context, d time.Duration) error

// DefaultSleeper realiza a pausa respeitando a expiração ou cancelamento do contexto.
func DefaultSleeper(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// BackoffCalculator encapsula o algoritmo de Exponential Backoff com Full Jitter.
// delay = random(0, min(maxDelay, initialDelay * 2^attempt))
type BackoffCalculator struct {
	initialDelay time.Duration
	maxDelay     time.Duration
	rng          RandomSource
	mu           sync.Mutex
}

// NewBackoffCalculator inicializa o calculador com os parâmetros definidos no ADR 009.
func NewBackoffCalculator(initialDelay, maxDelay time.Duration, rng RandomSource) *BackoffCalculator {
	if initialDelay <= 0 {
		initialDelay = 100 * time.Millisecond
	}
	if maxDelay <= 0 {
		maxDelay = 5 * time.Second
	}
	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return &BackoffCalculator{
		initialDelay: initialDelay,
		maxDelay:     maxDelay,
		rng:          rng,
	}
}

// CalculateCeiling calcula o teto determinístico min(maxDelay, initialDelay * 2^attempt).
func (b *BackoffCalculator) CalculateCeiling(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	// Previne overflow de int64; 2^30 multiplicado por 100ms ultrapassa maxDelay com folga
	if attempt > 30 {
		return b.maxDelay
	}
	multiplier := int64(1) << attempt
	ceiling := b.initialDelay * time.Duration(multiplier)
	if ceiling > b.maxDelay || ceiling < 0 {
		return b.maxDelay
	}
	return ceiling
}

// CalculateDelay aplica Full Jitter sobre o teto do nível attempt: random(0, ceiling).
func (b *BackoffCalculator) CalculateDelay(attempt int) time.Duration {
	ceiling := b.CalculateCeiling(attempt)
	if ceiling <= 0 {
		return 0
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// random(0, ceiling) inclusivo
	val := b.rng.Int63n(int64(ceiling) + 1)
	return time.Duration(val)
}
