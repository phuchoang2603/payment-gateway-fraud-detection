package metrics

import (
	"context"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/observe"
	"go.opentelemetry.io/otel/metric"
)

type Metrics struct {
	attemptLatency                      metric.Float64Histogram
	attemptFailures                     metric.Int64Counter
	pendingOutcomeAge                   metric.Float64Gauge
	redisHits, redisMisses, redisErrors metric.Int64Counter
}

func New() *Metrics {
	return &Metrics{
		attemptLatency:    observe.Histogram("payments.attempt.duration", "s"),
		attemptFailures:   observe.Counter("payments.attempt.failures", "{attempt}"),
		pendingOutcomeAge: observe.Gauge("payments.pending.outcome.age", "s"),
		redisHits:         observe.Counter("payments.redis.idempotency.hits", "{lookup}"),
		redisMisses:       observe.Counter("payments.redis.idempotency.misses", "{lookup}"),
		redisErrors:       observe.Counter("payments.redis.idempotency.errors", "{operation}"),
	}
}

func (m *Metrics) ObserveAttempt(duration time.Duration) {
	m.attemptLatency.Record(context.Background(), duration.Seconds())
}

func (m *Metrics) AttemptFailure(reason string) { observe.Count(m.attemptFailures, "reason", reason) }

func (m *Metrics) SetPendingOutcomeAge(age time.Duration) {
	m.pendingOutcomeAge.Record(context.Background(), age.Seconds())
}
func (m *Metrics) CacheHit()   { m.redisHits.Add(context.Background(), 1) }
func (m *Metrics) CacheMiss()  { m.redisMisses.Add(context.Background(), 1) }
func (m *Metrics) CacheError() { m.redisErrors.Add(context.Background(), 1) }
