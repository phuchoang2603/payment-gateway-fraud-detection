package metrics

import (
	"context"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/notifier/internal/domain"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/observe"
	"go.opentelemetry.io/otel/metric"
)

type Metrics struct {
	deliveries, consumed, consumerErrors                 metric.Int64Counter
	backlog, oldestPending, lastEventAge, lastConsumedAt metric.Float64Gauge
}

func New() *Metrics {
	return &Metrics{
		deliveries:     observe.Counter("notifier.deliveries", "{attempt}"),
		consumed:       observe.Counter("notifier.consumed.events", "{event}"),
		consumerErrors: observe.Counter("notifier.consumer.errors", "{error}"),
		backlog:        observe.Gauge("notifier.deliveries.pending", "{delivery}"),
		oldestPending:  observe.Gauge("notifier.oldest.pending.age", "s"),
		lastEventAge:   observe.Gauge("notifier.consumer.last.event.age", "s"),
		lastConsumedAt: observe.Gauge("notifier.consumer.last.commit.timestamp", "s"),
	}
}
func (m *Metrics) Delivery(outcome string) { observe.Count(m.deliveries, "outcome", outcome) }
func (m *Metrics) Backlog(stats domain.Stats) {
	m.backlog.Record(context.Background(), float64(stats.Pending))
	m.oldestPending.Record(context.Background(), stats.OldestPending.Seconds())
}
func (m *Metrics) Consumed(result string)     { observe.Count(m.consumed, "result", result) }
func (m *Metrics) ConsumerError(stage string) { observe.Count(m.consumerErrors, "stage", stage) }
func (m *Metrics) EventAge(age time.Duration) {
	m.lastEventAge.Record(context.Background(), age.Seconds())
}

func (m *Metrics) Committed(at time.Time) {
	m.lastConsumedAt.Record(context.Background(), float64(at.Unix()))
}
