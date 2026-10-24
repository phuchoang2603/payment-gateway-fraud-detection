package application

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/observe"
	"go.opentelemetry.io/otel"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/notifier/internal/domain"
)

type Queue interface {
	Claim(ctx context.Context, now time.Time, limit int, lease time.Duration) ([]domain.Delivery, error)
	Record(ctx context.Context, eventID string, attempt domain.Attempt) error
	Stats(ctx context.Context, now time.Time) (domain.Stats, error)
}

type Request struct {
	URL       string
	EventID   string
	Signature string
	Body      []byte
}

type Sender interface {
	Send(ctx context.Context, request Request) (status int, err error)
}

type DeliveryMetrics interface {
	Delivery(outcome string)
	Backlog(stats domain.Stats)
}

type DeliveryWorker struct {
	Queue        Queue
	Sender       Sender
	Integrations domain.Integrations
	Schedule     domain.Schedule
	Metrics      DeliveryMetrics
	Now          func() time.Time
	Jitter       func() float64
	BatchSize    int
	Concurrency  int
	Lease        time.Duration
	PollInterval time.Duration
}

func (w *DeliveryWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.PollInterval)
	defer ticker.Stop()
	for {
		if _, err := w.Tick(ctx); err != nil && ctx.Err() == nil {
			slog.ErrorContext(ctx, "delivery tick failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *DeliveryWorker) Tick(ctx context.Context) (int, error) {
	now := w.Now()
	if stats, err := w.Queue.Stats(ctx, now); err == nil {
		w.Metrics.Backlog(stats)
	}
	claimed, err := w.Queue.Claim(ctx, now, w.BatchSize, w.Lease)
	if err != nil {
		return 0, err
	}
	var wg sync.WaitGroup
	slots := make(chan struct{}, max(w.Concurrency, 1))
	for _, delivery := range claimed {
		slots <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			w.deliver(ctx, delivery)
		}()
	}
	wg.Wait()
	return len(claimed), nil
}

func (w *DeliveryWorker) deliver(ctx context.Context, delivery domain.Delivery) {
	ctx, span := otel.Tracer("notifier").Start(observe.RestoreContext(ctx, delivery.TraceContext), "webhook.delivery.attempt")
	defer span.End()
	attemptNumber := delivery.Attempts + 1
	attempt := w.attempt(ctx, delivery, attemptNumber)
	switch attempt.Status {
	case domain.DeliveryDelivered:
		w.Metrics.Delivery("delivered")
	case domain.DeliveryFailed:
		w.Metrics.Delivery("failed")
	default:
		w.Metrics.Delivery("retry")
	}
	if err := w.Queue.Record(context.WithoutCancel(ctx), delivery.EventID, attempt); err != nil {
		slog.ErrorContext(ctx, "record delivery attempt", "event_id", delivery.EventID, "error", err)
		return
	}
	slog.InfoContext(ctx, "webhook delivery attempt", "event_id", delivery.EventID, "payment_id", delivery.PaymentID, "attempt", attemptNumber, "status", attempt.Status)
}

func (w *DeliveryWorker) attempt(ctx context.Context, delivery domain.Delivery, attemptNumber int) domain.Attempt {
	now := w.Now()
	failure := func(status *int, reason string) domain.Attempt {
		if w.Schedule.Exhausted(attemptNumber) {
			return domain.Attempt{Status: domain.DeliveryFailed, NextAttemptAt: now, LastStatus: status, LastError: reason}
		}
		return domain.Attempt{
			Status:        domain.DeliveryPending,
			NextAttemptAt: now.Add(w.Schedule.Delay(attemptNumber, w.Jitter())),
			LastStatus:    status,
			LastError:     reason,
		}
	}

	integration, ok := w.Integrations.Lookup(delivery.IntegrationID)
	if !ok || integration.Webhook == nil || integration.Webhook.URL == "" {
		return domain.Attempt{Status: domain.DeliveryFailed, NextAttemptAt: now, LastError: "integration has no webhook endpoint"}
	}

	status, err := w.Sender.Send(ctx, Request{
		URL:       integration.Webhook.URL,
		EventID:   delivery.EventID,
		Signature: domain.Sign(integration.Webhook.Secret, now, delivery.Body),
		Body:      delivery.Body,
	})
	if err != nil {
		return failure(nil, err.Error())
	}
	if status >= http.StatusOK && status < http.StatusMultipleChoices {
		return domain.Attempt{Status: domain.DeliveryDelivered, NextAttemptAt: now, LastStatus: &status, DeliveredAt: &now}
	}
	return failure(&status, errors.New(http.StatusText(status)).Error())
}
