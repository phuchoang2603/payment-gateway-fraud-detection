package workers

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/application"
)

const batchSize = 50

type Elector interface {
	Run(ctx context.Context, work func(ctx context.Context))
}

type AgeReporter interface {
	SetPendingOutcomeAge(age time.Duration)
}

type Config struct {
	RecoveryGrace time.Duration
	Interval      time.Duration
}

type Workers struct {
	config   Config
	service  *application.Service
	elector  Elector
	reporter AgeReporter
}

func New(config Config, service *application.Service, elector Elector, reporter AgeReporter) *Workers {
	return &Workers{config: config, service: service, elector: elector, reporter: reporter}
}

func (w *Workers) Run(ctx context.Context) {
	var group sync.WaitGroup
	group.Add(2)
	go func() {
		defer group.Done()
		w.elector.Run(ctx, w.lead)
	}()
	go func() {
		defer group.Done()
		w.reportAge(ctx)
	}()
	group.Wait()
}

func (w *Workers) lead(ctx context.Context) {
	slog.InfoContext(ctx, "payments workers elected leader")
	ticker := time.NewTicker(w.config.Interval)
	defer ticker.Stop()
	for {
		if _, err := w.RecoverOnce(ctx); err != nil && ctx.Err() == nil {
			slog.WarnContext(ctx, "recovery pass failed", "error", err)
		}
		if _, err := w.ExpireOnce(ctx); err != nil && ctx.Err() == nil {
			slog.WarnContext(ctx, "expiry pass failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *Workers) RecoverOnce(ctx context.Context) (int, error) {
	candidates, err := w.service.RecoveryCandidates(ctx, w.config.RecoveryGrace, batchSize)
	if err != nil {
		return 0, err
	}
	recovered := 0
	for _, candidate := range candidates {
		if err := w.service.RecoverAttempt(ctx, candidate); err != nil {
			slog.WarnContext(ctx, "recover attempt", "payment_id", candidate.PaymentID, "attempt_id", candidate.AttemptID, "error", err)
			continue
		}
		recovered++
	}
	return recovered, nil
}

func (w *Workers) ExpireOnce(ctx context.Context) (int, error) {
	payments, err := w.service.ExpiryCandidates(ctx, batchSize)
	if err != nil {
		return 0, err
	}
	expired := 0
	for _, paymentID := range payments {
		if err := w.service.ExpirePayment(ctx, paymentID); err != nil {
			slog.WarnContext(ctx, "expire payment", "payment_id", paymentID, "error", err)
			continue
		}
		expired++
	}
	return expired, nil
}

func (w *Workers) reportAge(ctx context.Context) {
	ticker := time.NewTicker(w.config.Interval)
	defer ticker.Stop()
	for {
		queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		age, err := w.service.OldestAwaitingProcessor(queryCtx)
		cancel()
		if err == nil {
			w.reporter.SetPendingOutcomeAge(age)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
