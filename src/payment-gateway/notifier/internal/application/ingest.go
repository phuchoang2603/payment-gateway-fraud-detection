package application

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/observe"

	"google.golang.org/protobuf/proto"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/notifier/internal/domain"
	paymentsv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/payments/v1"
)

var ErrMalformed = errors.New("malformed payment event")

type Repository interface {
	SavePaymentRef(ctx context.Context, paymentID, integrationID, orderReference string) error
	OrderReference(ctx context.Context, paymentID string) (string, error)
	InsertDelivery(ctx context.Context, delivery domain.Delivery) (bool, error)
}

type EventStore interface {
	InTx(ctx context.Context, fn func(Repository) error) error
}

type Result int

const (
	ResultIgnored Result = iota
	ResultSkipped
	ResultDuplicate
	ResultRecorded
)

type Ingestor struct {
	Store        EventStore
	Integrations domain.Integrations
	Now          func() time.Time
}

func (i *Ingestor) Handle(ctx context.Context, value []byte) (Result, error) {
	var event paymentsv1.PaymentEvent
	if err := proto.Unmarshal(value, &event); err != nil {
		return ResultIgnored, fmt.Errorf("%w: %w", ErrMalformed, err)
	}
	if event.GetEventId() == "" || event.GetPaymentId() == "" {
		return ResultIgnored, fmt.Errorf("%w: missing event or payment id", ErrMalformed)
	}

	created := event.GetPaymentCreated()
	outcome, notify := outcomeOf(&event)
	integration, known := i.Integrations.Lookup(event.GetIntegrationId())
	deliver := notify && known && integration.Subscribes(outcome.Type)
	if created == nil && !deliver {
		return ResultSkipped, nil
	}

	result := ResultSkipped
	err := i.Store.InTx(ctx, func(repo Repository) error {
		result = ResultSkipped
		if created != nil {
			ref := created.GetSnapshot().GetExternalOrderId()
			if err := repo.SavePaymentRef(ctx, event.GetPaymentId(), event.GetIntegrationId(), ref); err != nil {
				return err
			}
		}
		if !deliver {
			return nil
		}
		orderReference, err := repo.OrderReference(ctx, event.GetPaymentId())
		if err != nil {
			return err
		}
		body, err := domain.BuildBody(domain.Event{ID: event.GetEventId(), PaymentID: event.GetPaymentId(), OccurredAt: event.GetOccurredAt().AsTime()}, outcome, orderReference)
		if err != nil {
			return err
		}
		inserted, err := repo.InsertDelivery(ctx, domain.Delivery{
			EventID:       event.GetEventId(),
			TraceContext:  observe.SerializeContext(ctx),
			IntegrationID: event.GetIntegrationId(),
			PaymentID:     event.GetPaymentId(),
			EventType:     outcome.Type,
			Body:          body,
			NextAttemptAt: i.Now(),
		})
		if err != nil {
			return err
		}
		if inserted {
			result = ResultRecorded
		} else {
			result = ResultDuplicate
		}
		return nil
	})
	if err != nil {
		return ResultIgnored, err
	}
	slog.InfoContext(ctx, "payment event handled", "event_id", event.GetEventId(), "payment_id", event.GetPaymentId(), "request_id", event.GetCorrelationId(), "result", result)
	return result, nil
}
