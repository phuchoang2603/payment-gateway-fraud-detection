package wire

import (
	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/domain"
	paymentsv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/payments/v1"
)

func Event(value *domain.Event) *paymentsv1.PaymentEvent {
	event := &paymentsv1.PaymentEvent{EventId: value.ID, PaymentId: value.PaymentID, IntegrationId: value.IntegrationID, CorrelationId: value.CorrelationID, AggregateVersion: value.Version, OccurredAt: timestamp(value.OccurredAt), RecordedAt: timestamp(value.RecordedAt), SchemaVersion: domain.SchemaVersion}
	switch cmd := value.Command.(type) {
	case domain.CreatePayment:
		event.Payload = &paymentsv1.PaymentEvent_PaymentCreated{PaymentCreated: &paymentsv1.PaymentCreated{Merchant: MerchantProto(cmd.Merchant), Customer: CustomerProto(cmd.Customer), Snapshot: SnapshotProto(cmd.Snapshot)}}
	case domain.StartAttempt:
		event.Payload = &paymentsv1.PaymentEvent_AttemptStarted{AttemptStarted: &paymentsv1.AttemptStarted{AttemptId: cmd.AttemptID, Signals: signalsProto(cmd.Signals), Context: &paymentsv1.AttemptContext{Merchant: MerchantProto(value.Context.Merchant), Customer: CustomerProto(value.Context.Customer), Snapshot: SnapshotProto(value.Context.Snapshot)}}}
	case domain.RecordRiskDecision:
		event.Payload = &paymentsv1.PaymentEvent_RiskDecisionRecorded{RiskDecisionRecorded: &paymentsv1.RiskDecisionRecorded{AttemptId: cmd.AttemptID, Assessment: assessmentProto(cmd.Assessment)}}
	case domain.RequestProcessorOperation:
		event.Payload = &paymentsv1.PaymentEvent_ProcessorOperationRequested{ProcessorOperationRequested: &paymentsv1.ProcessorOperationRequested{AttemptId: cmd.AttemptID, OperationKey: cmd.OperationKey}}
	case domain.RecordOutcomeUnknown:
		event.Payload = &paymentsv1.PaymentEvent_ProcessorOutcomeUnknown{ProcessorOutcomeUnknown: &paymentsv1.ProcessorOutcomeUnknown{AttemptId: cmd.AttemptID}}
	case domain.DeclineAttempt:
		event.Payload = &paymentsv1.PaymentEvent_AttemptDeclined{AttemptDeclined: &paymentsv1.AttemptDeclined{AttemptId: cmd.AttemptID, Source: paymentsv1.DeclineSource(cmd.Source), Reason: cmd.Reason}}
	case domain.SucceedPayment:
		event.Payload = &paymentsv1.PaymentEvent_PaymentSucceeded{PaymentSucceeded: &paymentsv1.PaymentSucceeded{AttemptId: cmd.AttemptID, ProcessorReference: cmd.ProcessorReference}}
	case domain.ExpirePayment:
		event.Payload = &paymentsv1.PaymentEvent_PaymentExpired{PaymentExpired: &paymentsv1.PaymentExpired{Present: true}}
	}
	return event
}
