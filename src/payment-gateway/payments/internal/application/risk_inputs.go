package application

import (
	"context"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/domain"
)

type AttemptRiskInputsQuery struct {
	IntegrationID string
	PaymentID     string
	AttemptID     string
}

type AttemptRiskInputs struct {
	PaymentID                string
	AttemptID                string
	CustomerID               string
	PaymentMethodFingerprint string
	AmountMinor              int64
	Currency                 string
	ItemCount                int
	ShippingCountry          string
	IPCountry                string
	HasDevice                bool
	CustomerAccountCreatedAt time.Time
	MerchantAccountCreatedAt time.Time
	DeclineSource            string
	DeclineReason            string
}

func (s *Service) AttemptRiskInputs(ctx context.Context, query AttemptRiskInputsQuery) (AttemptRiskInputs, error) {
	integrationID, err := parseUUID("integration_id", query.IntegrationID)
	if err != nil {
		return AttemptRiskInputs{}, err
	}
	paymentID, err := parseUUID("payment_id", query.PaymentID)
	if err != nil {
		return AttemptRiskInputs{}, err
	}
	state, err := s.deps.Store.LoadPayment(ctx, paymentID)
	if err != nil {
		return AttemptRiskInputs{}, err
	}
	if state.IntegrationID != integrationID.String() {
		return AttemptRiskInputs{}, ErrNotFound
	}
	attempt, err := selectAttempt(state, query.AttemptID)
	if err != nil {
		return AttemptRiskInputs{}, err
	}
	attemptID, err := parseUUID("attempt_id", attempt.ID)
	if err != nil {
		return AttemptRiskInputs{}, err
	}
	signals, err := s.deps.Store.AttemptSignals(ctx, paymentID, attemptID)
	if err != nil {
		return AttemptRiskInputs{}, err
	}
	snapshot := state.Snapshot
	inputs := AttemptRiskInputs{
		PaymentID:                state.PaymentID,
		AttemptID:                attempt.ID,
		CustomerID:               state.Customer.CustomerID,
		PaymentMethodFingerprint: signals.GetPaymentMethod().GetFingerprint(),
		AmountMinor:              snapshot.Total.MinorUnits,
		Currency:                 snapshot.Total.Currency,
		ItemCount:                len(snapshot.Items),
		ShippingCountry:          snapshot.Shipping.Country,
		IPCountry:                signals.GetIpCountry(),
		HasDevice:                signals != nil && signals.DeviceFingerprint != nil,
		CustomerAccountCreatedAt: snapshot.CustomerAccountCreatedAt,
		MerchantAccountCreatedAt: snapshot.MerchantAccountCreatedAt,
		DeclineReason:            attempt.DeclineReason,
	}
	switch attempt.DeclineSource {
	case domain.DeclineSourceRisk:
		inputs.DeclineSource = "risk"
	case domain.DeclineSourceProcessor:
		inputs.DeclineSource = "processor"
	}
	return inputs, nil
}

func selectAttempt(state domain.State, attemptID string) (domain.Attempt, error) {
	if attemptID == "" {
		if len(state.Attempts) == 0 {
			return domain.Attempt{}, ErrNotFound
		}
		return state.Attempts[len(state.Attempts)-1], nil
	}
	id, err := parseUUID("attempt_id", attemptID)
	if err != nil {
		return domain.Attempt{}, err
	}
	attempt, ok := state.Attempt(id.String())
	if !ok {
		return domain.Attempt{}, ErrNotFound
	}
	return attempt, nil
}
