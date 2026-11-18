package application

import (
	"context"
	"errors"
	"net/netip"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/domain"
)

type PayRequest struct {
	SessionID         string
	CheckoutToken     string
	IdempotencyKey    string
	CardNumber        string
	DeviceFingerprint string
	UserAgent         string
	ClientIP          netip.Addr
}

type PayResult struct {
	Payment Payment
	Attempt Attempt
}

func (s *Service) Pay(ctx context.Context, req PayRequest) (PayResult, error) {
	number, ok := domain.NormalizeCardNumber(req.CardNumber)
	if !ok {
		return PayResult{}, domain.ErrInvalidCard
	}
	return s.attempts.do(req.SessionID+"\x00"+req.IdempotencyKey, func() (PayResult, error) {
		return s.pay(ctx, req, number)
	})
}

func (s *Service) pay(ctx context.Context, req PayRequest, number string) (PayResult, error) {
	view, err := s.Checkout(ctx, req.SessionID, req.CheckoutToken)
	if err != nil {
		return PayResult{}, err
	}
	if view.State == StateSucceeded || view.State == StateExpired {
		return PayResult{}, domain.New(domain.ErrGone, "checkout session is closed")
	}
	integration, ok := s.integrations.ByID(view.Payment.IntegrationID)
	if !ok {
		return PayResult{}, domain.New(domain.ErrNotFound, "integration is not configured")
	}
	token, err := s.vault.Tokenize(ctx, integration.ID, number)
	if errors.Is(err, domain.ErrInvalid) {
		return PayResult{}, domain.ErrInvalidCard
	}
	if err != nil {
		return PayResult{}, err
	}
	signals := Signals{
		AttemptedAt:          s.now(),
		MethodType:           token.Method.Type,
		MethodFingerprint:    token.Method.Fingerprint,
		DeviceFingerprint:    req.DeviceFingerprint,
		UserAgentFamily:      domain.UserAgentFamily(req.UserAgent),
		IPNetworkFingerprint: s.networkFingerprint(integration, req.ClientIP),
	}
	attempt, err := s.payments.StartAttempt(ctx, StartAttemptCommand{
		IntegrationID:      integration.ID,
		SessionID:          req.SessionID,
		CheckoutToken:      req.CheckoutToken,
		IdempotencyKey:     req.IdempotencyKey,
		PaymentMethodToken: token.Value,
		Signals:            signals,
	})
	if errors.Is(err, domain.ErrConflict) {
		return s.latestAttempt(ctx, req)
	}
	if err != nil {
		return PayResult{}, err
	}
	return PayResult{Payment: view.Payment, Attempt: attempt}, nil
}

func (s *Service) latestAttempt(ctx context.Context, req PayRequest) (PayResult, error) {
	view, err := s.Checkout(ctx, req.SessionID, req.CheckoutToken)
	if err != nil {
		return PayResult{}, err
	}
	attempts := view.Payment.Attempts
	if len(attempts) == 0 {
		return PayResult{}, domain.New(domain.ErrConflict, "attempt conflicts with an earlier request")
	}
	return PayResult{Payment: view.Payment, Attempt: attempts[len(attempts)-1]}, nil
}
