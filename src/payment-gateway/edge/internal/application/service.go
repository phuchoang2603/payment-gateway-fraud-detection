package application

import (
	"context"
	"errors"
	"net/netip"
	"net/url"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/domain"
)

type Config struct {
	Payments                Payments
	Vault                   Vault
	Integrations            Integrations
	FingerprintKey          string
	SyntheticFingerprintKey string
	PublicBaseURL           string
	Now                     func() time.Time
}

type Service struct {
	payments     Payments
	vault        Vault
	integrations Integrations
	real         domain.Fingerprinter
	synthetic    domain.Fingerprinter
	publicBase   string
	now          func() time.Time
	attempts     *attemptCache
}

func New(cfg Config) *Service {
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Service{
		payments:     cfg.Payments,
		vault:        cfg.Vault,
		integrations: cfg.Integrations,
		real:         domain.NewFingerprinter(cfg.FingerprintKey),
		synthetic:    domain.NewFingerprinter(cfg.SyntheticFingerprintKey),
		publicBase:   cfg.PublicBaseURL,
		now:          now,
		attempts:     newAttemptCache(now),
	}
}

func (s *Service) Authenticate(apiKey string) (domain.Integration, error) {
	integration, ok := s.integrations.Authenticate(apiKey)
	if !ok {
		return domain.Integration{}, domain.New(domain.ErrUnauthorized, "unknown api key")
	}
	return integration, nil
}

func (s *Service) fingerprinter(integration domain.Integration) domain.Fingerprinter {
	if integration.Synthetic {
		return s.synthetic
	}
	return s.real
}

func (s *Service) CheckoutURL(p Payment) string {
	return s.publicBase + "/checkout/" + url.PathEscape(p.SessionID) + "?token=" + url.QueryEscape(p.CheckoutToken)
}

func (s *Service) CreatePayment(ctx context.Context, integration domain.Integration, key string, input domain.CheckoutInput) (Payment, error) {
	if err := input.Validate(); err != nil {
		return Payment{}, err
	}
	return s.payments.CreatePayment(ctx, CreatePaymentCommand{
		IntegrationID:    integration.ID,
		IdempotencyKey:   key,
		SellerExternalID: input.Seller.ExternalID,
		BuyerExternalID:  input.Buyer.ExternalID,
		Snapshot:         input.Snapshot(s.fingerprinter(integration)),
		ReturnURL:        input.ReturnURL,
	})
}

func (s *Service) GetPayment(ctx context.Context, integration domain.Integration, paymentID string) (Payment, error) {
	return s.payments.GetPayment(ctx, integration.ID, paymentID)
}

func (s *Service) TokenizeTestCard(ctx context.Context, integration domain.Integration, cardNumber string) (Token, error) {
	if !integration.Synthetic {
		return Token{}, domain.New(domain.ErrForbidden, "payment methods are limited to synthetic integrations")
	}
	number, ok := domain.NormalizeCardNumber(cardNumber)
	if !ok {
		return Token{}, domain.ErrInvalidCard
	}
	token, err := s.vault.Tokenize(ctx, integration.ID, number)
	if err != nil {
		return Token{}, err
	}
	return token, nil
}

type AttemptRequest struct {
	SessionID          string
	CheckoutToken      string
	IdempotencyKey     string
	PaymentMethodToken string
	DeviceFingerprint  string
	UserAgentFamily    string
	Simulated          *SimulatedClient
}

func (s *Service) StartAttempt(ctx context.Context, req AttemptRequest) (Attempt, error) {
	payment, err := s.payments.GetCheckoutSession(ctx, req.SessionID, req.CheckoutToken)
	if err != nil {
		return Attempt{}, err
	}
	integration, ok := s.integrations.ByID(payment.IntegrationID)
	if !ok {
		return Attempt{}, domain.New(domain.ErrNotFound, "integration is not configured")
	}
	if req.Simulated != nil && !integration.Synthetic {
		return Attempt{}, domain.New(domain.ErrForbidden, "simulated client context is limited to synthetic integrations")
	}
	method, err := s.vault.Describe(ctx, integration.ID, req.PaymentMethodToken)
	if err != nil {
		return Attempt{}, err
	}
	signals := Signals{
		AttemptedAt:       s.now(),
		MethodType:        method.Type,
		MethodFingerprint: method.Fingerprint,
		DeviceFingerprint: req.DeviceFingerprint,
		UserAgentFamily:   req.UserAgentFamily,
	}
	if req.Simulated != nil {
		signals.IPCountry = req.Simulated.IPCountry
		signals.IPNetworkFingerprint = s.networkFingerprint(integration, req.Simulated.IPNetwork)
		if attempted := req.Simulated.AttemptedAt; !attempted.IsZero() {
			if attempted.After(signals.AttemptedAt) {
				return Attempt{}, domain.Invalid("simulated_client.attempted_at must not be after server time")
			}
			signals.AttemptedAt = attempted
		}
	}
	return s.payments.StartAttempt(ctx, StartAttemptCommand{
		IntegrationID:      integration.ID,
		SessionID:          req.SessionID,
		CheckoutToken:      req.CheckoutToken,
		IdempotencyKey:     req.IdempotencyKey,
		PaymentMethodToken: req.PaymentMethodToken,
		Signals:            signals,
	})
}

func (s *Service) networkFingerprint(integration domain.Integration, ip netip.Addr) string {
	if !ip.IsValid() {
		return ""
	}
	fingerprint, _ := s.fingerprinter(integration).Network(ip)
	return fingerprint
}

type CheckoutState string

const (
	StateOpen      CheckoutState = "open"
	StatePending   CheckoutState = "pending"
	StateSucceeded CheckoutState = "succeeded"
	StateExpired   CheckoutState = "expired"
)

type CheckoutView struct {
	Payment Payment
	State   CheckoutState
}

func (s *Service) Checkout(ctx context.Context, sessionID, checkoutToken string) (CheckoutView, error) {
	payment, err := s.payments.GetCheckoutSession(ctx, sessionID, checkoutToken)
	if errors.Is(err, domain.ErrConflict) {
		return CheckoutView{}, domain.New(domain.ErrGone, "checkout session is closed")
	}
	if err != nil {
		return CheckoutView{}, err
	}
	return CheckoutView{Payment: payment, State: s.stateOf(payment)}, nil
}

func (s *Service) stateOf(payment Payment) CheckoutState {
	switch payment.Status {
	case PaymentSucceeded:
		return StateSucceeded
	case PaymentExpired:
		return StateExpired
	}
	for _, attempt := range payment.Attempts {
		if attempt.Status == AttemptPending {
			return StatePending
		}
	}
	if !s.now().Before(payment.ExpiresAt) {
		return StateExpired
	}
	return StateOpen
}
