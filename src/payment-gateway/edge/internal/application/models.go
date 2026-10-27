package application

import (
	"context"
	"net/netip"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/domain"
)

const (
	PaymentOpen      = "OPEN"
	PaymentSucceeded = "SUCCEEDED"
	PaymentExpired   = "EXPIRED"

	AttemptPending   = "PENDING"
	AttemptSucceeded = "SUCCEEDED"
	AttemptDeclined  = "DECLINED"
)

type Attempt struct {
	AssessmentStatus string
	ID               string
	Status           string
	Reason           string
}

type Payment struct {
	ID               string
	SessionID        string
	CheckoutToken    string
	IntegrationID    string
	ExpiresAt        time.Time
	Status           string
	MerchantID       string
	CustomerID       string
	ExternalSellerID string
	OrderReference   string
	Total            domain.Money
	Items            []domain.Item
	ReturnURL        string
	Attempts         []Attempt
}

type CreatePaymentCommand struct {
	IntegrationID    string
	IdempotencyKey   string
	SellerExternalID string
	BuyerExternalID  string
	Snapshot         domain.Snapshot
	ReturnURL        string
}

// Empty strings mean the signal was not observed and stay absent downstream.
type Signals struct {
	AttemptedAt          time.Time
	MethodType           string
	MethodFingerprint    string
	DeviceFingerprint    string
	IPCountry            string
	IPNetworkFingerprint string
	UserAgentFamily      string
}

type StartAttemptCommand struct {
	IntegrationID      string
	SessionID          string
	CheckoutToken      string
	IdempotencyKey     string
	PaymentMethodToken string
	Signals            Signals
}

type PaymentMethod struct {
	Type        string
	Fingerprint string
}

type Token struct {
	Value  string
	Method PaymentMethod
}

type Payments interface {
	CreatePayment(ctx context.Context, cmd CreatePaymentCommand) (Payment, error)
	GetPayment(ctx context.Context, integrationID, paymentID string) (Payment, error)
	GetCheckoutSession(ctx context.Context, sessionID, checkoutToken string) (Payment, error)
	StartAttempt(ctx context.Context, cmd StartAttemptCommand) (Attempt, error)
}

type Vault interface {
	Tokenize(ctx context.Context, integrationID, cardNumber string) (Token, error)
	Describe(ctx context.Context, integrationID, token string) (PaymentMethod, error)
}

type Integrations interface {
	Authenticate(apiKey string) (domain.Integration, bool)
	ByID(id string) (domain.Integration, bool)
}

type SimulatedClient struct {
	IPCountry string
	IPNetwork netip.Addr
}
