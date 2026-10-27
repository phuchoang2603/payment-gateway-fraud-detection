package domain

import "time"

type Money struct {
	MinorUnits int64
	Currency   string
}

type MerchantRef struct {
	MerchantID       string
	ExternalSellerID string
}

type CustomerRef struct {
	CustomerID      string
	ExternalBuyerID string
}

type LineItem struct {
	ProductID string
	Category  string
	Condition string
	Quantity  int32
	UnitPrice *Money
}

type ShippingLocation struct {
	Country            string
	Region             string
	PostalCode         string
	AddressFingerprint string
}

type CheckoutSnapshot struct {
	ExternalOrderID          string
	Total                    *Money
	Items                    []*LineItem
	MerchantAccountCreatedAt time.Time
	CustomerAccountCreatedAt time.Time
	Shipping                 *ShippingLocation
}

type PaymentMethod struct {
	Type        string
	Fingerprint string
}

type AttemptSignals struct {
	AttemptedAt          time.Time
	PaymentMethod        *PaymentMethod
	DeviceFingerprint    *string
	IPCountry            *string
	IPNetworkFingerprint *string
	UserAgentFamily      *string
}

type RiskOutcome int32

const (
	RiskOutcomeApprove RiskOutcome = 1
	RiskOutcomeDecline RiskOutcome = 2
)

type RiskAssessment struct {
	AssessmentID      string
	EvidenceSHA256    string
	Outcome           RiskOutcome
	ReasonCodes       []string
	PolicyVersion     string
	ModelVersion      *string
	FeatureSetVersion *string
	RiskScore         *float64
}

type DeclineSource int32

const (
	DeclineSourceRisk      DeclineSource = 1
	DeclineSourceProcessor DeclineSource = 2
)

type AttemptContext struct {
	Merchant *MerchantRef
	Customer *CustomerRef
	Snapshot *CheckoutSnapshot
}

type Event struct {
	Context       *AttemptContext
	ID            string
	PaymentID     string
	IntegrationID string
	CorrelationID string
	Version       uint64
	OccurredAt    time.Time
	RecordedAt    time.Time
	Command       Command
}
