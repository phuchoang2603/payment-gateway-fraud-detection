package domain

import "time"

const (
	DeliveryPending   = "pending"
	DeliveryDelivered = "delivered"
	DeliveryFailed    = "failed"
)

type Delivery struct {
	TraceContext  *string
	EventID       string
	IntegrationID string
	PaymentID     string
	EventType     string
	Body          []byte
	Attempts      int
	NextAttemptAt time.Time
	CreatedAt     time.Time
}

type Attempt struct {
	Status        string
	NextAttemptAt time.Time
	LastStatus    *int
	LastError     string
	DeliveredAt   *time.Time
}

type Stats struct {
	Pending       int64
	OldestPending time.Duration
}
