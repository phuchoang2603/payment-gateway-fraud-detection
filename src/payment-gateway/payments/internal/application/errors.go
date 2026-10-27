package application

import "errors"

var (
	ErrAssessmentBusy      = errors.New("assessment is owned or awaiting recovery")
	ErrInvalidRequest      = errors.New("invalid request")
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different request")
	ErrOrderConflict       = errors.New("order reference already used by another payment")
	ErrPaymentNotOpen      = errors.New("payment is not open")
	ErrAttemptActive       = errors.New("another attempt is active")
	ErrAccessDenied        = errors.New("checkout token does not authorize this session")
)
