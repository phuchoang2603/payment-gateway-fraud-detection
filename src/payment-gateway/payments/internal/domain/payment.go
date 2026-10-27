package domain

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

const SchemaVersion = 3

type Status string

const (
	StatusNone      Status = ""
	StatusOpen      Status = "open"
	StatusSucceeded Status = "succeeded"
	StatusExpired   Status = "expired"
)

type Phase string

const (
	PhaseStarted      Phase = "started"
	PhaseRiskApproved Phase = "risk_approved"
	PhaseRiskDeclined Phase = "risk_declined"
	PhaseRequested    Phase = "requested"
	PhaseUnknown      Phase = "unknown"
	PhaseDeclined     Phase = "declined"
	PhaseSucceeded    Phase = "succeeded"
)

func (p Phase) Active() bool {
	return p != PhaseDeclined && p != PhaseSucceeded
}

func (p Phase) AwaitingProcessor() bool {
	return p == PhaseRequested || p == PhaseUnknown
}

type Attempt struct {
	AssessmentStatus   string
	ID                 string
	Version            uint64
	Phase              Phase
	DeclineSource      DeclineSource
	DeclineReason      string
	OperationKey       string
	ProcessorReference string
	StartedAt          time.Time
	UpdatedAt          time.Time
}

type State struct {
	PaymentID     string
	IntegrationID string
	CorrelationID string
	SessionID     string
	Status        Status
	Version       uint64
	Merchant      *MerchantRef
	Customer      *CustomerRef
	Snapshot      *CheckoutSnapshot
	ReturnURL     string
	ExpiresAt     time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
	Attempts      []Attempt
}

func (s State) ActiveAttempt() (Attempt, bool) {
	for _, attempt := range s.Attempts {
		if attempt.Phase.Active() {
			return attempt, true
		}
	}
	return Attempt{}, false
}

func (s State) Attempt(id string) (Attempt, bool) {
	for _, attempt := range s.Attempts {
		if attempt.ID == id {
			return attempt, true
		}
	}
	return Attempt{}, false
}

func (s State) clone() State {
	s.Attempts = slices.Clone(s.Attempts)
	return s
}

const (
	RuleFirstEventNotCreated     = "first_event_not_created"
	RulePaymentExists            = "payment_already_created"
	RuleVersionGap               = "version_gap_or_repeat"
	RuleEventAfterTerminal       = "event_after_terminal"
	RuleAttemptWhileActive       = "attempt_while_active"
	RuleDuplicateAttempt         = "duplicate_attempt_id"
	RuleNoActiveAttempt          = "no_active_attempt"
	RuleAttemptMismatch          = "attempt_id_mismatch"
	RuleRiskRequiresStart        = "risk_decision_requires_started_attempt"
	RuleProcessorRequiresApprove = "processor_request_requires_approve"
	RuleUnknownRequiresRequest   = "unknown_requires_processor_request"
	RuleSuccessRequiresRequest   = "success_requires_processor_request"
	RuleRiskDeclineRequiresRisk  = "risk_decline_requires_decline_decision"
	RuleProcessorDeclineNoReq    = "processor_decline_requires_processor_request"
	RuleExpiryWhileUnresolved    = "expiry_while_outcome_unresolved"
	RuleInvalidCommand           = "invalid_command"
)

type RuleError struct {
	Rule   string
	Detail string
}

func (e *RuleError) Error() string {
	if e.Detail == "" {
		return "payment lifecycle rule violated: " + e.Rule
	}
	return fmt.Sprintf("payment lifecycle rule violated: %s: %s", e.Rule, e.Detail)
}

var ErrStaleVersion = errors.New("payment version is stale")

func violation(rule, detail string) error {
	return &RuleError{Rule: rule, Detail: detail}
}

func RuleOf(err error) string {
	var ruleErr *RuleError
	if errors.As(err, &ruleErr) {
		return ruleErr.Rule
	}
	return ""
}

type Envelope struct {
	EventID         string
	Now             time.Time
	ExpectedVersion uint64
}

type Command interface {
	envelope() Envelope
}

type CreatePayment struct {
	Envelope
	PaymentID     string
	IntegrationID string
	CorrelationID string
	SessionID     string
	ReturnURL     string
	ExpiresAt     time.Time
	Merchant      *MerchantRef
	Customer      *CustomerRef
	Snapshot      *CheckoutSnapshot
}

type StartAttempt struct {
	Envelope
	AttemptID string
	Signals   *AttemptSignals
}

type RecordRiskDecision struct {
	Envelope
	AttemptID  string
	Assessment *RiskAssessment
}

type RequestProcessorOperation struct {
	Envelope
	AttemptID    string
	OperationKey string
}

type RecordOutcomeUnknown struct {
	Envelope
	AttemptID string
}

type DeclineAttempt struct {
	Envelope
	AttemptID string
	Source    DeclineSource
	Reason    string
}

type SucceedPayment struct {
	Envelope
	AttemptID          string
	ProcessorReference string
}

type ExpirePayment struct {
	Envelope
}

func (c Envelope) envelope() Envelope { return c }

func Transition(state State, command Command) (State, *Event, error) {
	envelope := command.envelope()
	if envelope.EventID == "" {
		return State{}, nil, violation(RuleInvalidCommand, "event id is empty")
	}
	if _, isCreate := command.(CreatePayment); isCreate {
		if state.Status != StatusNone {
			return State{}, nil, violation(RulePaymentExists, "")
		}
	} else {
		if state.Status == StatusNone {
			return State{}, nil, violation(RuleFirstEventNotCreated, "")
		}
		if state.Status != StatusOpen {
			return State{}, nil, violation(RuleEventAfterTerminal, string(state.Status))
		}
	}
	if envelope.ExpectedVersion != state.Version {
		if _, isCreate := command.(CreatePayment); isCreate {
			return State{}, nil, violation(RuleFirstEventNotCreated, "created event must carry version 1")
		}
		return State{}, nil, fmt.Errorf("%w: expected %d, current %d: %w", ErrStaleVersion, envelope.ExpectedVersion, state.Version, violation(RuleVersionGap, ""))
	}

	next := state.clone()
	next.Version = state.Version + 1
	occurred := envelope.Now.UTC()
	if occurred.Before(state.UpdatedAt) {
		occurred = state.UpdatedAt
	}
	next.UpdatedAt = occurred

	event := &Event{
		ID: envelope.EventID, Version: next.Version, OccurredAt: occurred, RecordedAt: occurred,
		Command: command,
	}

	switch cmd := command.(type) {
	case CreatePayment:
		if err := validateCreate(cmd); err != nil {
			return State{}, nil, err
		}
		next.PaymentID = cmd.PaymentID
		next.IntegrationID = cmd.IntegrationID
		next.CorrelationID = cmd.CorrelationID
		next.SessionID = cmd.SessionID
		next.ReturnURL = cmd.ReturnURL
		next.ExpiresAt = cmd.ExpiresAt.UTC()
		next.CreatedAt = occurred
		next.Status = StatusOpen
		next.Merchant = cmd.Merchant
		next.Customer = cmd.Customer
		next.Snapshot = cmd.Snapshot

	case StartAttempt:
		event.Context = &AttemptContext{Merchant: state.Merchant, Customer: state.Customer, Snapshot: state.Snapshot}
		if cmd.AttemptID == "" {
			return State{}, nil, violation(RuleInvalidCommand, "attempt id is empty")
		}
		if _, active := state.ActiveAttempt(); active {
			return State{}, nil, violation(RuleAttemptWhileActive, "")
		}
		if _, exists := state.Attempt(cmd.AttemptID); exists {
			return State{}, nil, violation(RuleDuplicateAttempt, cmd.AttemptID)
		}
		next.Attempts = append(next.Attempts, Attempt{ID: cmd.AttemptID, Version: 1, Phase: PhaseStarted, StartedAt: occurred, UpdatedAt: occurred})
	case RecordRiskDecision:
		index, err := activeIndex(state, cmd.AttemptID)
		if err != nil {
			return State{}, nil, err
		}
		if state.Attempts[index].Phase != PhaseStarted {
			return State{}, nil, violation(RuleRiskRequiresStart, string(state.Attempts[index].Phase))
		}
		if err := validateAssessment(cmd.Assessment); err != nil {
			return State{}, nil, err
		}
		phase := PhaseRiskApproved
		if cmd.Assessment.Outcome == RiskOutcomeDecline {
			phase = PhaseRiskDeclined
		}
		advance(&next.Attempts[index], phase, occurred)
		if phase == PhaseRiskDeclined {
			next.Attempts[index].DeclineReason = "risk_declined"
			if reasons := cmd.Assessment.ReasonCodes; len(reasons) > 0 {
				next.Attempts[index].DeclineReason = strings.ToLower(reasons[0])
			}
		}
	case RequestProcessorOperation:
		index, err := activeIndex(state, cmd.AttemptID)
		if err != nil {
			return State{}, nil, err
		}
		if state.Attempts[index].Phase != PhaseRiskApproved {
			return State{}, nil, violation(RuleProcessorRequiresApprove, string(state.Attempts[index].Phase))
		}
		if cmd.OperationKey == "" {
			return State{}, nil, violation(RuleInvalidCommand, "operation key is empty")
		}
		advance(&next.Attempts[index], PhaseRequested, occurred)
		next.Attempts[index].OperationKey = cmd.OperationKey
	case RecordOutcomeUnknown:
		index, err := activeIndex(state, cmd.AttemptID)
		if err != nil {
			return State{}, nil, err
		}
		if state.Attempts[index].Phase != PhaseRequested {
			return State{}, nil, violation(RuleUnknownRequiresRequest, string(state.Attempts[index].Phase))
		}
		advance(&next.Attempts[index], PhaseUnknown, occurred)
	case DeclineAttempt:
		index, err := activeIndex(state, cmd.AttemptID)
		if err != nil {
			return State{}, nil, err
		}
		if cmd.Reason == "" {
			return State{}, nil, violation(RuleInvalidCommand, "decline reason is empty")
		}
		phase := state.Attempts[index].Phase
		switch cmd.Source {
		case DeclineSourceRisk:
			if phase != PhaseRiskDeclined {
				return State{}, nil, violation(RuleRiskDeclineRequiresRisk, string(phase))
			}
		case DeclineSourceProcessor:
			if !phase.AwaitingProcessor() {
				return State{}, nil, violation(RuleProcessorDeclineNoReq, string(phase))
			}
		default:
			return State{}, nil, violation(RuleInvalidCommand, "decline source is unspecified")
		}
		advance(&next.Attempts[index], PhaseDeclined, occurred)
		next.Attempts[index].DeclineSource = cmd.Source
		next.Attempts[index].DeclineReason = cmd.Reason
	case SucceedPayment:
		index, err := activeIndex(state, cmd.AttemptID)
		if err != nil {
			return State{}, nil, err
		}
		if !state.Attempts[index].Phase.AwaitingProcessor() {
			return State{}, nil, violation(RuleSuccessRequiresRequest, string(state.Attempts[index].Phase))
		}
		if cmd.ProcessorReference == "" {
			return State{}, nil, violation(RuleInvalidCommand, "processor reference is empty")
		}
		advance(&next.Attempts[index], PhaseSucceeded, occurred)
		next.Attempts[index].ProcessorReference = cmd.ProcessorReference
		next.Status = StatusSucceeded
	case ExpirePayment:
		if active, ok := state.ActiveAttempt(); ok && active.Phase.AwaitingProcessor() {
			return State{}, nil, violation(RuleExpiryWhileUnresolved, active.ID)
		}
		next.Status = StatusExpired
	default:
		return State{}, nil, violation(RuleInvalidCommand, fmt.Sprintf("unknown command %T", command))
	}

	event.PaymentID = next.PaymentID
	event.IntegrationID = next.IntegrationID
	event.CorrelationID = next.CorrelationID
	return next, event, nil
}

func advance(attempt *Attempt, phase Phase, at time.Time) {
	attempt.Phase = phase
	attempt.Version++
	attempt.UpdatedAt = at
}

func activeIndex(state State, attemptID string) (int, error) {
	active, ok := state.ActiveAttempt()
	if !ok {
		return 0, violation(RuleNoActiveAttempt, attemptID)
	}
	if active.ID != attemptID {
		return 0, violation(RuleAttemptMismatch, attemptID)
	}
	return slices.IndexFunc(state.Attempts, func(a Attempt) bool { return a.ID == attemptID }), nil
}

func validateCreate(cmd CreatePayment) error {
	switch {
	case cmd.PaymentID == "", cmd.IntegrationID == "", cmd.CorrelationID == "", cmd.SessionID == "":
		return violation(RuleInvalidCommand, "identities must not be empty")
	case cmd.Merchant == nil, cmd.Customer == nil, cmd.Snapshot == nil:
		return violation(RuleInvalidCommand, "merchant, customer and snapshot are required")
	}
	return nil
}

func validateAssessment(assessment *RiskAssessment) error {
	switch {
	case assessment == nil:
		return violation(RuleInvalidCommand, "assessment is missing")
	case assessment.Outcome != RiskOutcomeApprove && assessment.Outcome != RiskOutcomeDecline:
		return violation(RuleInvalidCommand, "assessment outcome is unspecified")
	case assessment.PolicyVersion == "":
		return violation(RuleInvalidCommand, "assessment policy version is empty")
	case slices.Contains(assessment.ReasonCodes, ""):
		return violation(RuleInvalidCommand, "assessment reason codes are empty")
	}
	return nil
}
