package application

import (
	"context"
	"errors"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/fraud/internal/domain"
)

// The statuses of one model call.
const (
	ModelAvailable = "available"
	ModelTimeout   = "timeout"
	ModelError     = "error"
)

// ModelObservation is what one model call produced and how it was used: the model, its version and
// score, the threshold, and whether the policy let the score apply and whether it changed the outcome.
type ModelObservation struct {
	Name, Version, Status string
	Score                 *float64
	ReadAt                time.Time
	Threshold             float64
	Enforced, Declined    bool
}

// ModelScorer scores one assessment's features, and returns a status instead of failing.
type ModelScorer interface {
	Score(context.Context, FeatureObservation) ModelObservation
}

// ModelConfig enables model scoring: the scorer, the call's deadline and the decline threshold.
type ModelConfig struct {
	Scorer    ModelScorer
	Timeout   time.Duration
	Threshold float64
}

// RequiredViews are the feature views that must all be available before a score can be enforced. They
// are the views of the feature service, which the Feast adapter's tests check.
var RequiredViews = []string{"customer_daily", "payment_method_daily", "customer_stream", "payment_method_stream", "request_features"}

func complete(features FeatureObservation) bool {
	for _, name := range RequiredViews {
		if features.Views[name].Status != "available" {
			return false
		}
	}
	return true
}

// WithModel enables model scoring after the feature read. The feature read and the model call must fit in the assessment deadline together.
func (s *Service) WithModel(config ModelConfig) error {
	switch {
	case config.Scorer == nil:
		return errors.New("model scoring needs a scorer")
	case config.Timeout <= 0 || s.config.HistoryTimeout+config.Timeout > s.config.Deadline:
		return errors.New("the feature read and the model call must fit in the assessment deadline")
	case config.Threshold <= 0 || config.Threshold > 1:
		return errors.New("the model needs a threshold above 0 and up to 1")
	}
	s.model = config
	return nil
}

// scoreModel calls the scorer and applies the missing-feature policy to the evidence's decision.
func (s *Service) scoreModel(ctx context.Context, evidence *Evidence) {
	call, stop := context.WithTimeout(ctx, s.model.Timeout)
	defer stop()
	observation := s.model.Scorer.Score(call, evidence.Features)
	if observation.Threshold <= 0 || observation.Threshold > 1 {
		observation.Threshold = s.model.Threshold
	}
	input := domain.ModelInput{
		Available: observation.Status == ModelAvailable && observation.Score != nil,
		Complete:  complete(evidence.Features),
		Threshold: observation.Threshold,
	}
	if observation.Score != nil {
		input.Score = *observation.Score
	}
	decision, applied := domain.ApplyModel(evidence.Decision, input)
	observation.Enforced = applied
	observation.Declined = applied && decision.Declined && !evidence.Decision.Declined
	evidence.Decision = decision
	evidence.HistoricalShadowOnly = !applied
	evidence.Model = &observation
}
