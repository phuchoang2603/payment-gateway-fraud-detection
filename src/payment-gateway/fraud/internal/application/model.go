package application

import (
	"context"
	"errors"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/fraud/internal/domain"
)

type ModelMode string

const (
	ModelShadow  ModelMode = "shadow"
	ModelEnforce ModelMode = "enforce"
)

const (
	ModelAvailable = "available"
	ModelTimeout   = "timeout"
	ModelError     = "error"
)

type ModelObservation struct {
	Name, Version, Mode, Status string
	Score                       *float64
	ReadAt                      time.Time
	Threshold                   float64
	Enforced, Declined          bool
}

type ModelScorer interface {
	Score(context.Context, FeatureObservation) ModelObservation
}

type ModelConfig struct {
	Scorer    ModelScorer
	Mode      ModelMode
	Timeout   time.Duration
	Threshold float64
}

var requiredViews = []string{"customer_daily", "payment_method_daily", "customer_stream", "payment_method_stream", "request_features"}

func complete(features FeatureObservation) bool {
	for _, name := range requiredViews {
		if features.Views[name].Status != "available" {
			return false
		}
	}
	return true
}

// WithModel enables model scoring after the feature read. The feature read and the model call must fit in the assessment deadline together.
func (s *Service) WithModel(config ModelConfig) error {
	switch {
	case config.Scorer == nil || (config.Mode != ModelShadow && config.Mode != ModelEnforce):
		return errors.New("model scoring needs a scorer and the shadow or enforce mode")
	case config.Timeout <= 0 || s.config.HistoryTimeout+config.Timeout > s.config.Deadline:
		return errors.New("the feature read and the model call must fit in the assessment deadline")
	case config.Mode == ModelEnforce && (config.Threshold <= 0 || config.Threshold > 1):
		return errors.New("an enforced model needs a threshold above 0 and up to 1")
	}
	s.model = config
	return nil
}

func (s *Service) scoreModel(ctx context.Context, evidence *Evidence) {
	call, stop := context.WithTimeout(ctx, s.model.Timeout)
	defer stop()
	observation := s.model.Scorer.Score(call, evidence.Features)
	observation.Mode, observation.Threshold = string(s.model.Mode), s.model.Threshold
	input := domain.ModelInput{
		Enforce:   s.model.Mode == ModelEnforce,
		Available: observation.Status == ModelAvailable && observation.Score != nil,
		Complete:  complete(evidence.Features),
		Threshold: s.model.Threshold,
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
