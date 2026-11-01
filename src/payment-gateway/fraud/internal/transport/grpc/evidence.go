package grpctransport

import (
	"strings"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/fraud/internal/application"
	fraudv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/fraud/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func optionalTime(value *time.Time) *timestamppb.Timestamp {
	if value == nil {
		return nil
	}
	return timestamppb.New(*value)
}

func viewProto(name string, value application.ViewObservation) *fraudv1.FeatureViewEvidence {
	status := fraudv1.FeatureViewStatus_FEATURE_VIEW_STATUS_ERROR
	if code, ok := fraudv1.FeatureViewStatus_value["FEATURE_VIEW_STATUS_"+strings.ToUpper(value.Status)]; ok {
		status = fraudv1.FeatureViewStatus(code)
	}
	result := &fraudv1.FeatureViewEvidence{View: name, Status: status, EventTimestamp: optionalTime(value.EventTimestamp), Values: map[string]*fraudv1.FeatureValue{}}
	if !value.ReadAt.IsZero() {
		result.ReadAt = timestamppb.New(value.ReadAt)
	}
	for field, quantity := range value.Values {
		feature := &fraudv1.FeatureValue{}
		if quantity.Integer != nil {
			feature.Value = &fraudv1.FeatureValue_Integer{Integer: *quantity.Integer}
		}
		if quantity.Number != nil {
			feature.Value = &fraudv1.FeatureValue_Number{Number: *quantity.Number}
		}
		if quantity.Boolean != nil {
			feature.Value = &fraudv1.FeatureValue_Boolean{Boolean: *quantity.Boolean}
		}
		result.Values[field] = feature
	}
	return result
}

func evidenceProto(value application.Evidence) *fraudv1.AssessmentEvidence {
	result := &fraudv1.AssessmentEvidence{
		SchemaVersion: uint32(value.SchemaVersion), AssessmentId: value.AssessmentID, RequestSha256: value.RequestDigest, IntegrationId: value.IntegrationID, PaymentId: value.PaymentID, AttemptId: value.AttemptID, PolicyVersion: value.Decision.PolicyVersion,
		FeatureService:  value.Features.Service,
		EvaluatedAt:     timestamppb.New(value.EvaluatedAt),
		UsedAmountMinor: value.Input.AmountMinor, UsedCustomerCreatedAt: timestamppb.New(value.Input.CustomerCreatedAt), UsedAttemptedAt: timestamppb.New(value.Input.AttemptedAt), UsedShippingCountry: value.Input.ShippingCountry, UsedIpCountry: value.Input.IPCountry, HistoricalFeaturesShadowOnly: value.HistoricalShadowOnly,
	}
	for _, name := range []string{"customer_daily", "payment_method_daily", "customer_stream", "payment_method_stream", "request_features"} {
		if view, exists := value.Features.Views[name]; exists {
			result.Views = append(result.Views, viewProto(name, view))
		}
	}
	return result
}
