package grpctransport

import (
	"strings"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/fraud/internal/application"
	featuresv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/features/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func optionalTime(value *time.Time) *timestamppb.Timestamp {
	if value == nil {
		return nil
	}
	return timestamppb.New(*value)
}

func snapshotValues(value application.SnapshotValues) *featuresv1.SnapshotValues {
	return &featuresv1.SnapshotValues{AssessedCount_1H: value.Count1h, AssessedCount_24H: value.Count24h, AssessedAmountMinor_24H: value.AmountMinor24h, DistinctDevices_7D: value.Devices7d, LastAssessedStartAt: optionalTime(value.LastAssessedAt)}
}

func viewProto(scope featuresv1.EntityView, entity, integration string, value application.ViewObservation) *featuresv1.ViewEvidence {
	status := featuresv1.ViewStatus_VIEW_STATUS_ERROR
	if code, ok := featuresv1.ViewStatus_value["VIEW_STATUS_"+strings.ToUpper(value.Status)]; ok {
		status = featuresv1.ViewStatus(code)
	}
	result := &featuresv1.ViewEvidence{View: scope, Status: status, ReadAt: timestamppb.New(value.ReadAt), OnlineAvailableAt: optionalTime(value.AvailableAt), AvailableValues: snapshotValues(value.Values), SecondsSinceLastAssessed_7D: value.ElapsedSeconds}
	if value.ReadAt.IsZero() {
		result.ReadAt = nil
	}
	if value.Observed != nil {
		observed := value.Observed
		coverage := func(window string) featuresv1.Coverage {
			if observed.Coverage[window] == "complete" {
				return featuresv1.Coverage_COVERAGE_COMPLETE
			}
			return featuresv1.Coverage_COVERAGE_UNKNOWN
		}
		result.Observed = &featuresv1.SnapshotUpdate{
			SchemaVersion: 1, Definition: &featuresv1.DefinitionIdentity{Version: observed.Definition, Sha256: observed.DefinitionHash}, Epoch: observed.Epoch,
			View: scope, IntegrationId: integration, EntityId: entity, SnapshotId: observed.ID, Revision: observed.Revision,
			Anchor: timestamppb.New(observed.Anchor), ComputedAt: timestamppb.New(observed.ComputedAt), PublishedAt: timestamppb.New(observed.PublishedAt), SourceAvailableAt: timestamppb.New(observed.SourceAvailableAt), OnlineWriteAt: timestamppb.New(observed.WriteAt),
			Coverage: &featuresv1.WindowCoverage{OneHour: coverage("1h"), TwentyFourHours: coverage("24h"), SevenDays: coverage("7d")}, Values: snapshotValues(observed.Values),
		}
		for _, progress := range observed.SourceProgress {
			result.Observed.SourceProgress = append(result.Observed.SourceProgress, &featuresv1.SourcePartitionProgress{Topic: progress.Topic, Partition: progress.Partition, ConsumedOffset: progress.ConsumedOffset, ObservedHighWatermark: progress.ObservedHighWatermark, ObservedAt: timestamppb.New(progress.ObservedAt), HeartbeatAt: optionalTime(progress.HeartbeatAt)})
		}
	}
	return result
}

func guardProto(scope featuresv1.EntityView, value application.GuardObservation) *featuresv1.GuardEvidence {
	status := featuresv1.GuardStatus_GUARD_STATUS_ERROR
	if code, ok := featuresv1.GuardStatus_value["GUARD_STATUS_"+strings.ToUpper(value.Status)]; ok {
		status = featuresv1.GuardStatus(code)
	}
	return &featuresv1.GuardEvidence{View: scope, Status: status, RecoveryEpoch: value.Epoch, RegisteredAt: optionalTime(value.RegisteredAt), ReadAt: optionalTime(value.ReadAt), SubmissionCount_10S: value.Count10s, SubmissionCount_60S: value.Count60s, ThresholdExceeded: value.ThresholdExceeded, PolicyVersion: value.PolicyVersion, ObserveOnly: true, Threshold_10S: value.Threshold10s, Threshold_60S: value.Threshold60s}
}

func evidenceProto(value application.Evidence) *featuresv1.AssessmentEvidence {
	return &featuresv1.AssessmentEvidence{
		SchemaVersion: uint32(value.SchemaVersion), AssessmentId: value.AssessmentID, RequestSha256: value.RequestDigest, IntegrationId: value.IntegrationID, PaymentId: value.PaymentID, AttemptId: value.AttemptID, PolicyVersion: value.Decision.PolicyVersion,
		Customer: viewProto(featuresv1.EntityView_ENTITY_VIEW_CUSTOMER, value.CustomerID, value.IntegrationID, value.Customer), Method: viewProto(featuresv1.EntityView_ENTITY_VIEW_METHOD, value.MethodFingerprint, value.IntegrationID, value.Method),
		CustomerGuard: guardProto(featuresv1.EntityView_ENTITY_VIEW_CUSTOMER, value.CustomerGuard), MethodGuard: guardProto(featuresv1.EntityView_ENTITY_VIEW_METHOD, value.MethodGuard), EvaluatedAt: timestamppb.New(value.EvaluatedAt),
		UsedAmountMinor: value.Input.AmountMinor, UsedCustomerCreatedAt: timestamppb.New(value.Input.CustomerCreatedAt), UsedAttemptedAt: timestamppb.New(value.Input.AttemptedAt), UsedShippingCountry: value.Input.ShippingCountry, UsedIpCountry: value.Input.IPCountry, HistoricalFeaturesShadowOnly: value.HistoricalShadowOnly,
	}
}
