package wire

import (
	"slices"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/domain"
	paymentsv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/payments/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func timestamp(value time.Time) *timestamppb.Timestamp {
	if value.IsZero() {
		return nil
	}
	return timestamppb.New(value)
}

func instant(value *timestamppb.Timestamp) time.Time {
	if value == nil {
		return time.Time{}
	}
	return value.AsTime()
}

func Snapshot(value *paymentsv1.CheckoutSnapshot) *domain.CheckoutSnapshot {
	if value == nil {
		return nil
	}
	result := &domain.CheckoutSnapshot{ExternalOrderID: value.GetExternalOrderId(), Total: money(value.GetTotal()), MerchantAccountCreatedAt: instant(value.GetMerchantAccountCreatedAt()), CustomerAccountCreatedAt: instant(value.GetCustomerAccountCreatedAt())}
	for _, item := range value.GetItems() {
		if item == nil {
			result.Items = append(result.Items, nil)
			continue
		}
		result.Items = append(result.Items, &domain.LineItem{ProductID: item.GetProductId(), Category: item.GetCategory(), Condition: item.GetCondition(), Quantity: item.GetQuantity(), UnitPrice: money(item.GetUnitPrice())})
	}
	if shipping := value.GetShipping(); shipping != nil {
		result.Shipping = &domain.ShippingLocation{Country: shipping.GetCountry(), Region: shipping.GetRegion(), PostalCode: shipping.GetPostalCode(), AddressFingerprint: shipping.GetAddressFingerprint()}
	}
	return result
}

func SnapshotProto(value *domain.CheckoutSnapshot) *paymentsv1.CheckoutSnapshot {
	if value == nil {
		return nil
	}
	result := &paymentsv1.CheckoutSnapshot{ExternalOrderId: value.ExternalOrderID, Total: moneyProto(value.Total), MerchantAccountCreatedAt: timestamp(value.MerchantAccountCreatedAt), CustomerAccountCreatedAt: timestamp(value.CustomerAccountCreatedAt)}
	for _, item := range value.Items {
		if item == nil {
			result.Items = append(result.Items, nil)
			continue
		}
		result.Items = append(result.Items, &paymentsv1.LineItem{ProductId: item.ProductID, Category: item.Category, Condition: item.Condition, Quantity: item.Quantity, UnitPrice: moneyProto(item.UnitPrice)})
	}
	if shipping := value.Shipping; shipping != nil {
		result.Shipping = &paymentsv1.ShippingLocation{Country: shipping.Country, Region: shipping.Region, PostalCode: shipping.PostalCode, AddressFingerprint: shipping.AddressFingerprint}
	}
	return result
}

func money(value *paymentsv1.Money) *domain.Money {
	if value == nil {
		return nil
	}
	return &domain.Money{MinorUnits: value.GetMinorUnits(), Currency: value.GetCurrency()}
}

func moneyProto(value *domain.Money) *paymentsv1.Money {
	if value == nil {
		return nil
	}
	return &paymentsv1.Money{MinorUnits: value.MinorUnits, Currency: value.Currency}
}

func Merchant(value *paymentsv1.MerchantRef) *domain.MerchantRef {
	if value == nil {
		return nil
	}
	return &domain.MerchantRef{MerchantID: value.GetMerchantId(), ExternalSellerID: value.GetExternalSellerId()}
}

func MerchantProto(value *domain.MerchantRef) *paymentsv1.MerchantRef {
	if value == nil {
		return nil
	}
	return &paymentsv1.MerchantRef{MerchantId: value.MerchantID, ExternalSellerId: value.ExternalSellerID}
}

func Customer(value *paymentsv1.CustomerRef) *domain.CustomerRef {
	if value == nil {
		return nil
	}
	return &domain.CustomerRef{CustomerID: value.GetCustomerId(), ExternalBuyerID: value.GetExternalBuyerId()}
}

func CustomerProto(value *domain.CustomerRef) *paymentsv1.CustomerRef {
	if value == nil {
		return nil
	}
	return &paymentsv1.CustomerRef{CustomerId: value.CustomerID, ExternalBuyerId: value.ExternalBuyerID}
}

func Signals(value *paymentsv1.AttemptSignals) *domain.AttemptSignals {
	if value == nil {
		return nil
	}
	result := &domain.AttemptSignals{AttemptedAt: instant(value.GetAttemptedAt()), DeviceFingerprint: value.DeviceFingerprint, IPCountry: value.IpCountry, IPNetworkFingerprint: value.IpNetworkFingerprint, UserAgentFamily: value.UserAgentFamily}
	if method := value.GetPaymentMethod(); method != nil {
		result.PaymentMethod = &domain.PaymentMethod{Type: method.GetType(), Fingerprint: method.GetFingerprint()}
	}
	return result
}

func signalsProto(value *domain.AttemptSignals) *paymentsv1.AttemptSignals {
	if value == nil {
		return nil
	}
	result := &paymentsv1.AttemptSignals{AttemptedAt: timestamp(value.AttemptedAt), DeviceFingerprint: value.DeviceFingerprint, IpCountry: value.IPCountry, IpNetworkFingerprint: value.IPNetworkFingerprint, UserAgentFamily: value.UserAgentFamily}
	if method := value.PaymentMethod; method != nil {
		result.PaymentMethod = &paymentsv1.PaymentMethod{Type: method.Type, Fingerprint: method.Fingerprint}
	}
	return result
}

func Assessment(value *paymentsv1.RiskAssessment) *domain.RiskAssessment {
	if value == nil {
		return nil
	}
	return &domain.RiskAssessment{Outcome: domain.RiskOutcome(value.GetOutcome()), ReasonCodes: slices.Clone(value.GetReasonCodes()), PolicyVersion: value.GetPolicyVersion(), ModelVersion: value.ModelVersion, FeatureSetVersion: value.FeatureSetVersion, RiskScore: value.RiskScore, AssessmentID: value.GetAssessmentId(), EvidenceSHA256: value.GetEvidenceSha256()}
}

func assessmentProto(value *domain.RiskAssessment) *paymentsv1.RiskAssessment {
	if value == nil {
		return nil
	}
	return &paymentsv1.RiskAssessment{Outcome: paymentsv1.RiskOutcome(value.Outcome), ReasonCodes: slices.Clone(value.ReasonCodes), PolicyVersion: value.PolicyVersion, ModelVersion: value.ModelVersion, FeatureSetVersion: value.FeatureSetVersion, RiskScore: value.RiskScore, AssessmentId: value.AssessmentID, EvidenceSha256: value.EvidenceSHA256}
}
