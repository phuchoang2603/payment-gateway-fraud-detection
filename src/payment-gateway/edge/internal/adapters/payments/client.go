package payments

import (
	"context"
	"strings"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/adapters/grpcerr"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/application"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/domain"
	paymentsv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/payments/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Client struct {
	client paymentsv1.PaymentServiceClient
}

func New(conn grpc.ClientConnInterface) *Client {
	return &Client{client: paymentsv1.NewPaymentServiceClient(conn)}
}

func (c *Client) CreatePayment(ctx context.Context, cmd application.CreatePaymentCommand) (application.Payment, error) {
	items := make([]*paymentsv1.LineItem, len(cmd.Snapshot.Items))
	for index, item := range cmd.Snapshot.Items {
		items[index] = &paymentsv1.LineItem{
			ProductId: item.ProductID,
			Category:  item.Category,
			Condition: item.Condition,
			Quantity:  item.Quantity,
			UnitPrice: &paymentsv1.Money{MinorUnits: item.UnitPrice, Currency: cmd.Snapshot.Total.Currency},
		}
	}
	response, err := c.client.CreatePayment(ctx, &paymentsv1.CreatePaymentRequest{
		IntegrationId:    cmd.IntegrationID,
		IdempotencyKey:   cmd.IdempotencyKey,
		ExternalSellerId: cmd.SellerExternalID,
		ExternalBuyerId:  cmd.BuyerExternalID,
		ReturnUrl:        cmd.ReturnURL,
		Snapshot: &paymentsv1.CheckoutSnapshot{
			ExternalOrderId:          cmd.Snapshot.OrderReference,
			Total:                    &paymentsv1.Money{MinorUnits: cmd.Snapshot.Total.MinorUnits, Currency: cmd.Snapshot.Total.Currency},
			Items:                    items,
			MerchantAccountCreatedAt: timestamppb.New(cmd.Snapshot.MerchantAccountCreatedAt),
			CustomerAccountCreatedAt: timestamppb.New(cmd.Snapshot.CustomerAccountCreatedAt),
			Shipping: &paymentsv1.ShippingLocation{
				Country:            cmd.Snapshot.Shipping.Country,
				Region:             cmd.Snapshot.Shipping.Region,
				PostalCode:         cmd.Snapshot.Shipping.PostalCode,
				AddressFingerprint: cmd.Snapshot.Shipping.AddressFingerprint,
			},
		},
	})
	if err != nil {
		return application.Payment{}, grpcerr.Map(err, "payments create")
	}
	return paymentFrom(response), nil
}

func (c *Client) GetPayment(ctx context.Context, integrationID, paymentID string) (application.Payment, error) {
	response, err := c.client.GetPayment(ctx, &paymentsv1.GetPaymentRequest{IntegrationId: integrationID, PaymentId: paymentID})
	if err != nil {
		return application.Payment{}, grpcerr.Map(err, "payments get")
	}
	return paymentFrom(response), nil
}

func (c *Client) GetCheckoutSession(ctx context.Context, sessionID, checkoutToken string) (application.Payment, error) {
	response, err := c.client.GetCheckoutSession(ctx, &paymentsv1.GetCheckoutSessionRequest{SessionId: sessionID, CheckoutToken: checkoutToken})
	if err != nil {
		return application.Payment{}, grpcerr.Map(err, "payments session")
	}
	return paymentFrom(response), nil
}

func (c *Client) StartAttempt(ctx context.Context, cmd application.StartAttemptCommand) (application.Attempt, error) {
	signals := &paymentsv1.AttemptSignals{
		AttemptedAt:   timestamppb.New(cmd.Signals.AttemptedAt),
		PaymentMethod: &paymentsv1.PaymentMethod{Type: cmd.Signals.MethodType, Fingerprint: cmd.Signals.MethodFingerprint},
	}
	signals.DeviceFingerprint = optional(cmd.Signals.DeviceFingerprint)
	signals.IpCountry = optional(cmd.Signals.IPCountry)
	signals.IpNetworkFingerprint = optional(cmd.Signals.IPNetworkFingerprint)
	signals.UserAgentFamily = optional(cmd.Signals.UserAgentFamily)
	response, err := c.client.StartAttempt(ctx, &paymentsv1.StartAttemptRequest{
		IntegrationId:      cmd.IntegrationID,
		SessionId:          cmd.SessionID,
		IdempotencyKey:     cmd.IdempotencyKey,
		PaymentMethodToken: cmd.PaymentMethodToken,
		CheckoutToken:      cmd.CheckoutToken,
		Signals:            signals,
	})
	if err != nil {
		return application.Attempt{}, grpcerr.Map(err, "payments attempt")
	}
	attempt := attemptFrom(response)
	if attempt.Status == "" {
		return application.Attempt{}, domain.New(domain.ErrInternal, "payments attempt: unspecified status")
	}
	return attempt, nil
}

func optional(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func attemptFrom(result *paymentsv1.AttemptResult) application.Attempt {
	attempt := application.Attempt{ID: result.GetAttemptId(), Reason: result.GetDeclineReason()}
	if result.GetAssessmentStatus() != paymentsv1.AssessmentStatus_ASSESSMENT_STATUS_UNSPECIFIED {
		attempt.AssessmentStatus = strings.TrimPrefix(result.GetAssessmentStatus().String(), "ASSESSMENT_STATUS_")
	}
	switch result.GetStatus() {
	case paymentsv1.AttemptStatus_ATTEMPT_STATUS_PENDING:
		attempt.Status = application.AttemptPending
	case paymentsv1.AttemptStatus_ATTEMPT_STATUS_SUCCEEDED:
		attempt.Status = application.AttemptSucceeded
	case paymentsv1.AttemptStatus_ATTEMPT_STATUS_DECLINED:
		attempt.Status = application.AttemptDeclined
	}
	return attempt
}

func paymentFrom(p *paymentsv1.Payment) application.Payment {
	payment := application.Payment{
		ID:               p.GetPaymentId(),
		SessionID:        p.GetSessionId(),
		CheckoutToken:    p.GetCheckoutToken(),
		IntegrationID:    p.GetIntegrationId(),
		MerchantID:       p.GetMerchant().GetMerchantId(),
		ExternalSellerID: p.GetMerchant().GetExternalSellerId(),
		CustomerID:       p.GetCustomer().GetCustomerId(),
		OrderReference:   p.GetSnapshot().GetExternalOrderId(),
		ReturnURL:        p.GetReturnUrl(),
		Total: domain.Money{
			MinorUnits: p.GetSnapshot().GetTotal().GetMinorUnits(),
			Currency:   p.GetSnapshot().GetTotal().GetCurrency(),
		},
	}
	if p.GetExpiresAt() != nil {
		payment.ExpiresAt = p.GetExpiresAt().AsTime()
	}
	switch p.GetStatus() {
	case paymentsv1.PaymentStatus_PAYMENT_STATUS_OPEN:
		payment.Status = application.PaymentOpen
	case paymentsv1.PaymentStatus_PAYMENT_STATUS_SUCCEEDED:
		payment.Status = application.PaymentSucceeded
	case paymentsv1.PaymentStatus_PAYMENT_STATUS_EXPIRED:
		payment.Status = application.PaymentExpired
	}
	for _, item := range p.GetSnapshot().GetItems() {
		payment.Items = append(payment.Items, domain.Item{
			ProductID: item.GetProductId(),
			Category:  item.GetCategory(),
			Condition: item.GetCondition(),
			Quantity:  item.GetQuantity(),
			UnitPrice: item.GetUnitPrice().GetMinorUnits(),
		})
	}
	for _, attempt := range p.GetAttempts() {
		payment.Attempts = append(payment.Attempts, attemptFrom(attempt))
	}
	return payment
}
