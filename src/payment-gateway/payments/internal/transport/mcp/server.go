package mcp

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/application"
	paymentsv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/payments/v1"
)

const (
	integrationHeader = "x-fraud-integration"
	scopeHeader       = "x-fraud-scope"
	scopeIntegration  = "integration"
	scopeAll          = "all"
)

var (
	errNotFound      = errors.New("payment not found")
	errScopeRequired = errors.New("caller scope required")
)

type tools struct {
	service *application.Service
}

func NewHandler(service *application.Service) http.Handler {
	t := &tools{service: service}
	server := sdk.NewServer(&sdk.Implementation{Name: "payments", Version: "1.0.0"}, nil)
	readOnly := &sdk.ToolAnnotations{ReadOnlyHint: true}
	sdk.AddTool(server, &sdk.Tool{
		Name:        "get_payment_status",
		Description: "Read a payment's status, version and its attempts' status, decline reason and assessment status.",
		Annotations: readOnly,
	}, t.paymentStatus)
	sdk.AddTool(server, &sdk.Tool{
		Name:        "get_attempt_risk_inputs",
		Description: "Rebuild the inputs the risk tools take for one attempt of a payment (the latest by default) from what was recorded when it started, with the attempt's decline source and reason.",
		Annotations: readOnly,
	}, t.attemptRiskInputs)
	return sdk.NewStreamableHTTPHandler(func(*http.Request) *sdk.Server { return server }, &sdk.StreamableHTTPOptions{Stateless: true, JSONResponse: true})
}

type paymentStatusInput struct {
	IntegrationID string `json:"integration_id" jsonschema:"the integration that owns the payment"`
	PaymentID     string `json:"payment_id" jsonschema:"the payment id"`
}

type attemptSummary struct {
	AttemptID        string `json:"attempt_id"`
	Status           string `json:"status"`
	DeclineReason    string `json:"decline_reason,omitempty"`
	AssessmentStatus string `json:"assessment_status,omitempty"`
}

type paymentStatusOutput struct {
	PaymentID string           `json:"payment_id"`
	Status    string           `json:"status"`
	Version   uint64           `json:"version"`
	Attempts  []attemptSummary `json:"attempts"`
}

func (t *tools) paymentStatus(ctx context.Context, request *sdk.CallToolRequest, input paymentStatusInput) (*sdk.CallToolResult, paymentStatusOutput, error) {
	if err := scoped(request, input.IntegrationID); err != nil {
		return nil, paymentStatusOutput{}, err
	}
	payment, err := t.service.GetPayment(ctx, &paymentsv1.GetPaymentRequest{IntegrationId: input.IntegrationID, PaymentId: input.PaymentID})
	if err != nil {
		return nil, paymentStatusOutput{}, toolError(ctx, err)
	}
	output := paymentStatusOutput{
		PaymentID: payment.GetPaymentId(),
		Status:    enumName(payment.GetStatus().String(), "PAYMENT_STATUS_"),
		Version:   payment.GetVersion(),
		Attempts:  []attemptSummary{},
	}
	for _, attempt := range payment.GetAttempts() {
		output.Attempts = append(output.Attempts, attemptSummary{
			AttemptID:        attempt.GetAttemptId(),
			Status:           enumName(attempt.GetStatus().String(), "ATTEMPT_STATUS_"),
			DeclineReason:    attempt.GetDeclineReason(),
			AssessmentStatus: enumName(attempt.GetAssessmentStatus().String(), "ASSESSMENT_STATUS_"),
		})
	}
	return nil, output, nil
}

type attemptRiskInputsInput struct {
	IntegrationID string `json:"integration_id" jsonschema:"the integration that owns the payment"`
	PaymentID     string `json:"payment_id" jsonschema:"the payment id"`
	AttemptID     string `json:"attempt_id,omitempty" jsonschema:"the attempt id; the latest attempt when omitted"`
}

type riskInputs struct {
	CustomerID               string    `json:"customer_id"`
	PaymentMethodFingerprint string    `json:"payment_method_fingerprint"`
	AmountMinor              int64     `json:"amount_minor"`
	CustomerAccountCreatedAt time.Time `json:"customer_account_created_at"`
	MerchantAccountCreatedAt time.Time `json:"merchant_account_created_at"`
	ShippingCountry          string    `json:"shipping_country"`
	IPCountry                string    `json:"ip_country"`
	ItemCount                int       `json:"item_count"`
	HasDevice                bool      `json:"has_device"`
}

type attemptRiskInputsOutput struct {
	PaymentID     string     `json:"payment_id"`
	AttemptID     string     `json:"attempt_id"`
	DeclineSource string     `json:"decline_source,omitempty"`
	DeclineReason string     `json:"decline_reason,omitempty"`
	RiskInputs    riskInputs `json:"risk_inputs"`
}

func (t *tools) attemptRiskInputs(ctx context.Context, request *sdk.CallToolRequest, input attemptRiskInputsInput) (*sdk.CallToolResult, attemptRiskInputsOutput, error) {
	if err := scoped(request, input.IntegrationID); err != nil {
		return nil, attemptRiskInputsOutput{}, err
	}
	inputs, err := t.service.AttemptRiskInputs(ctx, application.AttemptRiskInputsQuery{
		IntegrationID: input.IntegrationID, PaymentID: input.PaymentID, AttemptID: input.AttemptID,
	})
	if err != nil {
		return nil, attemptRiskInputsOutput{}, toolError(ctx, err)
	}
	return nil, attemptRiskInputsOutput{
		PaymentID:     inputs.PaymentID,
		AttemptID:     inputs.AttemptID,
		DeclineSource: inputs.DeclineSource,
		DeclineReason: inputs.DeclineReason,
		RiskInputs: riskInputs{
			CustomerID:               inputs.CustomerID,
			PaymentMethodFingerprint: inputs.PaymentMethodFingerprint,
			AmountMinor:              inputs.AmountMinor,
			CustomerAccountCreatedAt: inputs.CustomerAccountCreatedAt,
			MerchantAccountCreatedAt: inputs.MerchantAccountCreatedAt,
			ShippingCountry:          inputs.ShippingCountry,
			IPCountry:                inputs.IPCountry,
			ItemCount:                inputs.ItemCount,
			HasDevice:                inputs.HasDevice,
		},
	}, nil
}

func scoped(request *sdk.CallToolRequest, integrationID string) error {
	if request.Extra == nil {
		return errScopeRequired
	}
	switch request.Extra.Header.Get(scopeHeader) {
	case scopeAll:
		return nil
	case scopeIntegration:
		if bound := request.Extra.Header.Get(integrationHeader); bound != "" && bound == integrationID {
			return nil
		}
		return errNotFound
	}
	return errScopeRequired
}

func toolError(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, application.ErrNotFound):
		return errNotFound
	case errors.Is(err, application.ErrInvalidRequest):
		return err
	}
	slog.ErrorContext(ctx, "payments tool failed", "error", err)
	return errors.New("payments are unavailable")
}

func enumName(name, prefix string) string {
	name = strings.ToLower(strings.TrimPrefix(name, prefix))
	if name == "unspecified" {
		return ""
	}
	return name
}
