package httptransport

import (
	"errors"
	"net/http"
	"net/netip"
	"regexp"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/application"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/domain"
)

var (
	userAgentFamilyPattern = regexp.MustCompile(`^[a-z]{1,32}$`)
	deviceFingerprintRegex = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
)

type moneyJSON struct {
	MinorUnits int64  `json:"minor_units"`
	Currency   string `json:"currency"`
}

type sellerJSON struct {
	ExternalID       string    `json:"external_id"`
	AccountCreatedAt time.Time `json:"account_created_at"`
}

// Email is accepted so marketplaces can send their full buyer record, and is discarded at decode.
type buyerJSON struct {
	ExternalID       string    `json:"external_id"`
	AccountCreatedAt time.Time `json:"account_created_at"`
	Email            string    `json:"email"`
}

type itemJSON struct {
	ProductID string `json:"product_id"`
	Category  string `json:"category"`
	Condition string `json:"condition"`
	Quantity  int32  `json:"quantity"`
	UnitPrice int64  `json:"unit_price"`
}

type addressJSON struct {
	Name       string `json:"name"`
	Line1      string `json:"line1"`
	Line2      string `json:"line2"`
	City       string `json:"city"`
	Region     string `json:"region"`
	PostalCode string `json:"postal_code"`
	Country    string `json:"country"`
}

type createPaymentJSON struct {
	OrderReference  string      `json:"order_reference"`
	Seller          sellerJSON  `json:"seller"`
	Buyer           buyerJSON   `json:"buyer"`
	Amount          moneyJSON   `json:"amount"`
	Items           []itemJSON  `json:"items"`
	ShippingAddress addressJSON `json:"shipping_address"`
	ReturnURL       string      `json:"return_url"`
}

func (c createPaymentJSON) toDomain() domain.CheckoutInput {
	items := make([]domain.Item, len(c.Items))
	for index, item := range c.Items {
		items[index] = domain.Item{
			ProductID: item.ProductID,
			Category:  item.Category,
			Condition: item.Condition,
			Quantity:  item.Quantity,
			UnitPrice: item.UnitPrice,
		}
	}
	return domain.CheckoutInput{
		OrderReference: c.OrderReference,
		Seller:         domain.Party{ExternalID: c.Seller.ExternalID, AccountCreatedAt: c.Seller.AccountCreatedAt},
		Buyer:          domain.Party{ExternalID: c.Buyer.ExternalID, AccountCreatedAt: c.Buyer.AccountCreatedAt},
		Total:          domain.Money{MinorUnits: c.Amount.MinorUnits, Currency: c.Amount.Currency},
		Items:          items,
		Shipping:       domain.Address(c.ShippingAddress),
		ReturnURL:      c.ReturnURL,
	}
}

type paymentCreatedJSON struct {
	PaymentID     string `json:"payment_id"`
	SessionID     string `json:"session_id"`
	CheckoutToken string `json:"checkout_token"`
	CheckoutURL   string `json:"checkout_url"`
	ExpiresAt     string `json:"expires_at"`
	Status        string `json:"status"`
	MerchantID    string `json:"merchant_id"`
	CustomerID    string `json:"customer_id"`
}

type attemptJSON struct {
	AssessmentStatus string `json:"assessment_status,omitempty"`
	AttemptID        string `json:"attempt_id"`
	Status           string `json:"status"`
	Reason           string `json:"reason,omitempty"`
}

type paymentJSON struct {
	PaymentID      string        `json:"payment_id"`
	SessionID      string        `json:"session_id"`
	OrderReference string        `json:"order_reference"`
	Status         string        `json:"status"`
	ExpiresAt      string        `json:"expires_at"`
	MerchantID     string        `json:"merchant_id"`
	CustomerID     string        `json:"customer_id"`
	Amount         moneyJSON     `json:"amount"`
	ReturnURL      string        `json:"return_url"`
	Attempts       []attemptJSON `json:"attempts"`
}

func attemptBody(attempt application.Attempt) attemptJSON {
	return attemptJSON{AttemptID: attempt.ID, Status: attempt.Status, Reason: attempt.Reason, AssessmentStatus: attempt.AssessmentStatus}
}

func (s *Server) authenticate(r *http.Request) (domain.Integration, error) {
	key, ok := bearerToken(r)
	if !ok {
		return domain.Integration{}, domain.New(domain.ErrUnauthorized, "missing api key")
	}
	return s.opts.Service.Authenticate(key)
}

func (s *Server) createPayment(w http.ResponseWriter, r *http.Request) {
	integration, err := s.authenticate(r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	key, err := idempotencyKey(r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	var body createPaymentJSON
	if err := decodeJSON(w, r, maxJSONBody, false, &body); err != nil {
		s.fail(w, r, err)
		return
	}
	payment, err := s.opts.Service.CreatePayment(r.Context(), integration, key, body.toDomain())
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, paymentCreatedJSON{
		PaymentID:     payment.ID,
		SessionID:     payment.SessionID,
		CheckoutToken: payment.CheckoutToken,
		CheckoutURL:   s.opts.Service.CheckoutURL(payment),
		ExpiresAt:     payment.ExpiresAt.UTC().Format(time.RFC3339),
		Status:        payment.Status,
		MerchantID:    payment.MerchantID,
		CustomerID:    payment.CustomerID,
	})
}

func (s *Server) getPayment(w http.ResponseWriter, r *http.Request) {
	integration, err := s.authenticate(r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	payment, err := s.opts.Service.GetPayment(r.Context(), integration, r.PathValue("payment_id"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	attempts := make([]attemptJSON, len(payment.Attempts))
	for index, attempt := range payment.Attempts {
		attempts[index] = attemptBody(attempt)
	}
	writeJSON(w, http.StatusOK, paymentJSON{
		PaymentID:      payment.ID,
		SessionID:      payment.SessionID,
		OrderReference: payment.OrderReference,
		Status:         payment.Status,
		ExpiresAt:      payment.ExpiresAt.UTC().Format(time.RFC3339),
		MerchantID:     payment.MerchantID,
		CustomerID:     payment.CustomerID,
		Amount:         moneyJSON{MinorUnits: payment.Total.MinorUnits, Currency: payment.Total.Currency},
		ReturnURL:      payment.ReturnURL,
		Attempts:       attempts,
	})
}

type simulatedClientJSON struct {
	IPCountry   string `json:"ip_country"`
	IPNetwork   string `json:"ip_network"`
	AttemptedAt string `json:"attempted_at"`
}

type createAttemptJSON struct {
	PaymentMethodToken string               `json:"payment_method_token"`
	DeviceFingerprint  string               `json:"device_fingerprint"`
	UserAgentFamily    string               `json:"user_agent_family"`
	SimulatedClient    *simulatedClientJSON `json:"simulated_client"`
}

func parseSimulated(in *simulatedClientJSON) (*application.SimulatedClient, error) {
	if in == nil {
		return nil, nil
	}
	simulated := &application.SimulatedClient{}
	if in.IPCountry != "" {
		if !domain.ValidCountry(in.IPCountry) {
			return nil, domain.Invalid("simulated_client.ip_country must be an ISO 3166 alpha-2 code")
		}
		simulated.IPCountry = in.IPCountry
	}
	if in.IPNetwork != "" {
		address, err := netip.ParseAddr(in.IPNetwork)
		if err != nil {
			prefix, prefixErr := netip.ParsePrefix(in.IPNetwork)
			if prefixErr != nil {
				return nil, domain.Invalid("simulated_client.ip_network must be an IP address or CIDR")
			}
			address = prefix.Addr()
		}
		simulated.IPNetwork = address
	}
	if in.AttemptedAt != "" {
		attempted, err := time.Parse(time.RFC3339Nano, in.AttemptedAt)
		if err != nil {
			return nil, domain.Invalid("simulated_client.attempted_at must be an RFC 3339 timestamp")
		}
		simulated.AttemptedAt = attempted.UTC()
	}
	return simulated, nil
}

func (s *Server) createAttempt(w http.ResponseWriter, r *http.Request) {
	token, ok := bearerToken(r)
	if !ok {
		s.fail(w, r, domain.New(domain.ErrUnauthorized, "missing checkout token"))
		return
	}
	key, err := idempotencyKey(r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	var body createAttemptJSON
	if err := decodeJSON(w, r, maxJSONBody, false, &body); err != nil {
		s.fail(w, r, err)
		return
	}
	if body.PaymentMethodToken == "" || len(body.PaymentMethodToken) > 256 {
		s.fail(w, r, domain.Invalid("payment_method_token is required"))
		return
	}
	if body.DeviceFingerprint != "" && !deviceFingerprintRegex.MatchString(body.DeviceFingerprint) {
		s.fail(w, r, domain.Invalid("device_fingerprint is malformed"))
		return
	}
	if body.UserAgentFamily != "" && !userAgentFamilyPattern.MatchString(body.UserAgentFamily) {
		s.fail(w, r, domain.Invalid("user_agent_family is malformed"))
		return
	}
	simulated, err := parseSimulated(body.SimulatedClient)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	attempt, err := s.opts.Service.StartAttempt(r.Context(), application.AttemptRequest{
		SessionID:          r.PathValue("session_id"),
		CheckoutToken:      token,
		IdempotencyKey:     key,
		PaymentMethodToken: body.PaymentMethodToken,
		DeviceFingerprint:  body.DeviceFingerprint,
		UserAgentFamily:    body.UserAgentFamily,
		Simulated:          simulated,
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, attemptBody(attempt))
}

type paymentMethodRequestJSON struct {
	CardNumber string `json:"card_number"`
}

type paymentMethodResponseJSON struct {
	PaymentMethodToken string `json:"payment_method_token"`
	Type               string `json:"type"`
	Fingerprint        string `json:"fingerprint"`
}

// Tokenization is not replay-cached: the idempotency header is required for uniformity, but each
// call mints a fresh opaque token and the card number never outlives the vault call.
func (s *Server) createPaymentMethod(w http.ResponseWriter, r *http.Request) {
	integration, err := s.authenticate(r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if !integration.Synthetic {
		s.fail(w, r, domain.New(domain.ErrForbidden, "payment methods are limited to synthetic integrations"))
		return
	}
	if _, err := idempotencyKey(r); err != nil {
		s.fail(w, r, err)
		return
	}
	var body paymentMethodRequestJSON
	if err := decodeJSON(w, r, maxTokenizeBody, true, &body); err != nil {
		s.fail(w, r, err)
		return
	}
	token, err := s.opts.Service.TokenizeTestCard(r.Context(), integration, body.CardNumber)
	if err != nil {
		if errors.Is(err, domain.ErrInvalid) {
			err = domain.Invalid("card number is not an accepted test card")
		}
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, paymentMethodResponseJSON{
		PaymentMethodToken: token.Value,
		Type:               token.Method.Type,
		Fingerprint:        token.Method.Fingerprint,
	})
}
