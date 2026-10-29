package httptransport_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/netip"
	"strings"
	"testing"
	"time"

	vaultv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/vault/v1"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/domain"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/testvectors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/prototext"
)

const (
	buyerEmail = "ada.buyer@example.com"
	buyerName  = "Ada Lovelace"
	streetLine = "42 Main St"
	cityName   = "Springfield"
)

func orderBody(reference string) string {
	return fmt.Sprintf(`{
  "order_reference": %q,
  "seller": {"external_id": "seller-01", "account_created_at": "2024-01-02T03:04:05Z"},
  "buyer": {"external_id": "buyer-01", "account_created_at": "2024-02-03T04:05:06Z", "email": %q},
  "amount": {"minor_units": 3500, "currency": "USD"},
  "items": [
    {"product_id": "p1", "category": "phone", "condition": "good", "quantity": 2, "unit_price": 1000},
    {"product_id": "p2", "category": "case", "condition": "new", "quantity": 1, "unit_price": 1500}
  ],
  "shipping_address": {"name": %q, "line1": %q, "line2": "Apt 5", "city": %q, "region": "IL", "postal_code": "62701", "country": "US"},
  "return_url": "https://shop.example.com/return"
}`, reference, buyerEmail, buyerName, streetLine, cityName)
}

type created struct {
	PaymentID     string `json:"payment_id"`
	SessionID     string `json:"session_id"`
	CheckoutToken string `json:"checkout_token"`
	CheckoutURL   string `json:"checkout_url"`
	ExpiresAt     string `json:"expires_at"`
	Status        string `json:"status"`
	MerchantID    string `json:"merchant_id"`
	CustomerID    string `json:"customer_id"`
}

func (h *harness) createPayment(key, apiKey, reference string) created {
	h.t.Helper()
	res := h.do("POST", "/v1/payments", apiHeaders(apiKey, key), orderBody(reference))
	if res.StatusCode != http.StatusCreated {
		h.t.Fatalf("create payment: %d %s", res.StatusCode, res.body)
	}
	var out created
	if err := json.Unmarshal([]byte(res.body), &out); err != nil {
		h.t.Fatalf("decode: %v", err)
	}
	return out
}

func (h *harness) tokenize(apiKey, number string) (string, string) {
	h.t.Helper()
	res := h.do("POST", "/v1/payment-methods", apiHeaders(apiKey, "tok-"+number), fmt.Sprintf(`{"card_number": %q}`, number))
	if res.StatusCode != http.StatusOK {
		h.t.Fatalf("tokenize: %d %s", res.StatusCode, res.body)
	}
	var out struct {
		Token       string `json:"payment_method_token"`
		Fingerprint string `json:"fingerprint"`
	}
	if err := json.Unmarshal([]byte(res.body), &out); err != nil {
		h.t.Fatalf("decode: %v", err)
	}
	return out.Token, out.Fingerprint
}

func TestMarketplaceShapedRequestCreatesPayment(t *testing.T) {
	h := newHarness(t)
	res := h.do("POST", "/v1/payments", apiHeaders(syntheticKey, "order-1"), orderBody("order-1"))
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d: %s", res.StatusCode, res.body)
	}
	var out created
	if err := json.Unmarshal([]byte(res.body), &out); err != nil {
		t.Fatal(err)
	}
	if out.PaymentID == "" || out.SessionID == "" || out.CheckoutToken == "" || out.MerchantID == "" || out.CustomerID == "" || out.Status != "OPEN" {
		t.Fatalf("incomplete response: %+v", out)
	}
	wantURL := publicOrigin + "/checkout/" + out.SessionID + "?token=" + out.CheckoutToken
	if out.CheckoutURL != wantURL {
		t.Fatalf("checkout_url = %q, want %q", out.CheckoutURL, wantURL)
	}
	if _, err := time.Parse(time.RFC3339, out.ExpiresAt); err != nil {
		t.Fatalf("expires_at: %v", err)
	}
	h.world.mu.Lock()
	defer h.world.mu.Unlock()
	if len(h.world.createCalls) != 1 {
		t.Fatalf("create calls = %d", len(h.world.createCalls))
	}
	call := h.world.createCalls[0]
	if call.GetIntegrationId() != syntheticID || call.GetExternalSellerId() != "seller-01" || call.GetExternalBuyerId() != "buyer-01" {
		t.Fatalf("unexpected identities: %v", call)
	}
	rendered := prototext.Format(call)
	for _, personal := range []string{buyerEmail, buyerName, streetLine, cityName, "Apt 5"} {
		if strings.Contains(rendered, personal) {
			t.Fatalf("personal data %q reached Payments: %s", personal, rendered)
		}
	}
	vectors := testvectors.Load(t)
	want := domain.NewFingerprinter(vectors.SyntheticKey).Address(domain.Address{Line1: streetLine, Line2: "Apt 5", City: cityName, Region: "IL", PostalCode: "62701", Country: "US"})
	shipping := call.GetSnapshot().GetShipping()
	if shipping.GetAddressFingerprint() != want || shipping.GetCountry() != "US" || shipping.GetRegion() != "IL" || shipping.GetPostalCode() != "62701" {
		t.Fatalf("shipping = %v, want fingerprint %s", shipping, want)
	}
	h.assertLogsExclude(buyerEmail, buyerName, streetLine, out.CheckoutToken, syntheticKey)
}

func TestRealIntegrationUsesSecretFingerprintKey(t *testing.T) {
	h := newHarness(t)
	h.createPayment("a", syntheticKey, "order-1")
	h.createPayment("a", realKey, "order-1")
	h.world.mu.Lock()
	defer h.world.mu.Unlock()
	synthetic := h.world.createCalls[0].GetSnapshot().GetShipping().GetAddressFingerprint()
	real := h.world.createCalls[1].GetSnapshot().GetShipping().GetAddressFingerprint()
	if synthetic == real {
		t.Fatal("the same address must fingerprint differently for real and synthetic integrations")
	}
	wantReal := domain.NewFingerprinter(realFPKey).Address(domain.Address{Line1: streetLine, Line2: "Apt 5", City: cityName, Region: "IL", PostalCode: "62701", Country: "US"})
	if real != wantReal {
		t.Fatalf("real fingerprint = %s, want %s", real, wantReal)
	}
}

func TestRepeatedCreateReturnsTheOriginalPayment(t *testing.T) {
	h := newHarness(t)
	first := h.do("POST", "/v1/payments", apiHeaders(syntheticKey, "order-7"), orderBody("order-7"))
	second := h.do("POST", "/v1/payments", apiHeaders(syntheticKey, "order-7"), orderBody("order-7"))
	if first.StatusCode != http.StatusCreated || second.StatusCode != http.StatusCreated {
		t.Fatalf("statuses = %d, %d", first.StatusCode, second.StatusCode)
	}
	if first.body != second.body {
		t.Fatalf("replay differs:\n%s\n%s", first.body, second.body)
	}
	h.world.mu.Lock()
	defer h.world.mu.Unlock()
	if len(h.world.payments) != 1 {
		t.Fatalf("payments = %d, want 1", len(h.world.payments))
	}
}

func TestChangedSnapshotUnderSameKeyConflicts(t *testing.T) {
	h := newHarness(t)
	h.createPayment("order-9", syntheticKey, "order-9")
	res := h.do("POST", "/v1/payments", apiHeaders(syntheticKey, "order-9"), orderBody("order-other"))
	if res.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d: %s", res.StatusCode, res.body)
	}
}

func TestInvalidSnapshotsAreRejectedBeforePayments(t *testing.T) {
	tests := map[string]func(string) string{
		"items do not sum": func(b string) string { return strings.Replace(b, `"minor_units": 3500`, `"minor_units": 3600`, 1) },
		"http return url":  func(b string) string { return strings.Replace(b, "https://shop", "http://shop", 1) },
		"relative return":  func(b string) string { return strings.Replace(b, "https://shop.example.com/return", "/return", 1) },
		"bad currency":     func(b string) string { return strings.Replace(b, `"USD"`, `"usd"`, 1) },
		"unknown field": func(b string) string {
			return strings.Replace(b, `"order_reference"`, `"integration_id": "x", "order_reference"`, 1)
		},
		"not json":           func(string) string { return `{` },
		"trailing data":      func(b string) string { return b + `{}` },
		"missing seller":     func(b string) string { return strings.Replace(b, `"external_id": "seller-01"`, `"external_id": ""`, 1) },
		"fractional minor":   func(b string) string { return strings.Replace(b, `"minor_units": 3500`, `"minor_units": 35.5`, 1) },
		"bad creation time":  func(b string) string { return strings.Replace(b, "2024-01-02T03:04:05Z", "yesterday", 1) },
		"bad shipping state": func(b string) string { return strings.Replace(b, `"country": "US"`, `"country": "USA"`, 1) },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			res := h.do("POST", "/v1/payments", apiHeaders(syntheticKey, "k"), mutate(orderBody("o")))
			if res.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d: %s", res.StatusCode, res.body)
			}
			h.world.mu.Lock()
			defer h.world.mu.Unlock()
			if len(h.world.createCalls) != 0 {
				t.Fatal("Payments was called")
			}
		})
	}
}

func TestMutatingRoutesRequireAnIdempotencyKey(t *testing.T) {
	h := newHarness(t)
	payment := h.createPayment("order-1", syntheticKey, "order-1")
	token, _ := h.tokenize(syntheticKey, "4111000000000008")
	h.world.mu.Lock()
	baseline := len(h.world.createCalls)
	h.world.mu.Unlock()
	tests := []struct {
		name, path, auth, body string
	}{
		{"create payment", "/v1/payments", syntheticKey, orderBody("order-2")},
		{"attempt", "/v1/checkout/sessions/" + payment.SessionID + "/attempts", payment.CheckoutToken, fmt.Sprintf(`{"payment_method_token": %q}`, token)},
		{"payment method", "/v1/payment-methods", syntheticKey, `{"card_number": "4111000000000008"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res := h.do("POST", test.path, apiHeaders(test.auth, ""), test.body)
			if res.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d: %s", res.StatusCode, res.body)
			}
		})
	}
	h.world.mu.Lock()
	defer h.world.mu.Unlock()
	if len(h.world.createCalls) != baseline || len(h.world.startCalls) != 0 {
		t.Fatalf("Payments was called: creates=%d starts=%d", len(h.world.createCalls), len(h.world.startCalls))
	}
}

func TestUnknownAPIKeyIsRejected(t *testing.T) {
	h := newHarness(t)
	for name, header := range map[string]map[string]string{
		"unknown key": apiHeaders("wrong-key", "k"),
		"no header":   {"Idempotency-Key": "k"},
		"basic auth":  {"Authorization": "Basic abc", "Idempotency-Key": "k"},
		"empty":       {"Authorization": "Bearer ", "Idempotency-Key": "k"},
	} {
		t.Run(name, func(t *testing.T) {
			for _, request := range []struct{ method, path string }{
				{"POST", "/v1/payments"}, {"GET", "/v1/payments/pay-1"}, {"POST", "/v1/payment-methods"},
			} {
				res := h.do(request.method, request.path, header, orderBody("o"))
				if res.StatusCode != http.StatusUnauthorized {
					t.Fatalf("%s %s status = %d", request.method, request.path, res.StatusCode)
				}
			}
		})
	}
	h.world.mu.Lock()
	defer h.world.mu.Unlock()
	if len(h.world.createCalls)+len(h.world.getCalls)+h.world.tokenizeCalls != 0 {
		t.Fatal("a dependency was called for an unauthenticated request")
	}
}

func TestRawCardFieldsAreRejectedOnEveryJSONRoute(t *testing.T) {
	const number = "4111000000000008"
	h := newHarness(t)
	payment := h.createPayment("order-1", syntheticKey, "order-1")
	attemptPath := "/v1/checkout/sessions/" + payment.SessionID + "/attempts"
	withField := func(field string) string {
		return strings.Replace(orderBody("o"), `"order_reference"`, fmt.Sprintf(`%q: %q, "order_reference"`, field, number), 1)
	}
	nested := strings.Replace(orderBody("o"), `"email": `, `"payment": {"inner": [{"cvv": "123"}]}, "email": `, 1)
	cases := []struct {
		name, path, auth, body string
	}{
		{"payment card_number", "/v1/payments", syntheticKey, withField("card_number")},
		{"payment number", "/v1/payments", syntheticKey, withField("number")},
		{"payment pan", "/v1/payments", syntheticKey, withField("PAN")},
		{"payment card", "/v1/payments", syntheticKey, withField("card")},
		{"payment nested cvv", "/v1/payments", syntheticKey, nested},
		{"attempt card_number", attemptPath, payment.CheckoutToken, fmt.Sprintf(`{"payment_method_token": "t", "card_number": %q}`, number)},
		{"attempt cvc", attemptPath, payment.CheckoutToken, `{"payment_method_token": "t", "cvc": "123"}`},
		{"attempt expiry", attemptPath, payment.CheckoutToken, `{"payment_method_token": "t", "expiry": "12/30"}`},
		{"attempt security code", attemptPath, payment.CheckoutToken, `{"payment_method_token": "t", "card_security_code": "123"}`},
		{"attempt nested card", attemptPath, payment.CheckoutToken, fmt.Sprintf(`{"payment_method_token": "t", "simulated_client": {"card": {"number": %q}}}`, number)},
		{"payment method cvv", "/v1/payment-methods", syntheticKey, fmt.Sprintf(`{"card_number": %q, "cvv": "123"}`, number)},
		{"payment method expiry", "/v1/payment-methods", syntheticKey, fmt.Sprintf(`{"card_number": %q, "expiry": "12/30"}`, number)},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			res := h.do("POST", test.path, apiHeaders(test.auth, "k-"+test.name), test.body)
			if res.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d: %s", res.StatusCode, res.body)
			}
			if strings.Contains(res.body, number) || strings.Contains(res.body, "123") {
				t.Fatalf("response echoes card data: %s", res.body)
			}
		})
	}
	h.world.mu.Lock()
	defer h.world.mu.Unlock()
	if len(h.world.createCalls) != 1 || len(h.world.startCalls) != 0 || h.world.tokenizeCalls != 0 {
		t.Fatalf("a dependency was called: creates=%d starts=%d tokenize=%d", len(h.world.createCalls), len(h.world.startCalls), h.world.tokenizeCalls)
	}
	h.assertLogsExclude(number, `"cvv"`)
}

func TestCallerClaimingAnotherIntegrationActsOnlyWithinItsOwn(t *testing.T) {
	h := newHarness(t)
	payment := h.createPayment("order-1", syntheticKey, "order-1")

	claimed := apiHeaders(realKey, "")
	claimed["X-Integration-ID"] = syntheticID
	res := h.do("GET", "/v1/payments/"+payment.PaymentID, claimed, "")
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("foreign GET status = %d: %s", res.StatusCode, res.body)
	}
	body := strings.Replace(orderBody("o"), `"order_reference"`, fmt.Sprintf(`"integration_id": %q, "order_reference"`, syntheticID), 1)
	if res := h.do("POST", "/v1/payments", apiHeaders(realKey, "k"), body); res.StatusCode != http.StatusBadRequest {
		t.Fatalf("body naming another integration status = %d", res.StatusCode)
	}
	h.createPayment("k2", realKey, "o2")

	h.world.mu.Lock()
	defer h.world.mu.Unlock()
	if got := h.world.getCalls[0].GetIntegrationId(); got != realID {
		t.Fatalf("Payments saw integration %s, want %s", got, realID)
	}
	if got := h.world.createCalls[len(h.world.createCalls)-1].GetIntegrationId(); got != realID {
		t.Fatalf("create used integration %s, want %s", got, realID)
	}
}

func TestGetPaymentReturnsOwnPayment(t *testing.T) {
	h := newHarness(t)
	payment := h.createPayment("order-1", syntheticKey, "order-1")
	res := h.do("GET", "/v1/payments/"+payment.PaymentID, apiHeaders(syntheticKey, ""), "")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d: %s", res.StatusCode, res.body)
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(res.body), &out); err != nil {
		t.Fatal(err)
	}
	if out["payment_id"] != payment.PaymentID || out["status"] != "OPEN" || out["order_reference"] != "order-1" {
		t.Fatalf("body = %v", out)
	}
	if strings.Contains(res.body, payment.CheckoutToken) {
		t.Fatal("GET must not return the checkout token")
	}
}

func TestPaymentMethodRouteIsSyntheticOnly(t *testing.T) {
	h := newHarness(t)
	res := h.do("POST", "/v1/payment-methods", apiHeaders(realKey, "k"), `{"card_number": "4111000000000008"}`)
	if res.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d: %s", res.StatusCode, res.body)
	}
	h.world.mu.Lock()
	defer h.world.mu.Unlock()
	if h.world.tokenizeCalls != 0 {
		t.Fatal("the vault was called for a real integration")
	}
}

func TestTestCardTokensMatchIdentityVectorsAndAreOpaque(t *testing.T) {
	vectors := testvectors.Load(t)
	if len(vectors.TestCards) == 0 {
		t.Fatal("identity vectors must include test_cards")
	}
	h := newHarness(t)
	for _, card := range vectors.TestCards {
		t.Run(card.CardRef, func(t *testing.T) {
			first, fingerprint := h.tokenize(syntheticKey, card.Number)
			second, again := h.tokenize(syntheticKey, card.Number)
			if fingerprint != card.Expected || again != card.Expected {
				t.Fatalf("fingerprint = %s / %s, want %s", fingerprint, again, card.Expected)
			}
			if first == second {
				t.Fatal("reissued tokens must differ")
			}
			if strings.Contains(first, card.Behavior) || strings.Contains(first, card.CardRef) {
				t.Fatalf("token %q leaks behavior or card reference", first)
			}
		})
	}
	h.world.mu.Lock()
	defer h.world.mu.Unlock()
	if h.world.tokenizeCalls != 2*len(vectors.TestCards) {
		t.Fatalf("tokenize calls = %d: tokenization must not be replay-cached", h.world.tokenizeCalls)
	}
}

func TestNumberOutsideAllowListIsRejectedAndNeverLogged(t *testing.T) {
	const number = "4111111111111111"
	h := newHarness(t)
	for _, body := range []string{
		fmt.Sprintf(`{"card_number": %q}`, number),
		`{"card_number": "not a number"}`,
	} {
		res := h.do("POST", "/v1/payment-methods", apiHeaders(syntheticKey, "k"), body)
		if res.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d: %s", res.StatusCode, res.body)
		}
		if strings.Contains(res.body, number) {
			t.Fatalf("response echoes the number: %s", res.body)
		}
	}
	h.assertLogsExclude(number, "not a number")
}

func TestUnavailableVaultDoesNotLeakIntoRejectedCardStatus(t *testing.T) {
	h := newHarness(t)
	h.world.fail("TokenizeCard", status.Error(codes.Unavailable, "vault down"))
	res := h.do("POST", "/v1/payment-methods", apiHeaders(syntheticKey, "k"), `{"card_number": "4111000000000008"}`)
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", res.StatusCode)
	}
}

func attemptPath(payment created) string {
	return "/v1/checkout/sessions/" + payment.SessionID + "/attempts"
}

func TestSyntheticAttemptCarriesVaultFingerprintAndSimulatedContext(t *testing.T) {
	vectors := testvectors.Load(t)
	network := vectors.Fingerprint(t, "ip_network")
	h := newHarness(t)
	payment := h.createPayment("order-1", syntheticKey, "order-1")
	token, fingerprint := h.tokenize(syntheticKey, "4111000000000008")

	clicked := h.world.now().Add(-10 * time.Minute)
	body := fmt.Sprintf(`{"payment_method_token": %q, "device_fingerprint": "device-abc", "user_agent_family": "chrome",
		"simulated_client": {"ip_country": "VN", "ip_network": %q, "attempted_at": %q}}`, token, network.Input, clicked.Format(time.RFC3339Nano))
	res := h.do("POST", attemptPath(payment), apiHeaders(payment.CheckoutToken, "attempt-1"), body)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d: %s", res.StatusCode, res.body)
	}
	var out struct {
		AttemptID string `json:"attempt_id"`
		Status    string `json:"status"`
	}
	if err := json.Unmarshal([]byte(res.body), &out); err != nil || out.Status != "SUCCEEDED" || out.AttemptID == "" {
		t.Fatalf("body = %s", res.body)
	}
	h.world.mu.Lock()
	defer h.world.mu.Unlock()
	call := h.world.startCalls[0]
	signals := call.GetSignals()
	if call.GetIntegrationId() != syntheticID || call.GetIdempotencyKey() != "attempt-1" || call.GetPaymentMethodToken() != token {
		t.Fatalf("call = %v", call)
	}
	if signals.GetPaymentMethod().GetFingerprint() != fingerprint || signals.GetPaymentMethod().GetType() != "card" {
		t.Fatalf("payment method = %v", signals.GetPaymentMethod())
	}
	if signals.GetIpCountry() != "VN" || signals.GetIpNetworkFingerprint() != network.Expected {
		t.Fatalf("simulated context = %v, want network %s", signals, network.Expected)
	}
	if signals.GetDeviceFingerprint() != "device-abc" || signals.GetUserAgentFamily() != "chrome" || !signals.GetAttemptedAt().AsTime().Equal(clicked) {
		t.Fatalf("signals = %v", signals)
	}
	h.assertLogsExclude(token, payment.CheckoutToken)
}

func TestAttemptWithoutContextLeavesSignalsAbsent(t *testing.T) {
	h := newHarness(t)
	payment := h.createPayment("order-1", realKey, "order-1")
	token := h.world.issue(realID, "ok")
	res := h.do("POST", attemptPath(payment), apiHeaders(payment.CheckoutToken, "attempt-1"), fmt.Sprintf(`{"payment_method_token": %q}`, token))
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d: %s", res.StatusCode, res.body)
	}
	h.world.mu.Lock()
	defer h.world.mu.Unlock()
	signals := h.world.startCalls[0].GetSignals()
	if signals.IpCountry != nil || signals.IpNetworkFingerprint != nil || signals.DeviceFingerprint != nil || signals.UserAgentFamily != nil {
		t.Fatalf("absent signals were populated: %v", signals)
	}
}

func (w *world) issue(integration, behavior string) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.counter++
	value := fmt.Sprintf("vt_issued_%d", w.counter)
	w.tokens[value] = &vaultToken{integration: integration, behavior: behavior, method: &vaultv1.MethodMetadata{Type: "card", Fingerprint: "issued-fingerprint"}}
	return value
}

func TestRealIntegrationCannotSendSimulatedContext(t *testing.T) {
	h := newHarness(t)
	payment := h.createPayment("order-1", realKey, "order-1")
	token := h.world.issue(realID, "ok")
	body := fmt.Sprintf(`{"payment_method_token": %q, "simulated_client": {"ip_country": "VN", "ip_network": "203.0.113.5"}}`, token)
	res := h.do("POST", attemptPath(payment), apiHeaders(payment.CheckoutToken, "attempt-1"), body)
	if res.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d: %s", res.StatusCode, res.body)
	}
	h.world.mu.Lock()
	defer h.world.mu.Unlock()
	if len(h.world.startCalls) != 0 {
		t.Fatal("an attempt started")
	}
}

func TestAttemptResultsMapToApiStatuses(t *testing.T) {
	tests := []struct {
		card, status, reason string
		pending              bool
	}{
		{card: "4111000000000008", status: "SUCCEEDED"},
		{card: "4112000000000007", status: "DECLINED", reason: "INSUFFICIENT_FUNDS"},
		{card: "4113000000000006", status: "DECLINED", reason: "ISSUER_DECLINED"},
		{card: "4114000000000005", status: "DECLINED", reason: "INVALID_CARD"},
		{card: "4111000000000008", status: "PENDING", pending: true},
	}
	for _, test := range tests {
		t.Run(test.status+test.reason, func(t *testing.T) {
			h := newHarness(t)
			h.world.forcePending = test.pending
			payment := h.createPayment("order-1", syntheticKey, "order-1")
			token, _ := h.tokenize(syntheticKey, test.card)
			res := h.do("POST", attemptPath(payment), apiHeaders(payment.CheckoutToken, "a1"), fmt.Sprintf(`{"payment_method_token": %q}`, token))
			var out struct {
				Status string `json:"status"`
				Reason string `json:"reason"`
			}
			if err := json.Unmarshal([]byte(res.body), &out); err != nil || res.StatusCode != http.StatusOK {
				t.Fatalf("%d %s", res.StatusCode, res.body)
			}
			if out.Status != test.status || out.Reason != test.reason {
				t.Fatalf("got %+v", out)
			}
		})
	}
}

func TestAttemptRejectsBadCheckoutCredentials(t *testing.T) {
	h := newHarness(t)
	payment := h.createPayment("order-1", syntheticKey, "order-1")
	token, _ := h.tokenize(syntheticKey, "4111000000000008")
	body := fmt.Sprintf(`{"payment_method_token": %q}`, token)

	if res := h.do("POST", attemptPath(payment), map[string]string{"Idempotency-Key": "a"}, body); res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no token: %d", res.StatusCode)
	}
	if res := h.do("POST", attemptPath(payment), apiHeaders("wrong", "a"), body); res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong token: %d", res.StatusCode)
	}
	if res := h.do("POST", "/v1/checkout/sessions/sess-unknown/attempts", apiHeaders(payment.CheckoutToken, "a"), body); res.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown session: %d", res.StatusCode)
	}
	if res := h.do("POST", attemptPath(payment), apiHeaders(syntheticKey, "a"), body); res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("api key used as checkout token: %d", res.StatusCode)
	}
	h.world.mu.Lock()
	defer h.world.mu.Unlock()
	if len(h.world.startCalls) != 0 {
		t.Fatal("an attempt started")
	}
}

func TestUnknownInactiveOrForeignTokensAreRejectedBeforeStartingAnAttempt(t *testing.T) {
	h := newHarness(t)
	payment := h.createPayment("order-1", syntheticKey, "order-1")
	foreign := h.world.issue(realID, "ok")
	inactive := h.world.issue(syntheticID, "ok")
	h.world.tokens[inactive].inactive = true
	for name, token := range map[string]string{"unknown": "vt_unknown", "foreign": foreign, "inactive": inactive} {
		t.Run(name, func(t *testing.T) {
			res := h.do("POST", attemptPath(payment), apiHeaders(payment.CheckoutToken, "a-"+name), fmt.Sprintf(`{"payment_method_token": %q}`, token))
			if res.StatusCode != http.StatusNotFound {
				t.Fatalf("status = %d: %s", res.StatusCode, res.body)
			}
		})
	}
	h.world.mu.Lock()
	defer h.world.mu.Unlock()
	if len(h.world.startCalls) != 0 {
		t.Fatal("an attempt started")
	}
}

func TestSimulatedClientValidation(t *testing.T) {
	h := newHarness(t)
	payment := h.createPayment("order-1", syntheticKey, "order-1")
	token, _ := h.tokenize(syntheticKey, "4111000000000008")
	future := h.world.now().Add(time.Second).Format(time.RFC3339Nano)
	for name, client := range map[string]string{
		"bad country":    `{"ip_country": "United States"}`,
		"bad network":    `{"ip_network": "not-an-ip"}`,
		"bad time":       `{"attempted_at": "yesterday"}`,
		"future attempt": fmt.Sprintf(`{"attempted_at": %q}`, future),
	} {
		body := fmt.Sprintf(`{"payment_method_token": %q, "simulated_client": %s}`, token, client)
		if res := h.do("POST", attemptPath(payment), apiHeaders(payment.CheckoutToken, "a"), body); res.StatusCode != http.StatusBadRequest {
			t.Fatalf("%s: status = %d", name, res.StatusCode)
		}
	}
	body := fmt.Sprintf(`{"payment_method_token": %q, "simulated_client": {"ip_network": "2001:db8:1:2::9"}}`, token)
	if res := h.do("POST", attemptPath(payment), apiHeaders(payment.CheckoutToken, "a2"), body); res.StatusCode != http.StatusOK {
		t.Fatalf("ipv6 network: %d %s", res.StatusCode, res.body)
	}
	want, _ := domain.NewFingerprinter(testvectors.Load(t).SyntheticKey).Network(netip.MustParseAddr("2001:db8:1:2::9"))
	h.world.mu.Lock()
	defer h.world.mu.Unlock()
	if got := h.world.startCalls[0].GetSignals().GetIpNetworkFingerprint(); got != want {
		t.Fatalf("ipv6 network fingerprint = %s, want %s", got, want)
	}
}

func TestErrorMapping(t *testing.T) {
	tests := []struct {
		code codes.Code
		want int
	}{
		{codes.InvalidArgument, http.StatusBadRequest},
		{codes.Unauthenticated, http.StatusUnauthorized},
		{codes.NotFound, http.StatusNotFound},
		{codes.PermissionDenied, http.StatusNotFound},
		{codes.AlreadyExists, http.StatusConflict},
		{codes.Aborted, http.StatusConflict},
		{codes.Unavailable, http.StatusServiceUnavailable},
		{codes.DeadlineExceeded, http.StatusGatewayTimeout},
		{codes.Internal, http.StatusInternalServerError},
		{codes.Unknown, http.StatusInternalServerError},
	}
	for _, test := range tests {
		t.Run(test.code.String(), func(t *testing.T) {
			h := newHarness(t)
			h.world.fail("GetPayment", status.Error(test.code, "secret internal detail at 10.0.0.7"))
			res := h.do("GET", "/v1/payments/pay-1", apiHeaders(syntheticKey, ""), "")
			if res.StatusCode != test.want {
				t.Fatalf("status = %d, want %d: %s", res.StatusCode, test.want, res.body)
			}
			var out struct {
				Error struct{ Code, Message string }
			}
			if err := json.Unmarshal([]byte(res.body), &out); err != nil || out.Error.Code == "" {
				t.Fatalf("body = %s", res.body)
			}
			if strings.Contains(res.body, "secret internal detail") || strings.Contains(res.body, "10.0.0.7") {
				t.Fatalf("internal detail exposed: %s", res.body)
			}
			h.assertLogsExclude("secret internal detail")
		})
	}
}

func TestGatewayDeadlineBoundsOutboundCalls(t *testing.T) {
	h := newHarness(t, func(o *harnessOptions) { o.paymentsTimeout = 100 * time.Millisecond })
	h.world.block["GetPayment"] = make(chan struct{})
	start := time.Now()
	res := h.do("GET", "/v1/payments/pay-1", apiHeaders(syntheticKey, ""), "")
	if res.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("status = %d: %s", res.StatusCode, res.body)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("call took %s", elapsed)
	}
}

func TestRequestIDPropagatesToInternalCalls(t *testing.T) {
	h := newHarness(t)
	res := h.do("GET", "/v1/payments/pay-1", map[string]string{"Authorization": "Bearer " + syntheticKey, "X-Request-ID": "req-123"}, "")
	if res.Header.Get("X-Request-ID") != "req-123" {
		t.Fatalf("response request id = %q", res.Header.Get("X-Request-ID"))
	}
	generated := h.do("GET", "/health", nil, "")
	if generated.Header.Get("X-Request-ID") == "" {
		t.Fatal("a request id must be generated")
	}
	h.world.mu.Lock()
	defer h.world.mu.Unlock()
	if len(h.world.requestIDs) != 1 || h.world.requestIDs[0] != "req-123" {
		t.Fatalf("propagated ids = %v", h.world.requestIDs)
	}
}

func TestAccessLogHoldsOnlyMethodRouteStatusAndRequestID(t *testing.T) {
	h := newHarness(t)
	payment := h.createPayment("order-1", syntheticKey, "order-1")
	h.do("GET", "/checkout/"+payment.SessionID+"?token="+payment.CheckoutToken, nil, "")
	h.do("GET", "/health", nil, "")
	logs := h.logs.String()
	if strings.Contains(logs, payment.CheckoutToken) || strings.Contains(logs, payment.SessionID) || strings.Contains(logs, "/health") {
		t.Fatalf("access log leaks path data or health noise:\n%s", logs)
	}
	for _, want := range []string{`"route":"POST /v1/payments"`, `"route":"GET /checkout/{session_id}"`, `"status":201`, `"method":"POST"`, `"request_id"`} {
		if !strings.Contains(logs, want) {
			t.Fatalf("access log lacks %s:\n%s", want, logs)
		}
	}
}

func TestHealthEndpoints(t *testing.T) {
	h := newHarness(t)
	if res := h.do("GET", "/health", nil, ""); res.StatusCode != http.StatusOK {
		t.Fatalf("health = %d", res.StatusCode)
	}
}
