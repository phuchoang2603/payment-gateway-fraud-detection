package httptransport_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/testvectors"
)

const chromeAgent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"

var hiddenField = regexp.MustCompile(`name="([a-z_]+)" value="([^"]*)"`)

type formState struct {
	key, csrf string
}

func parseForm(t *testing.T, body string) formState {
	t.Helper()
	fields := map[string]string{}
	for _, match := range hiddenField.FindAllStringSubmatch(body, -1) {
		fields[match[1]] = match[2]
	}
	if fields["idempotency_key"] == "" || fields["csrf_token"] == "" {
		t.Fatalf("page has no idempotency key or csrf token:\n%s", body)
	}
	return formState{key: fields["idempotency_key"], csrf: fields["csrf_token"]}
}

func (h *harness) cookie(payment created) *http.Cookie {
	h.t.Helper()
	res := h.do("GET", "/checkout/"+payment.SessionID+"?token="+url.QueryEscape(payment.CheckoutToken), nil, "")
	if res.StatusCode != http.StatusSeeOther {
		h.t.Fatalf("exchange status = %d: %s", res.StatusCode, res.body)
	}
	for _, cookie := range res.Cookies() {
		if cookie.Name == "checkout_token" {
			return cookie
		}
	}
	h.t.Fatal("no checkout cookie")
	return nil
}

func (h *harness) page(payment created, cookie *http.Cookie) response {
	header := map[string]string{}
	if cookie != nil {
		header["Cookie"] = cookie.Name + "=" + cookie.Value
	}
	return h.do("GET", "/checkout/"+payment.SessionID, header, "")
}

type submission struct {
	form    formState
	number  string
	origin  string
	device  string
	forward string
	agent   string
	csrf    string
}

func (h *harness) submit(payment created, cookie *http.Cookie, s submission) response {
	return h.submitTo(h.server.URL, payment, cookie, s)
}

func (h *harness) submitTo(base string, payment created, cookie *http.Cookie, s submission) response {
	h.t.Helper()
	if s.origin == "" {
		s.origin = publicOrigin
	}
	if s.csrf == "" {
		s.csrf = s.form.csrf
	}
	if s.agent == "" {
		s.agent = chromeAgent
	}
	values := url.Values{
		"card_number":        {s.number},
		"idempotency_key":    {s.form.key},
		"csrf_token":         {s.csrf},
		"device_fingerprint": {s.device},
	}
	header := map[string]string{
		"Content-Type": "application/x-www-form-urlencoded",
		"Origin":       s.origin,
		"User-Agent":   s.agent,
	}
	if s.origin == "-" {
		delete(header, "Origin")
	}
	if cookie != nil {
		header["Cookie"] = cookie.Name + "=" + cookie.Value
	}
	if s.forward != "" {
		header["X-Forwarded-For"] = s.forward
	}
	request, err := http.NewRequest("POST", base+"/checkout/"+payment.SessionID+"/pay", strings.NewReader(values.Encode()))
	if err != nil {
		h.t.Fatal(err)
	}
	for name, value := range header {
		request.Header.Set(name, value)
	}
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	res, err := client.Do(request)
	if err != nil {
		h.t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	var body strings.Builder
	buffer := make([]byte, 4096)
	for {
		n, readErr := res.Body.Read(buffer)
		body.Write(buffer[:n])
		if readErr != nil {
			break
		}
	}
	return response{Response: res, body: body.String()}
}

func TestTokenIsExchangedForAScopedCookie(t *testing.T) {
	h := newHarness(t)
	payment := h.createPayment("order-1", realKey, "order-1")

	res := h.do("GET", "/checkout/"+payment.SessionID+"?token="+url.QueryEscape(payment.CheckoutToken), nil, "")
	if res.StatusCode != http.StatusSeeOther || res.Header.Get("Location") != "/checkout/"+payment.SessionID {
		t.Fatalf("exchange = %d Location=%q", res.StatusCode, res.Header.Get("Location"))
	}
	if res.Header.Get("Referrer-Policy") != "no-referrer" || res.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("redirect headers = %v", res.Header)
	}
	cookie := res.Cookies()[0]
	if cookie.Value != payment.CheckoutToken || !cookie.HttpOnly || !cookie.Secure || cookie.SameSite != http.SameSiteLaxMode ||
		cookie.Path != "/checkout/"+payment.SessionID {
		t.Fatalf("cookie = %+v", cookie)
	}

	clean := h.page(payment, cookie)
	if clean.StatusCode != http.StatusOK {
		t.Fatalf("page = %d: %s", clean.StatusCode, clean.body)
	}
	for _, want := range []string{"35.00 USD", "p1 (phone)", "p2 (case)", "seller-01", `name="card_number"`, `name="device_fingerprint"`} {
		if !strings.Contains(clean.body, want) {
			t.Fatalf("page lacks %q:\n%s", want, clean.body)
		}
	}
	if strings.Contains(clean.body, payment.CheckoutToken) {
		t.Fatal("the checkout token must not appear in the page")
	}
	policy := clean.Header.Get("Content-Security-Policy")
	if !strings.Contains(policy, "form-action 'self' https://shop.example.com") {
		t.Fatalf("form-action must allow the merchant return origin: %q", policy)
	}
	if !strings.Contains(policy, "default-src 'none'") || !strings.Contains(policy, "frame-ancestors 'none'") ||
		clean.Header.Get("X-Frame-Options") != "DENY" || clean.Header.Get("Referrer-Policy") != "no-referrer" ||
		clean.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("security headers = %v", clean.Header)
	}
	nonce := regexp.MustCompile(`script-src 'nonce-([^']+)'`).FindStringSubmatch(policy)
	if nonce == nil || !strings.Contains(clean.body, `<script nonce="`+nonce[1]+`">`) {
		t.Fatalf("inline script is not bound to the CSP nonce:\n%s", policy)
	}
	h.assertLogsExclude(payment.CheckoutToken)
}

func TestHostedPageRejectsMissingOrInvalidTokens(t *testing.T) {
	h := newHarness(t)
	payment := h.createPayment("order-1", realKey, "order-1")
	if res := h.page(payment, nil); res.StatusCode != http.StatusNotFound {
		t.Fatalf("no cookie = %d", res.StatusCode)
	}
	if res := h.do("GET", "/checkout/"+payment.SessionID+"?token=forged", nil, ""); res.StatusCode != http.StatusNotFound || len(res.Cookies()) != 0 {
		t.Fatalf("forged token = %d cookies=%v", res.StatusCode, res.Cookies())
	}
	if res := h.do("GET", "/checkout/bad%20id?token=x", nil, ""); res.StatusCode != http.StatusNotFound {
		t.Fatalf("malformed session = %d", res.StatusCode)
	}
	forged := &http.Cookie{Name: "checkout_token", Value: "forged"}
	if res := h.page(payment, forged); res.StatusCode != http.StatusNotFound {
		t.Fatalf("forged cookie = %d", res.StatusCode)
	}
}

func TestCrossSiteSubmissionIsRejected(t *testing.T) {
	h := newHarness(t)
	payment := h.createPayment("order-1", realKey, "order-1")
	cookie := h.cookie(payment)
	form := parseForm(t, h.page(payment, cookie).body)

	tests := map[string]submission{
		"foreign origin": {form: form, number: "4111000000000008", origin: "https://evil.example"},
		"wrong csrf":     {form: form, number: "4111000000000008", csrf: "AAAA"},
		"empty csrf":     {form: formState{key: form.key}, number: "4111000000000008"},
	}
	for name, s := range tests {
		t.Run(name, func(t *testing.T) {
			res := h.submit(payment, cookie, s)
			if res.StatusCode != http.StatusForbidden {
				t.Fatalf("status = %d: %s", res.StatusCode, res.body)
			}
		})
	}
	h.world.mu.Lock()
	tokenize, starts := h.world.tokenizeCalls, len(h.world.startCalls)
	h.world.mu.Unlock()
	if tokenize != 0 || starts != 0 {
		t.Fatalf("rejected submissions reached the vault or Payments: tokenize=%d starts=%d", tokenize, starts)
	}
	other := h.createPayment("order-2", realKey, "order-2")
	otherCookie := h.cookie(other)
	if res := h.submit(other, otherCookie, submission{form: form, number: "4111000000000008"}); res.StatusCode != http.StatusForbidden {
		t.Fatalf("csrf token replayed on another session = %d", res.StatusCode)
	}
	if res := h.submit(payment, nil, submission{form: form, number: "4111000000000008"}); res.StatusCode != http.StatusNotFound {
		t.Fatalf("no cookie = %d", res.StatusCode)
	}
}

func TestSubmissionWithoutOriginOrRefererUsesTheCSRFToken(t *testing.T) {
	h := newHarness(t)
	payment := h.createPayment("order-1", realKey, "order-1")
	cookie := h.cookie(payment)
	form := parseForm(t, h.page(payment, cookie).body)

	res := h.submit(payment, cookie, submission{form: form, number: "4111000000000008", origin: "-"})
	if res.StatusCode != http.StatusSeeOther {
		t.Fatalf("no Origin or Referer = %d: %s", res.StatusCode, res.body)
	}
	if res := h.submit(payment, cookie, submission{form: form, number: "4111000000000008", origin: "-", csrf: "AAAA"}); res.StatusCode != http.StatusForbidden {
		t.Fatalf("no Origin with a wrong CSRF token = %d", res.StatusCode)
	}
}

func TestOpaqueOriginUsesTheCSRFToken(t *testing.T) {
	h := newHarness(t)
	payment := h.createPayment("order-1", realKey, "order-1")
	cookie := h.cookie(payment)
	form := parseForm(t, h.page(payment, cookie).body)

	res := h.submit(payment, cookie, submission{form: form, number: "4111000000000008", origin: "null"})
	if res.StatusCode != http.StatusSeeOther {
		t.Fatalf("null Origin = %d: %s", res.StatusCode, res.body)
	}
	if res := h.submit(payment, cookie, submission{form: form, number: "4111000000000008", origin: "null", csrf: "AAAA"}); res.StatusCode != http.StatusForbidden {
		t.Fatalf("null Origin with a wrong CSRF token = %d", res.StatusCode)
	}
}

func TestSubmissionFromTheServingHostIsAccepted(t *testing.T) {
	h := newHarness(t)
	payment := h.createPayment("order-1", realKey, "order-1")
	cookie := h.cookie(payment)
	form := parseForm(t, h.page(payment, cookie).body)

	res := h.submit(payment, cookie, submission{form: form, number: "4111000000000008", origin: h.server.URL})
	if res.StatusCode != http.StatusSeeOther {
		t.Fatalf("origin of the serving host = %d: %s", res.StatusCode, res.body)
	}
}

func TestSuccessfulPaymentRedirectsWithPageContext(t *testing.T) {
	vectors := testvectors.Load(t)
	network := vectors.Fingerprint(t, "ip_network")
	h := newHarness(t)
	payment := h.createPayment("order-1", realKey, "order-1")
	cookie := h.cookie(payment)
	form := parseForm(t, h.page(payment, cookie).body)

	res := h.submit(payment, cookie, submission{
		form: form, number: "4111 0000 0000 0008", device: "device-hash-1",
		forward: "198.51.100.9, " + network.Input,
	})
	if res.StatusCode != http.StatusSeeOther || res.Header.Get("Location") != "https://shop.example.com/return" {
		t.Fatalf("status = %d Location=%q body=%s", res.StatusCode, res.Header.Get("Location"), res.body)
	}
	h.world.mu.Lock()
	defer h.world.mu.Unlock()
	call := h.world.startCalls[0]
	signals := call.GetSignals()
	if call.GetIntegrationId() != realID || call.GetIdempotencyKey() != form.key || call.GetCheckoutToken() != payment.CheckoutToken {
		t.Fatalf("call = %v", call)
	}
	if signals.GetUserAgentFamily() != "chrome" || signals.GetDeviceFingerprint() != "device-hash-1" || signals.GetIpCountry() != "" {
		t.Fatalf("signals = %v", signals)
	}
	wantNetwork, _ := domainFingerprinter(realFPKey).Network(mustAddr(network.Input))
	if signals.GetIpNetworkFingerprint() != wantNetwork {
		t.Fatalf("network fingerprint = %s, want %s (rightmost trusted entry under the real key)", signals.GetIpNetworkFingerprint(), wantNetwork)
	}
	h.assertLogsExclude("4111000000000008", "4111 0000", payment.CheckoutToken, call.GetPaymentMethodToken(), network.Input, "198.51.100.9")
}

func TestUntrustedForwardedHeaderYieldsNoNetworkSignal(t *testing.T) {
	h := newHarness(t)
	payment := h.createPayment("order-1", realKey, "order-1")
	cookie := h.cookie(payment)
	form := parseForm(t, h.page(payment, cookie).body)
	res := h.submit(payment, cookie, submission{form: form, number: "4111000000000008", forward: "not-an-ip", device: "bad device!"})
	if res.StatusCode != http.StatusSeeOther {
		t.Fatalf("status = %d: %s", res.StatusCode, res.body)
	}
	h.world.mu.Lock()
	defer h.world.mu.Unlock()
	signals := h.world.startCalls[0].GetSignals()
	if signals.IpNetworkFingerprint != nil || signals.DeviceFingerprint != nil {
		t.Fatalf("signals = %v", signals)
	}
}

func TestDeclineThenRetrySucceeds(t *testing.T) {
	h := newHarness(t)
	payment := h.createPayment("order-1", realKey, "order-1")
	cookie := h.cookie(payment)
	first := parseForm(t, h.page(payment, cookie).body)

	declined := h.submit(payment, cookie, submission{form: first, number: "4112000000000007"})
	if declined.StatusCode != http.StatusOK || !strings.Contains(declined.body, "insufficient funds") {
		t.Fatalf("decline = %d: %s", declined.StatusCode, declined.body)
	}
	second := parseForm(t, declined.body)
	if second.key == first.key || second.csrf == first.csrf {
		t.Fatal("a re-rendered form after a decline must carry a new idempotency key")
	}
	if page := h.page(payment, cookie); page.StatusCode != http.StatusOK || !strings.Contains(page.body, `name="card_number"`) {
		t.Fatalf("payment must stay open: %d", page.StatusCode)
	}

	success := h.submit(payment, cookie, submission{form: second, number: "4111000000000008"})
	if success.StatusCode != http.StatusSeeOther || success.Header.Get("Location") != "https://shop.example.com/return" {
		t.Fatalf("retry = %d: %s", success.StatusCode, success.body)
	}
	h.world.mu.Lock()
	created := h.world.attemptsCreated
	h.world.mu.Unlock()
	if created != 2 {
		t.Fatalf("attempts = %d, want 2", created)
	}
	if after := h.page(payment, cookie); after.StatusCode != http.StatusSeeOther {
		t.Fatalf("succeeded payment page = %d", after.StatusCode)
	}
}

func TestDoubleSubmitStartsOneAttempt(t *testing.T) {
	h := newHarness(t)
	payment := h.createPayment("order-1", realKey, "order-1")
	cookie := h.cookie(payment)
	form := parseForm(t, h.page(payment, cookie).body)

	one := h.submit(payment, cookie, submission{form: form, number: "4112000000000007"})
	two := h.submit(payment, cookie, submission{form: form, number: "4112000000000007"})
	for _, res := range []response{one, two} {
		if res.StatusCode != http.StatusOK || !strings.Contains(res.body, "insufficient funds") {
			t.Fatalf("response = %d: %s", res.StatusCode, res.body)
		}
	}
	h.world.mu.Lock()
	defer h.world.mu.Unlock()
	if h.world.attemptsCreated != 1 || len(h.world.attemptKeys) != 1 {
		t.Fatalf("attempts created = %d, keys = %d", h.world.attemptsCreated, len(h.world.attemptKeys))
	}
	if len(h.world.startCalls) != 1 {
		t.Fatalf("Payments calls = %d, want the replay served from the first result", len(h.world.startCalls))
	}
}

func TestDoubleSubmitThroughAnotherReplicaStillShowsTheSingleAttempt(t *testing.T) {
	h := newHarness(t)
	payment := h.createPayment("order-1", realKey, "order-1")
	cookie := h.cookie(payment)
	form := parseForm(t, h.page(payment, cookie).body)

	replica := h.mount(h.replicaService())
	one := h.submit(payment, cookie, submission{form: form, number: "4112000000000007"})
	two := h.submitTo(replica.URL, payment, cookie, submission{form: form, number: "4112000000000007"})
	for _, res := range []response{one, two} {
		if res.StatusCode != http.StatusOK || !strings.Contains(res.body, "insufficient funds") {
			t.Fatalf("response = %d: %s", res.StatusCode, res.body)
		}
	}
	h.world.mu.Lock()
	defer h.world.mu.Unlock()
	if h.world.attemptsCreated != 1 {
		t.Fatalf("attempts created = %d, want 1", h.world.attemptsCreated)
	}
}

func TestNumberOutsideAllowListIsRejectedOnThePage(t *testing.T) {
	const number = "4111111111111111"
	h := newHarness(t)
	payment := h.createPayment("order-1", realKey, "order-1")
	cookie := h.cookie(payment)
	form := parseForm(t, h.page(payment, cookie).body)

	for _, entered := range []string{number, "4111 1111 1111 1111", "hello", "123"} {
		res := h.submit(payment, cookie, submission{form: form, number: entered})
		if res.StatusCode != http.StatusBadRequest || !strings.Contains(res.body, "Invalid card") {
			t.Fatalf("%q: status = %d: %s", entered, res.StatusCode, res.body)
		}
		if strings.Contains(res.body, number) || strings.Contains(res.body, entered) {
			t.Fatalf("page echoes the number: %s", res.body)
		}
		if next := parseForm(t, res.body); next.key == form.key {
			t.Fatal("the re-rendered form must carry a new idempotency key")
		}
	}
	h.world.mu.Lock()
	defer h.world.mu.Unlock()
	if len(h.world.startCalls) != 0 || h.world.attemptsCreated != 0 {
		t.Fatal("an attempt started for a rejected card")
	}
	h.assertLogsExclude(number, "4111 1111", "hello")
}

func TestExpiredSessionShowsExpiryPage(t *testing.T) {
	h := newHarness(t)
	payment := h.createPayment("order-1", realKey, "order-1")
	cookie := h.cookie(payment)
	form := parseForm(t, h.page(payment, cookie).body)

	h.world.advance(16 * time.Minute)
	if res := h.page(payment, cookie); res.StatusCode != http.StatusGone || !strings.Contains(res.body, "expired") {
		t.Fatalf("page = %d: %s", res.StatusCode, res.body)
	}
	if res := h.submit(payment, cookie, submission{form: form, number: "4111000000000008"}); res.StatusCode != http.StatusGone {
		t.Fatalf("submit = %d: %s", res.StatusCode, res.body)
	}
	h.world.mu.Lock()
	defer h.world.mu.Unlock()
	if h.world.tokenizeCalls != 0 || len(h.world.startCalls) != 0 {
		t.Fatal("an expired session reached the vault or Payments")
	}
}

func TestPaymentMarkedExpiredShowsExpiryPage(t *testing.T) {
	h := newHarness(t)
	payment := h.createPayment("order-1", realKey, "order-1")
	cookie := h.cookie(payment)
	h.world.mu.Lock()
	h.world.sessions[payment.SessionID].payment.Status = 3
	h.world.mu.Unlock()
	if res := h.page(payment, cookie); res.StatusCode != http.StatusGone {
		t.Fatalf("page = %d", res.StatusCode)
	}
}

func TestPendingOutcomeShowsStatusPageUntilResolved(t *testing.T) {
	h := newHarness(t)
	payment := h.createPayment("order-1", realKey, "order-1")
	cookie := h.cookie(payment)
	form := parseForm(t, h.page(payment, cookie).body)
	h.world.forcePending = true

	res := h.submit(payment, cookie, submission{form: form, number: "4111000000000008"})
	if res.StatusCode != http.StatusOK || !strings.Contains(res.body, "/checkout/"+payment.SessionID+"/status") {
		t.Fatalf("pending page = %d: %s", res.StatusCode, res.body)
	}
	if again := h.page(payment, cookie); !strings.Contains(again.body, "Confirming your payment") {
		t.Fatalf("reload must keep showing the status page: %s", again.body)
	}
	header := map[string]string{"Cookie": cookie.Name + "=" + cookie.Value}
	status := h.do("GET", "/checkout/"+payment.SessionID+"/status", header, "")
	var body struct {
		State       string `json:"state"`
		RedirectURL string `json:"redirect_url"`
	}
	_ = json.Unmarshal([]byte(status.body), &body)
	if status.StatusCode != http.StatusOK || body.State != "pending" {
		t.Fatalf("status = %d %s", status.StatusCode, status.body)
	}

	h.world.mu.Lock()
	stored := h.world.sessions[payment.SessionID].payment
	stored.Attempts[0].Status = 2
	stored.Status = 2
	h.world.mu.Unlock()
	status = h.do("GET", "/checkout/"+payment.SessionID+"/status", header, "")
	_ = json.Unmarshal([]byte(status.body), &body)
	if body.State != "succeeded" || body.RedirectURL != "https://shop.example.com/return" {
		t.Fatalf("resolved status = %s", status.body)
	}
	if res := h.page(payment, cookie); res.StatusCode != http.StatusSeeOther {
		t.Fatalf("resolved page = %d", res.StatusCode)
	}
}

func TestHostedPageReportsDependencyOutageWithoutDetails(t *testing.T) {
	h := newHarness(t)
	payment := h.createPayment("order-1", realKey, "order-1")
	cookie := h.cookie(payment)
	form := parseForm(t, h.page(payment, cookie).body)
	h.world.fail("TokenizeCard", grpcUnavailable())
	res := h.submit(payment, cookie, submission{form: form, number: "4111000000000008"})
	if res.StatusCode != http.StatusServiceUnavailable || strings.Contains(res.body, "vault") {
		t.Fatalf("status = %d: %s", res.StatusCode, res.body)
	}
}
