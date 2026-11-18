package httptransport

import (
	"bytes"
	"crypto/rand"
	_ "embed"
	"encoding/base64"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"regexp"
	"strconv"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/application"
)

//go:embed templates/pages.html
var pagesSource string

var pages = template.Must(template.New("pages").Parse(pagesSource))

type itemView struct {
	Product  string
	Category string
	Quantity int32
	Price    string
}

type pageData struct {
	Nonce          string
	SessionID      string
	Merchant       string
	Total          string
	Items          []itemView
	Reason         string
	Error          string
	IdempotencyKey string
	CSRF           string
	Title          string
	Message        string
	FormOrigin     string
}

var hostnamePattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$`)

func merchantOrigin(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" {
		return ""
	}
	host := parsed.Hostname()
	if !hostnamePattern.MatchString(host) {
		return ""
	}
	if port := parsed.Port(); port != "" {
		return "https://" + host + ":" + port
	}
	return "https://" + host
}

var zeroDecimalCurrencies = map[string]bool{"JPY": true, "VND": true, "KRW": true, "CLP": true, "ISK": true}

var threeDecimalCurrencies = map[string]bool{"BHD": true, "KWD": true, "JOD": true, "OMR": true, "TND": true}

func formatMoney(minor int64, currency string) string {
	exponent := 2
	switch {
	case zeroDecimalCurrencies[currency]:
		exponent = 0
	case threeDecimalCurrencies[currency]:
		exponent = 3
	}
	if exponent == 0 {
		return fmt.Sprintf("%d %s", minor, currency)
	}
	divisor := int64(1)
	for range exponent {
		divisor *= 10
	}
	return fmt.Sprintf("%d.%0*d %s", minor/divisor, exponent, minor%divisor, currency)
}

func declineText(reason string) string {
	switch reason {
	case "INSUFFICIENT_FUNDS":
		return "The card has insufficient funds."
	case "ISSUER_DECLINED":
		return "The card issuer declined the payment."
	case "INVALID_CARD":
		return "The card was not accepted."
	default:
		return "The payment was declined."
	}
}

func (s *Server) formData(view application.CheckoutView) pageData {
	payment := view.Payment
	items := make([]itemView, len(payment.Items))
	for index, item := range payment.Items {
		items[index] = itemView{
			Product:  item.ProductID,
			Category: item.Category,
			Quantity: item.Quantity,
			Price:    formatMoney(item.UnitPrice, payment.Total.Currency),
		}
	}
	key := rand.Text()
	return pageData{
		SessionID:      payment.SessionID,
		Merchant:       payment.ExternalSellerID,
		Total:          formatMoney(payment.Total.MinorUnits, payment.Total.Currency),
		Items:          items,
		IdempotencyKey: key,
		CSRF:           s.csrfToken(payment.SessionID, key),
		FormOrigin:     merchantOrigin(payment.ReturnURL),
	}
}

func (s *Server) render(w http.ResponseWriter, status int, name string, data pageData) {
	nonce := base64.RawURLEncoding.EncodeToString([]byte(rand.Text()))
	data.Nonce = nonce
	if data.Title == "" {
		data.Title = "Checkout"
	}
	var buffer bytes.Buffer
	if err := pages.ExecuteTemplate(&buffer, name, data); err != nil {
		writeError(w, http.StatusInternalServerError, apiErrorInternal, "internal error")
		return
	}
	header := w.Header()
	header.Set("Content-Type", "text/html; charset=utf-8")
	header.Set("Cache-Control", "no-store")
	header.Set("Referrer-Policy", "no-referrer")
	header.Set("X-Frame-Options", "DENY")
	header.Set("X-Content-Type-Options", "nosniff")
	formAction := "form-action 'self'"
	if data.FormOrigin != "" {
		formAction += " " + data.FormOrigin
	}
	header.Set("Content-Security-Policy", "default-src 'none'; script-src 'nonce-"+nonce+"'; style-src 'nonce-"+nonce+
		"'; connect-src 'self'; "+formAction+"; base-uri 'none'; frame-ancestors 'none'")
	w.Header().Set("Content-Length", strconv.Itoa(buffer.Len()))
	w.WriteHeader(status)
	_, _ = w.Write(buffer.Bytes())
}

func (s *Server) renderMessage(w http.ResponseWriter, status int, title, message string) {
	s.render(w, status, "message", pageData{Title: title, Message: message})
}

func (s *Server) renderFailure(w http.ResponseWriter, r *http.Request, err error) {
	status, _, _ := classify(err)
	logFailure(r, status, err)
	switch status {
	case http.StatusGone:
		s.renderMessage(w, http.StatusGone, "Checkout closed", "This checkout session has expired or is already complete.")
	case http.StatusUnauthorized, http.StatusNotFound:
		s.renderMessage(w, http.StatusNotFound, "Checkout not found", "This checkout link is not valid. Return to the merchant and start again.")
	case http.StatusServiceUnavailable:
		s.renderMessage(w, status, "Temporarily unavailable", "We could not reach the payment service. Please try again shortly.")
	case http.StatusGatewayTimeout:
		s.renderMessage(w, status, "Still working", "We have not confirmed the result yet. Reload this page to see the latest status.")
	default:
		s.renderMessage(w, http.StatusInternalServerError, "Something went wrong", "We could not process this request.")
	}
}
