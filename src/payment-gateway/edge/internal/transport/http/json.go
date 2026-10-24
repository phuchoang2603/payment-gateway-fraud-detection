package httptransport

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/domain"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/requestid"
)

const (
	maxJSONBody       = 64 << 10
	maxTokenizeBody   = 1 << 10
	maxIdempotencyKey = 255
	apiErrorInvalid   = "invalid_request"
	apiErrorInternal  = "internal_error"
)

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorBody{Error: errorDetail{Code: code, Message: message}})
}

func classify(err error) (status int, code, message string) {
	switch {
	case errors.Is(err, domain.ErrInvalid):
		return http.StatusBadRequest, apiErrorInvalid, domain.InvalidDetail(err)
	case errors.Is(err, domain.ErrUnauthorized):
		return http.StatusUnauthorized, "unauthorized", "authentication failed"
	case errors.Is(err, domain.ErrForbidden):
		return http.StatusForbidden, "forbidden", "not permitted for this integration"
	case errors.Is(err, domain.ErrNotFound):
		return http.StatusNotFound, "not_found", "resource not found"
	case errors.Is(err, domain.ErrConflict):
		return http.StatusConflict, "conflict", "request conflicts with an earlier request"
	case errors.Is(err, domain.ErrGone):
		return http.StatusGone, "gone", "checkout session is closed"
	case errors.Is(err, domain.ErrUnavailable):
		return http.StatusServiceUnavailable, "unavailable", "a dependency is unavailable"
	case errors.Is(err, domain.ErrTimeout):
		return http.StatusGatewayTimeout, "timeout", "a dependency timed out"
	default:
		return http.StatusInternalServerError, apiErrorInternal, "internal error"
	}
}

func logFailure(r *http.Request, status int, err error) {
	if status < http.StatusInternalServerError {
		return
	}
	slog.ErrorContext(r.Context(), "edge request failed", "service", "edge", "request_id", requestid.From(r.Context()), "status", status, "error", err.Error())
}

func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	status, code, message := classify(err)
	logFailure(r, status, err)
	writeError(w, status, code, message)
}

func bearerToken(r *http.Request) (string, bool) {
	scheme, token, found := strings.Cut(r.Header.Get("Authorization"), " ")
	token = strings.TrimSpace(token)
	if !found || !strings.EqualFold(scheme, "Bearer") || token == "" {
		return "", false
	}
	return token, true
}

func idempotencyKey(r *http.Request) (string, error) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > maxIdempotencyKey {
		return "", domain.Invalid("Idempotency-Key header is required")
	}
	for _, character := range key {
		if character < 0x21 || character > 0x7e {
			return "", domain.Invalid("Idempotency-Key header is invalid")
		}
	}
	return key, nil
}

func containsCardField(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		for name, nested := range typed {
			if domain.IsCardField(name) || containsCardField(nested) {
				return true
			}
		}
	case []any:
		for _, nested := range typed {
			if containsCardField(nested) {
				return true
			}
		}
	}
	return false
}

// decodeJSON rejects card-like fields anywhere in the body before strict decoding; the offending
// name and value are never echoed or logged.
func decodeJSON(w http.ResponseWriter, r *http.Request, limit int64, allowCardNumber bool, dst any) error {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		return domain.Invalid("request body is unreadable or too large")
	}
	if !allowCardNumber {
		var generic any
		if err := json.NewDecoder(bytes.NewReader(body)).Decode(&generic); err != nil {
			return domain.Invalid("request body is not valid JSON")
		}
		if containsCardField(generic) {
			return domain.Invalid("card fields are not accepted")
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return domain.Invalid("request body has missing, unknown or malformed fields")
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return domain.Invalid("request body has trailing data")
	}
	return nil
}
