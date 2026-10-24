package httptransport

import (
	"crypto/rand"
	"log/slog"
	"net/http"
	"strings"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/requestid"
)

const maxRequestIDLength = 128

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	if r.status == 0 {
		r.status = status
	}
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(body []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.ResponseWriter.Write(body)
}

// The access log carries method, route pattern, status and request ID only: never paths with
// tokens, query strings, headers or bodies.
func (s *Server) withCorrelation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := strings.Map(func(character rune) rune {
			if character >= 32 && character < 127 {
				return character
			}
			return -1
		}, r.Header.Get("X-Request-ID"))
		requestID = strings.TrimSpace(requestID)
		if runes := []rune(requestID); len(runes) > maxRequestIDLength {
			requestID = string(runes[:maxRequestIDLength])
		}
		if requestID == "" {
			requestID = rand.Text()
		}
		w.Header().Set("X-Request-ID", requestID)
		r = r.WithContext(requestid.With(r.Context(), requestID))
		recorder := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(recorder, r)
		if r.Pattern == "GET /health" || r.Pattern == "GET /ready" {
			return
		}
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		slog.InfoContext(r.Context(), "edge request", "service", "edge", "request_id", requestID, "method", r.Method, "route", route, "status", recorder.status)
	})
}

func (s *Server) withRecovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recover() != nil {
				slog.ErrorContext(r.Context(), "edge handler panic", "service", "edge", "request_id", w.Header().Get("X-Request-ID"))
				writeError(w, http.StatusInternalServerError, "internal_error", "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
