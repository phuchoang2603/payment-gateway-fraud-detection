package observe

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestHTTPUsesRouteAndPropagatesContextWithoutSensitiveAttributes(t *testing.T) {
	isolate(t)
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = provider.Shutdown(context.Background()) }()
	otel.SetTracerProvider(provider)
	mux := http.NewServeMux()
	var active trace.SpanContext
	mux.HandleFunc("GET /checkout/{session}", func(w http.ResponseWriter, r *http.Request) {
		active = trace.SpanContextFromContext(r.Context())
		if baggage.FromContext(r.Context()).Len() != 0 {
			t.Error("untrusted baggage extracted")
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	handler := HTTPServer(mux, mux)
	req := httptest.NewRequest(http.MethodGet, "http://edge/checkout/token-SECRET?credential=query-SECRET", nil)
	req.Header.Set("traceparent", "00-01000000000000000000000000000000-0200000000000000-01")
	req.Header.Set("baggage", "credential=baggage-SECRET")
	handler.ServeHTTP(httptest.NewRecorder(), req)
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "http://edge/health", nil))
	spans := recorder.Ended()
	if len(spans) != 1 || spans[0].Name() != "GET /checkout/{session}" || active.TraceID().String() != "01000000000000000000000000000000" {
		t.Fatalf("HTTP spans: %v", spans)
	}
	for _, attr := range spans[0].Attributes() {
		if strings.Contains(attr.Value.String(), "SECRET") {
			t.Fatal("sensitive request attribute captured")
		}
	}
}
