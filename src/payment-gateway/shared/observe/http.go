package observe

import (
	"context"
	"io"
	"net/http"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type response struct {
	http.ResponseWriter
	status int
}

func (r *response) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *response) Write(value []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.ResponseWriter.Write(value)
}
func (r *response) Unwrap() http.ResponseWriter { return r.ResponseWriter }

func HTTPServer(mux *http.ServeMux, next http.Handler) http.Handler {
	duration := Histogram("http.server.request.duration", "s")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, route := mux.Handler(r)
		if route == "GET /health" || route == "GET /ready" {
			next.ServeHTTP(w, r)
			return
		}
		if route == "" {
			route = "unmatched"
		}
		method := r.Method
		switch method {
		case "GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS":
		default:
			method = "_OTHER"
		}
		attrs := []attribute.KeyValue{attribute.String("http.request.method", method), attribute.String("http.route", route)}
		ctx := propagation.TraceContext{}.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := otel.Tracer("payment-gateway/http").Start(ctx, route, trace.WithSpanKind(trace.SpanKindServer), trace.WithAttributes(attrs...))
		defer span.End()
		start := time.Now()
		writer := &response{ResponseWriter: w}
		next.ServeHTTP(writer, r.WithContext(ctx))
		code := writer.status
		if code == 0 {
			code = http.StatusOK
		}
		attrs = append(attrs, attribute.Int("http.response.status_code", code))
		span.SetAttributes(attrs...)
		if code >= 500 {
			span.SetStatus(codes.Error, "HTTP server error")
		}
		duration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(attrs...))
	})
}

type HTTPTransport struct{ Base http.RoundTripper }

type responseBody struct {
	io.ReadCloser
	finish func()
	once   sync.Once
}

func (b *responseBody) Read(value []byte) (int, error) {
	n, err := b.ReadCloser.Read(value)
	if err != nil {
		b.once.Do(b.finish)
	}
	return n, err
}

func (b *responseBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.finish)
	return err
}

func (t HTTPTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx, span := otel.Tracer("payment-gateway/http").Start(req.Context(), "POST webhook", trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attribute.String("http.request.method", "POST")))
	cloned := req.Clone(ctx)
	propagation.TraceContext{}.Inject(ctx, propagation.HeaderCarrier(cloned.Header))
	base := t.Base
	if base == nil {
		base = http.DefaultTransport
	}
	start := time.Now()
	resp, err := base.RoundTrip(cloned)
	attrs := []attribute.KeyValue{attribute.String("http.request.method", "POST")}
	if resp != nil {
		attrs = append(attrs, attribute.Int("http.response.status_code", resp.StatusCode))
	}
	if err != nil {
		span.SetStatus(codes.Error, "webhook transport failed")
		attrs = append(attrs, attribute.String("error.type", "transport"))
	} else if resp.StatusCode >= 400 {
		span.SetStatus(codes.Error, "webhook HTTP error")
	}
	span.SetAttributes(attrs...)
	finish := func() {
		Histogram("http.client.request.duration", "s").Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(attrs...))
		span.End()
	}
	if err != nil || resp.Body == nil {
		finish()
	} else {
		resp.Body = &responseBody{ReadCloser: resp.Body, finish: finish}
	}
	return resp, err
}

func StartConsumer(ctx context.Context, headers map[string]string) (context.Context, trace.Span) {
	return otel.Tracer("notifier").Start(Extract(ctx, headers), "payments.events.v1 process", trace.WithSpanKind(trace.SpanKindConsumer))
}
