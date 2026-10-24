package observe

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func isolate(t *testing.T) {
	t.Helper()
	errorHandler := otel.GetErrorHandler()
	logger, tracer, meter, propagator := slog.Default(), otel.GetTracerProvider(), otel.GetMeterProvider(), otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetErrorHandler(errorHandler)
		slog.SetDefault(logger)
		otel.SetTracerProvider(tracer)
		otel.SetMeterProvider(meter)
		otel.SetTextMapPropagator(propagator)
	})
	for _, key := range []string{"TRACING_ENABLED", "METRICS_ENABLED", "OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_TRACES_SAMPLER_ARG", "OTEL_METRIC_EXPORT_INTERVAL"} {
		t.Setenv(key, "")
	}
}

func TestConfigurationRejectsInvalidEnabledExport(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"TRACING_ENABLED", "invalid"}, {"OTEL_EXPORTER_OTLP_ENDPOINT", "http://agent:4317"}, {"OTEL_EXPORTER_OTLP_ENDPOINT", "agent:bad"}, {"TRACING_ENABLED", "true"}, {"METRICS_ENABLED", "true"}, {"OTEL_TRACES_SAMPLER_ARG", "NaN"}, {"OTEL_TRACES_SAMPLER_ARG", "1.1"}, {"OTEL_METRIC_EXPORT_INTERVAL", "0"},
	} {
		t.Run(tc.key+tc.value, func(t *testing.T) {
			isolate(t)
			t.Setenv(tc.key, tc.value)
			if tc.key == "OTEL_EXPORTER_OTLP_ENDPOINT" {
				t.Setenv("TRACING_ENABLED", "true")
			}
			if stop, err := Init(context.Background(), "unit"); err == nil {
				stop()
				t.Fatal("accepted invalid configuration")
			}
		})
	}
}

func TestExportDisabledAndUnreachableBackendShutdown(t *testing.T) {
	for _, enabled := range []string{"false", "true"} {
		t.Run(enabled, func(t *testing.T) {
			isolate(t)
			t.Setenv("TRACING_ENABLED", enabled)
			t.Setenv("METRICS_ENABLED", enabled)
			t.Setenv("OTEL_TRACES_SAMPLER_ARG", "1")
			if enabled == "true" {
				t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "127.0.0.1:1")
			}
			ctx, cancel := context.WithCancel(context.Background())
			stop, err := Init(ctx, "unit")
			if err != nil {
				cancel()
				t.Fatal(err)
			}
			active, span := otel.Tracer("unit").Start(ctx, "operation")
			Counter("unit.calls", "{call}").Add(active, 1)
			span.End()
			cancel()
			start := time.Now()
			stop()
			if time.Since(start) > 7*time.Second {
				t.Fatal("cancelled request context prevented bounded shutdown")
			}
		})
	}
}

func TestJSONLogsCorrelateRealSpansAndExcludeSecrets(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = provider.Shutdown(context.Background()) }()
	ctx, span := provider.Tracer("unit").Start(context.Background(), "request")
	defer span.End()
	var output bytes.Buffer
	logger := slog.New(NewLogHandler(&output, "fraud-service")).With("authorization", "bearer-SECRET")
	logger.Info("started")
	logger.With("worker", "delivery").WithGroup("details").InfoContext(ctx, "request", "card_number", "card-SECRET", "payload", map[string]string{"value": "body-SECRET"}, "safe", "ok", slog.Group("headers", slog.String("X-Unlisted", "header-SECRET")))
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("JSON records: %s", output.String())
	}
	var startup, request map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &startup); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(lines[1]), &request); err != nil {
		t.Fatal(err)
	}
	sc := span.SpanContext()
	if startup["trace_id"] != nil || request["trace_id"] != sc.TraceID().String() || request["span_id"] != sc.SpanID().String() || request["level"] != "info" || request["service"] != "fraud-service" {
		t.Fatalf("correlation fields: %s", output.String())
	}
	if strings.Contains(output.String(), "SECRET") {
		t.Fatal("sensitive log field escaped redaction")
	}
}

func TestPropertiesRoundTripAndMalformedContext(t *testing.T) {
	state, err := trace.ParseTraceState(`vendor=value\with\slashes`)
	if err != nil {
		t.Fatal(err)
	}
	parent := trace.NewSpanContext(trace.SpanContextConfig{TraceState: state, TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled})
	ctx := trace.ContextWithSpanContext(context.Background(), parent)
	value := SerializeContext(ctx)
	restored := trace.SpanContextFromContext(RestoreContext(context.Background(), value))
	if restored.TraceID() != parent.TraceID() || restored.SpanID() != parent.SpanID() || restored.TraceState().String() != state.String() || !restored.IsRemote() {
		t.Fatal("Properties lost remote parent")
	}
	for _, v := range []*string{nil, new("traceparent=broken\nbaggage=secret\n")} {
		if trace.SpanContextFromContext(RestoreContext(context.Background(), v)).IsValid() {
			t.Fatal("fabricated remote parent")
		}
	}
}
