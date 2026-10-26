package observe

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/stats"
)

func Init(ctx context.Context, service string) (func(), error) {
	if name := os.Getenv("OTEL_SERVICE_NAME"); name != "" {
		service = name
	}
	slog.SetDefault(slog.New(NewLogHandler(os.Stdout, service)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(error) { slog.Warn("telemetry export failed") }))
	enabled := func(key string) (bool, error) {
		v := os.Getenv(key)
		if v == "" {
			return false, nil
		}
		switch strings.ToLower(v) {
		case "true":
			return true, nil
		case "false":
			return false, nil
		default:
			return false, fmt.Errorf("%s must be a boolean", key)
		}
	}
	traces, err := enabled("TRACING_ENABLED")
	if err != nil {
		return nil, err
	}
	metrics, err := enabled("METRICS_ENABLED")
	if err != nil {
		return nil, err
	}
	tracesEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
	metricsEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
	for _, signal := range []struct {
		enabled       bool
		key, endpoint string
	}{
		{traces, "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", tracesEndpoint},
		{metrics, "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", metricsEndpoint},
	} {
		if !signal.enabled {
			continue
		}
		u, err := url.ParseRequestURI(signal.endpoint)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil {
			return nil, fmt.Errorf("enabled telemetry requires %s as an HTTP URL", signal.key)
		}
	}
	ratio := 0.1
	if v := os.Getenv("OTEL_TRACES_SAMPLER_ARG"); v != "" {
		ratio, err = strconv.ParseFloat(v, 64)
		if err != nil || !(ratio >= 0 && ratio <= 1) {
			return nil, errors.New("OTEL_TRACES_SAMPLER_ARG must be between zero and one")
		}
	}
	interval := 60 * time.Second
	if v := os.Getenv("OTEL_METRIC_EXPORT_INTERVAL"); v != "" {
		ms, err := strconv.Atoi(v)
		if err != nil || ms < 1000 || ms > 3600000 {
			return nil, errors.New("OTEL_METRIC_EXPORT_INTERVAL must be 1000..3600000 milliseconds")
		}
		interval = time.Duration(ms) * time.Millisecond
	}
	res, err := resource.New(ctx, resource.WithFromEnv(), resource.WithAttributes(attribute.String("service.name", service), attribute.String("service.instance.id", os.Getenv("POD_UID")), attribute.String("service.version", os.Getenv("SERVICE_REVISION"))))
	if err != nil {
		return nil, err
	}
	var shutdowns []func(context.Context) error
	shutdown := func() {
		for _, stop := range shutdowns {
			flush, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			if err := stop(flush); err != nil {
				slog.Warn("telemetry shutdown incomplete")
			}
			cancel()
		}
	}
	if traces {
		exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(tracesEndpoint), otlptracehttp.WithTimeout(2*time.Second))
		if err != nil {
			return nil, errors.New("initialize trace exporter")
		}
		provider := sdktrace.NewTracerProvider(sdktrace.WithResource(res), sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))), sdktrace.WithBatcher(exporter, sdktrace.WithMaxQueueSize(512), sdktrace.WithMaxExportBatchSize(128), sdktrace.WithExportTimeout(2*time.Second)))
		otel.SetTracerProvider(provider)
		shutdowns = append(shutdowns, provider.Shutdown)
	} else {
		provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.NeverSample())))
		otel.SetTracerProvider(provider)
		shutdowns = append(shutdowns, provider.Shutdown)
	}
	if metrics {
		exporter, err := otlpmetrichttp.New(ctx, otlpmetrichttp.WithEndpointURL(metricsEndpoint), otlpmetrichttp.WithTimeout(2*time.Second))
		if err != nil {
			shutdown()
			return nil, errors.New("initialize metric exporter")
		}
		provider := sdkmetric.NewMeterProvider(sdkmetric.WithResource(res), sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(interval), sdkmetric.WithTimeout(2*time.Second))))
		otel.SetMeterProvider(provider)
		shutdowns = append(shutdowns, provider.Shutdown)
	} else {
		otel.SetMeterProvider(sdkmetric.NewMeterProvider())
	}
	return shutdown, nil
}

func RPCFilter(info *stats.RPCTagInfo) bool {
	return !strings.HasPrefix(info.FullMethodName, "/grpc.health.v1.Health/")
}

func SerializeContext(ctx context.Context) *string {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	if carrier["traceparent"] == "" {
		return nil
	}
	value := "traceparent=" + carrier["traceparent"] + "\n"
	if state := carrier["tracestate"]; state != "" {
		value += "tracestate=" + strings.ReplaceAll(state, `\`, `\\`) + "\n"
	}
	return &value
}

func Extract(ctx context.Context, headers map[string]string) context.Context {
	return propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier(headers))
}

func RestoreContext(ctx context.Context, value *string) context.Context {
	headers := map[string]string{}
	if value != nil && len(*value) <= 1024 {
		for line := range strings.SplitSeq(*value, "\n") {
			key, val, ok := strings.Cut(line, "=")
			if ok && (key == "traceparent" || key == "tracestate") {
				headers[key] = strings.ReplaceAll(val, `\\`, `\`)
			}
		}
	}
	return Extract(ctx, headers)
}

func SpanIDs(ctx context.Context) (string, string) {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return "", ""
	}
	return sc.TraceID().String(), sc.SpanID().String()
}
