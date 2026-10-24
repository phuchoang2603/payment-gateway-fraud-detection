package observe

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

func Counter(name, unit string) metric.Int64Counter {
	value, err := otel.Meter("payment-gateway").Int64Counter(name, metric.WithUnit(unit))
	if err != nil {
		panic(err)
	}
	return value
}

func Histogram(name, unit string) metric.Float64Histogram {
	value, err := otel.Meter("payment-gateway").Float64Histogram(name, metric.WithUnit(unit))
	if err != nil {
		panic(err)
	}
	return value
}

func Gauge(name, unit string) metric.Float64Gauge {
	value, err := otel.Meter("payment-gateway").Float64Gauge(name, metric.WithUnit(unit))
	if err != nil {
		panic(err)
	}
	return value
}

func Label(key, value string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String(key, value))
}

func Count(counter metric.Int64Counter, key, value string) {
	counter.Add(context.Background(), 1, Label(key, value))
}
