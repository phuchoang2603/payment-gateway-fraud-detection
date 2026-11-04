package kserve

import (
	"context"
	"strconv"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdk "go.opentelemetry.io/otel/sdk/metric"
)

const benchFeatures, benchEdges = 50, 9

func benchLayout(version string) (served, []float32) {
	known := served{version: version, edges: make([][]float64, benchFeatures), options: make([][]metric.AddOption, benchFeatures)}
	values := make([]float32, benchFeatures)
	for index := range benchFeatures {
		name := "feature_" + strconv.Itoa(index)
		for edge := range benchEdges {
			known.edges[index] = append(known.edges[index], float64(edge+1))
		}
		for slot := 0; slot <= benchEdges+1; slot++ {
			label := strconv.Itoa(slot)
			if slot == benchEdges+1 {
				label = "m"
			}
			known.options[index] = append(known.options[index], metric.WithAttributes(attribute.String("feature", name), attribute.String("bin", label), attribute.String("model_version", version)))
		}
		values[index] = float32(index%benchEdges) + 0.5
	}
	return known, values
}

func BenchmarkCountingOneRequestOfFiftyFeatures(b *testing.B) {
	reader := sdk.NewManualReader()
	counter, err := sdk.NewMeterProvider(sdk.WithReader(reader)).Meter("bench").Int64Counter("fraud.model.feature_bin")
	if err != nil {
		b.Fatal(err)
	}
	client := &Client{config: Config{Bins: counter}}
	known, values := benchLayout("v4")
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		client.count(ctx, known, values)
	}
}

func BenchmarkCountingIsSkippedWithoutAProfile(b *testing.B) {
	client := &Client{config: Config{}}
	known, values := benchLayout("v4")
	known.edges = nil
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		client.count(ctx, known, values)
	}
}
