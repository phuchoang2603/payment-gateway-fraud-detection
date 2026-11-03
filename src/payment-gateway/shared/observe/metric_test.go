package observe

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestSecondsHistogramSeparatesFastAndSlowRequests(t *testing.T) {
	isolate(t)
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(ctx) })
	otel.SetMeterProvider(provider)
	histogram := Histogram("test.duration", "s")
	histogram.Record(ctx, .02)
	histogram.Record(ctx, 2)
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &collected); err != nil {
		t.Fatal(err)
	}
	for _, scope := range collected.ScopeMetrics {
		for _, measurement := range scope.Metrics {
			if measurement.Name != "test.duration" {
				continue
			}
			point := measurement.Data.(metricdata.Histogram[float64]).DataPoints[0]
			var fast uint64
			for i, bound := range point.Bounds {
				fast += point.BucketCounts[i]
				if bound == 1 {
					if fast != 1 || point.Count != 2 {
						t.Fatalf("one-second boundary: fast=%d, total=%d", fast, point.Count)
					}
					return
				}
			}
		}
	}
	t.Fatal("missing one-second histogram boundary")
}
