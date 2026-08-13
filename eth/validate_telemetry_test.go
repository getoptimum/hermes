package eth

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	promexp "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// The SDK's default histogram boundaries start at 0 and jump to 5 seconds, so a
// sub-millisecond measurement puts every observation in one bucket and no
// percentile survives. Exported through the real exporter so the layout is
// observed rather than assumed.
func TestValidationDurationHistogramResolvesSubMillisecond(t *testing.T) {
	reg := prometheus.NewRegistry()
	exp, err := promexp.New(promexp.WithRegisterer(reg), promexp.WithNamespace("hermes"))
	require.NoError(t, err)
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exp))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	ps := &PubSub{}
	require.NoError(t, ps.initValidationMetrics(mp.Meter("hermes")))

	// Structural validation measures ~90us in production; the signature checks add
	// low milliseconds, and a large block more again.
	for _, d := range []float64{0.00009, 0.00012, 0.0021, 0.0038, 0.045} {
		ps.meters.duration.Record(context.Background(), d)
	}

	families, err := reg.Gather()
	require.NoError(t, err)

	var counts []uint64
	for _, mf := range families {
		if !strings.Contains(mf.GetName(), "validation_duration_seconds") {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, b := range m.GetHistogram().GetBucket() {
				counts = append(counts, b.GetCumulativeCount())
			}
		}
	}
	require.NotEmpty(t, counts, "histogram was not exported")

	// Distinct cumulative counts mean the observations landed in different buckets.
	distinct := map[uint64]struct{}{}
	for _, c := range counts {
		distinct[c] = struct{}{}
	}
	assert.GreaterOrEqual(t, len(distinct), 5,
		"observations collapsed into too few buckets: %v", counts)
}
