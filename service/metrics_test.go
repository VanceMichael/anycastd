package service

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// TestMeasureCallIsolatesSeriesByName verifies that check_duration_seconds
// keeps one series per (service, check identity): repeated executions of one
// instance must never overwrite the sibling instance's series, and an
// unnamed legacy check keeps its kind as the check label.
func TestMeasureCallIsolatesSeriesByName(t *testing.T) {
	r := require.New(t)

	m := newMetrics()
	ctx := context.Background()

	ok := func(_ context.Context) error { return nil }
	fail := func(_ context.Context) error { return errors.New("boom") }

	r.NoError(m.MeasureCall(ctx, "edge", "ping-a", ok))
	r.NoError(m.MeasureCall(ctx, "edge", "ping-b", ok))
	// Failing and slow executions record only the instance's own series.
	r.Error(m.MeasureCall(ctx, "edge", "ping-a", fail))
	r.NoError(m.MeasureCall(ctx, "edge", "ping-b", ok))
	// Legacy unnamed single-instance configuration uses the kind label.
	r.NoError(m.MeasureCall(ctx, "edge", "icmp_ping", ok))

	count := testutil.CollectAndCount(m.checkDurationSeconds, "anycastd_check_duration_seconds")
	r.Equal(3, count, "three distinct (service, check) series expected")

	r.GreaterOrEqual(testutil.ToFloat64(m.checkDurationSeconds.WithLabelValues("edge", "ping-a")), 0.0)
	r.GreaterOrEqual(testutil.ToFloat64(m.checkDurationSeconds.WithLabelValues("edge", "ping-b")), 0.0)
	r.GreaterOrEqual(testutil.ToFloat64(m.checkDurationSeconds.WithLabelValues("edge", "icmp_ping")), 0.0)
}
