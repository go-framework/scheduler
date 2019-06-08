package scheduler

import (
	"github.com/rcrowley/go-metrics"
)

// Schedule metrics.
type Metrics struct {
	// Count of Scheduler error.
	Errors metrics.Meter
	// TotalMetrics of Runner run.
	Total metrics.Meter
	// Count of Runner running.
	Runs metrics.Meter
	// Count of Runner run succeed.
	Successes metrics.Meter
	// Count of Runner run failed.
	Failures metrics.Meter
	// Count of Runner retrying.
	Retries metrics.Meter
}

// Get Meter of metrics type.
func (m Metrics) GetMetrics(key MetricsType) Meter {
	switch key {
	case ErrorsMetrics:
		return m.Errors
	case TotalMetrics:
		return m.Total
	case RunsMetrics:
		return m.Runs
	case SuccessesMetrics:
		return m.Successes
	case FailuresMetrics:
		return m.Failures
	case RetriesMetrics:
		return m.Retries
	default:
		return m.Runs
	}
}

// New schedule metrics.
func NewMetrics() *Metrics {
	m := &Metrics{}

	m.Errors = metrics.NewMeter()
	metrics.MustRegister("schedule.errors", m.Errors)

	m.Total = metrics.NewMeter()
	metrics.MustRegister("schedule.runner.total", m.Total)

	m.Runs = metrics.NewMeter()
	metrics.MustRegister("schedule.runner.runs", m.Runs)

	m.Successes = metrics.NewMeter()
	metrics.MustRegister("schedule.runner.successes", m.Successes)

	m.Failures = metrics.NewMeter()
	metrics.MustRegister("schedule.runner.failures", m.Failures)

	m.Retries = metrics.NewMeter()
	metrics.MustRegister("schedule.runner.retries", m.Retries)

	return m
}
