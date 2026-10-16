package sweepers

import "github.com/prometheus/client_golang/prometheus"

// notifyFailures counts notifications a sweeper could not deliver.
// Labelled by sweeper only: nothing from the event or the error goes
// into a label.
var notifyFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "worker_sweeper_notify_failures_total",
	Help: "Sweeper notifications that failed to deliver, by sweeper.",
}, []string{"sweeper"})

// auditFailures counts worker audit events that could not be written.
var auditFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "worker_sweeper_audit_failures_total",
	Help: "Worker audit events that failed to write, by sweeper.",
}, []string{"sweeper"})

// Collectors returns the sweepers' Prometheus collectors. main
// registers them on the probe server's registry next to the
// scheduler's.
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{notifyFailures, auditFailures}
}
