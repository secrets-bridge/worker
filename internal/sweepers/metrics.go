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

// Gateway observation sweepers (worker#38). Counts only; the only
// label is the verdict reason, an enum.
var (
	observationLeasesExpired = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "worker_observation_task_leases_expired_total",
		Help: "Observation task leases the worker expired (task requeued, attempt closed as lease_expired).",
	})
	observationTasksTimedOut = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "worker_observation_tasks_timed_out_total",
		Help: "Observation tasks the worker closed as timed_out, by verdict reason.",
	}, []string{"reason"})
	gatewaysFlaggedStale = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "worker_gateways_stale_total",
		Help: "Gateways the worker flipped from active to stale after missed heartbeats.",
	})
	gatewayReplicasPruned = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "worker_gateway_replicas_pruned_total",
		Help: "gateway_replicas rows the worker deleted after the retention window.",
	})
)

// Collectors returns the sweepers' Prometheus collectors. main
// registers them on the probe server's registry next to the
// scheduler's.
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		notifyFailures, auditFailures,
		observationLeasesExpired, observationTasksTimedOut, gatewaysFlaggedStale, gatewayReplicasPruned,
	}
}
