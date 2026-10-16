// Package probes serves /healthz /readyz /metrics on a loopback
// listener. Same shape as api's internal/handlers/probes.go so K8s
// manifests can use identical probe paths across services.
//
// /readyz is gated by a set of named check functions; each returning
// nil means "ready". The aggregate body lists every failing check so
// `kubectl describe pod` shows exactly which dependency is unhealthy.
package probes

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Timeouts for the probe listener. Kubelet and Prometheus are the only
// expected clients, so these only need to bound a slow or stalled
// connection (gosec G112). They match the api's Fiber server.
const (
	readHeaderTimeout = 5 * time.Second
	readTimeout       = 10 * time.Second
	writeTimeout      = 15 * time.Second
	idleTimeout       = 60 * time.Second
)

// CheckFunc returns nil if the dependency is healthy.
type CheckFunc func(ctx context.Context) error

// Server is the loopback probe server. Construct with New, then call
// AddReadinessCheck zero or more times, then ListenAndServe in a
// goroutine.
type Server struct {
	addr     string
	mu       sync.RWMutex
	checks   map[string]CheckFunc
	ready    atomic.Bool
	registry *prometheus.Registry
	srv      *http.Server // assigned by ListenAndServe; read by Shutdown
}

// New builds a probe server bound to addr. registry is the Prometheus
// registry whose collectors will be exposed at /metrics. Callers
// should register process / Go collectors on it before starting.
func New(addr string, registry *prometheus.Registry) *Server {
	if registry == nil {
		registry = prometheus.NewRegistry()
		registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
		registry.MustRegister(collectors.NewGoCollector())
	}
	return &Server{
		addr:     addr,
		checks:   map[string]CheckFunc{},
		registry: registry,
	}
}

// AddReadinessCheck registers a named readiness check. Calls before
// SetReady(true) are still observed — /readyz returns 503 until both
// SetReady is true AND every check returns nil.
func (s *Server) AddReadinessCheck(name string, fn CheckFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checks[name] = fn
}

// SetReady flips the high-water mark. /readyz still gates on the
// per-check fns; this lets the boot path defer "ready" until every
// component has finished its own initialization.
func (s *Server) SetReady(ready bool) { s.ready.Store(ready) }

// ListenAndServe blocks until the underlying http.Server returns.
func (s *Server) ListenAndServe() error {
	srv := newHTTPServer(s.addr, s.handler())
	s.srv = srv
	return srv.ListenAndServe()
}

func (s *Server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.healthz)
	mux.HandleFunc("/readyz", s.readyz)
	mux.Handle("/metrics", promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{}))
	return mux
}

func newHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
	}
}

// Shutdown gracefully stops the http.Server.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.srv == nil {
		return nil
	}
	return s.srv.Shutdown(ctx)
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if !s.ready.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"booting"}`))
		return
	}

	s.mu.RLock()
	failing := map[string]string{}
	for name, fn := range s.checks {
		if err := fn(r.Context()); err != nil {
			failing[name] = err.Error()
		}
	}
	s.mu.RUnlock()

	body := map[string]any{"status": "ready"}
	status := http.StatusOK
	if len(failing) > 0 {
		body["status"] = "not_ready"
		body["failing"] = failing
		status = http.StatusServiceUnavailable
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
