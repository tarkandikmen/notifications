package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// NewRegistry builds a fresh Prometheus registry pre-populated with the
// stdlib Go runtime and process collectors. The server exposes the registry
// at GET /metrics. Pipeline-specific metrics arrive in Phase 5.
//
// docs/phases/01-foundation.md §7.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return reg
}
