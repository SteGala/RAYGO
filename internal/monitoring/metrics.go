package monitoring

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	cpuProfiling = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "profiling_cpu",
			Help: "",
		},
		[]string{"deployment_name", "deployment_namespace", "profiling_type"},
	)

	memoryProfiling = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "profiling_memory",
			Help: "",
		},
		[]string{"deployment_name", "deployment_namespace", "profiling_type"},
	)
)

func init() {
	// Register custom metrics with the global prometheus registry
	_ = metrics.Registry.Register(cpuProfiling)
	_ = metrics.Registry.Register(memoryProfiling)
}

func ExposeMemoryProfiling(depName string, depNamespace string, pType string, value float64) {
	memoryProfiling.WithLabelValues(depName, depNamespace, pType).Set(value)
}

func ExposeCPUProfiling(depName string, depNamespace string, pType string, value float64) {
	cpuProfiling.WithLabelValues(depName, depNamespace, pType).Set(value)
}
