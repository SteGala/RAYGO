package monitoring

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
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
	prometheus.MustRegister(cpuProfiling)
	prometheus.MustRegister(memoryProfiling)

	go func() {

		http.Handle("/metrics", promhttp.HandlerFor(
			prometheus.DefaultGatherer,
			promhttp.HandlerOpts{
				// Opt into OpenMetrics to support exemplars.
				EnableOpenMetrics: true,
			},
		))

		// There may be multiple calls to the init function, the first one will succeed in the port binding
		// the following ones will fail. One HTTP handler will always be available
		_ = http.ListenAndServe(":8090", nil)
	}()
}

func ExposeMemoryProfiling(depName string, depNamespace string, pType string, value float64) {
	memoryProfiling.WithLabelValues(depName, depNamespace, pType).Set(value)
}

func ExposeCPUProfiling(depName string, depNamespace string, pType string, value float64) {
	cpuProfiling.WithLabelValues(depName, depNamespace, pType).Set(value)
}
