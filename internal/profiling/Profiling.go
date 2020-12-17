package profiling

import (
	"context"
	crownlabsv1alpha1 "crownlabs.com/profiling/api/v1alpha1"
	"crownlabs.com/profiling/internal/system"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"log"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sync"
	"time"
)

type kubernetesProvider struct {
	client    *kubernetes.Clientset
	startTime time.Time
	config    *rest.Config
}

type ProfilingSystem struct {
	memory                      ResourceProfiling
	cpu                         ResourceProfiling
	prometheus                  *system.PrometheusProvider
	client                      *kubernetesProvider
	clientCRD                   client.Client
	clientMutex                 sync.Mutex
	backgroundRoutineUpdateTime int
	backgroundRoutineEnabled    bool
	enableDeploymentUpdate      bool
}

type UpdateType int

const (
	Scheduling UpdateType = 1
	Background UpdateType = 2
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	_ = clientgoscheme.AddToScheme(scheme)

	_ = crownlabsv1alpha1.AddToScheme(scheme)
	// +kubebuilder:scaffold:scheme
}

// Initialize the profiling system. Returns error if something unexpected happens in the init process.
func (p *ProfilingSystem) Init() error {
	var err error

	p.printInitialInformation()

	p.prometheus, p.client, err = runPreFlightCheck()
	if err != nil {
		return err
	}

	p.clientCRD, err = initKubernetesCRDClient()
	if err != nil {
		return err
	}
	log.Print("[CHECKED] ClientCRD created")

	p.memory.Init(p.prometheus, system.Memory)
	log.Print("[CHECKED] Memory model initialized")

	p.cpu.Init(p.prometheus, system.CPU)
	log.Print("[CHECKED] CPU model initialized")

	log.Print("Profiling setup completed")
	log.Print("")

	return nil
}

func (p *ProfilingSystem) printInitialInformation() {

	log.Print("-----------------------------------")
	log.Print("|      Job Profiler Crownlabs     |")
	log.Print("-----------------------------------")

	log.Print(" - Version: v0.1.6")
	log.Print(" - Author: Stefano Galantino")
	log.Println()
}

// StartProfiling starts the profiling system. It watches for pod creations and triggers:
//  - ConnectionProfilingModel
//  - CPUProfilingModel
//  - MemoryProfilingModel
// to compute the profiling on each element. Each profiling is executed in a different thread
// and the execution is synchronized using channels
func (p *ProfilingSystem) StartProfiling(namespace string) error {

	list := &crownlabsv1alpha1.LabTemplateList{}
	if err := p.clientCRD.List(context.TODO(), list); err != nil {
		return err
	}

	for _, template := range list.Items {

		memoryProfiling := p.memory.ComputePrediction(template.Name+"-xx-xx", template.Namespace, time.Now())
		cpuProfiling := p.cpu.ComputePrediction(template.Name+"-xx-xx", template.Namespace, time.Now())

		log.Print(template.Name + " {" + template.Namespace + "}")

		if memoryProfiling.resourceType == system.None {
			log.Printf("\tRAM: (not enough informations)")
		} else {
			log.Printf("\tRAM: %s", memoryProfiling.value)
		}

		if cpuProfiling.resourceType == system.None {
			log.Printf("\tCPU: (not enough informations)")
		} else {
			log.Printf("\tCPU: %s", cpuProfiling.value)
		}

		/*if memoryProfilingFloat, err := strconv.ParseFloat(memoryProfiling.value, 64); err != nil {
			log.Printf("\tRAM: %.3f \tGB (not enough informations)", memoryProfilingFloat/float64(1000000000))
		} else {
			log.Printf("\tRAM: %.3f \tGB", (memoryProfilingFloat+0.2*memoryProfilingFloat)/float64(1000000000))
		}

		if cpuProfilingFloat, err := strconv.ParseFloat(cpuProfiling.value, 64); err != nil {
			log.Printf("\tCPU: %.2f \tCPU CORE(S) (not enough informations)", cpuProfilingFloat)
		} else {
			log.Printf("\tCPU: %.2f \tCPU CORE(S)", cpuProfilingFloat+0.2*cpuProfilingFloat)
		}*/

	}

	time.Sleep(time.Hour * time.Duration(24))

	return nil
}

// This function:
//  - checks if there is an available instance of Prometheus
//  - checks if it's possible to create a Kubernetes client
func runPreFlightCheck() (*system.PrometheusProvider, *kubernetesProvider, error) {

	var prometheus system.PrometheusProvider
	var kubernetes *kubernetesProvider
	var err error

	err = prometheus.InitPrometheusSystem()
	if err != nil {
		return nil, nil, err
	}

	log.Print("[CHECKED] Connection to Prometheus established")

	kubernetes, err = initKubernetesClient()
	if err != nil {
		return nil, nil, err
	}

	log.Print("[CHECKED] Integration with Kubernetes established")

	return &prometheus, kubernetes, nil
}

func initKubernetesClient() (*kubernetesProvider, error) {
	//Set to in-cluster config.
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, errors.Wrap(err, "error building in cluster config")
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	provider := kubernetesProvider{
		client:    client,
		config:    config,
		startTime: time.Now(),
	}

	return &provider, nil
}

func initKubernetesCRDClient() (client.Client, error) {
	return client.New(config.GetConfigOrDie(), client.Options{
		Scheme: scheme,
	})
}
