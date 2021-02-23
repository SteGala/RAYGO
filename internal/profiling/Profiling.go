package profiling

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"sync"
	"time"

	crownlabsv1alpha1 "crownlabs.com/profiling/api/v1alpha2"
	"crownlabs.com/profiling/internal/system"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
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

type Value struct {
	time     int64  `json:"time"`
	CPUvalue string `json:"cpu_value"`
	RAMValue string `json:"ram_value"`
}

type VMInfo struct {
	vmName string  `json:"vm_name"`
	values []Value `json:"values"`
}

type ProfilingResults struct {
	testTime int64    `json:"test_time"`
	vmInfo   []VMInfo `json:"results"`
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

	startDate, _ := time.Parse("2006-01-02", "2020-11-02")
	currDate, _ := time.Parse("2006-01-02", "2021-01-24")
	result := ProfilingResults{
		testTime: currDate.Unix(),
		vmInfo:   make([]VMInfo, 0, 10),
	}

	list := &crownlabsv1alpha1.TemplateList{}
	if err := p.clientCRD.List(context.TODO(), list); err != nil {
		return err
	}

	log.Printf("Found %d Templates", len(list.Items))

	for _, template := range list.Items {

		log.Printf("Analyzing %s", template.Name)

		vmInfo := VMInfo{
			vmName: template.Name,
			values: make([]Value, 0, 5),
		}

		for d := startDate; d.After(currDate) == false; d = d.AddDate(0, 0, 1) {
			//log.Print(d)

			v := Value{
				time: d.Unix(),
			}

			memoryProfiling := p.memory.ComputePrediction(template.Name+"-xx-xx", template.Namespace, d)
			cpuProfiling := p.cpu.ComputePrediction(template.Name+"-xx-xx", template.Namespace, d)

			//log.Print(template.Name + " {" + template.Namespace + "}")

			if memoryProfiling.resourceType == system.None {
				//log.Printf("\tRAM: (not enough informations)")
				v.RAMValue = "NaN"
			} else {
				//log.Printf("\tRAM: %s", memoryProfiling.value)
				v.RAMValue = memoryProfiling.value
			}

			if cpuProfiling.resourceType == system.None {
				//log.Printf("\tCPU: (not enough informations)")
				v.CPUvalue = "NaN"
			} else {
				//log.Printf("\tCPU: %s", cpuProfiling.value)
				v.CPUvalue = cpuProfiling.value
			}

			vmInfo.values = append(vmInfo.values, v)
		}

		result.vmInfo = append(result.vmInfo, vmInfo)

		break
	}

	file, err := json.Marshal(result)
	if err != nil {
		log.Print(err)
	}

	err = ioutil.WriteFile("profiling_results.json", file, 0644)
	if err != nil {
		log.Print(err)
	}

	log.Print(result)

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
