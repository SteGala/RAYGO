package profiling

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	webappv1 "github.io/Liqo/JobProfiler/api/v1"
	"github.io/Liqo/JobProfiler/internal/system"
	"gomodules.xyz/jsonpatch/v2"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"log"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"strings"
	"time"
	// +kubebuilder:scaffold:imports
)

type kubernetesProvider struct {
	client    *kubernetes.Clientset
	startTime time.Time
	config    *rest.Config
}

type ProfilingSystem struct {
	connection ConnectionProfiling
	memory     ResourceProfiling
	cpu        ResourceProfiling
	prometheus *system.PrometheusProvider
	client     *kubernetesProvider
	clientCRD  client.Client
}

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	_ = clientgoscheme.AddToScheme(scheme)

	_ = webappv1.AddToScheme(scheme)
	// +kubebuilder:scaffold:scheme
}

// Initialize the profiling system. Returns error if something unexpected happens in the init process.
func (p *ProfilingSystem) Init() error {
	var err error

	printInitialInformation()

	p.prometheus, p.client, err = runPreFlightCheck()
	if err != nil {
		return err
	}

	p.clientCRD, err = initKubernetesCRDClient()
	if err != nil {
		return err
	}
	log.Print("[CHECKED] Connection clientCRD created")

	p.connection.Init(p.prometheus, p.clientCRD)
	log.Print("[CHECKED] Connection graph initialized")

	p.memory.Init(p.prometheus, p.clientCRD, system.Memory)
	log.Print("[CHECKED] Memory model initialized")

	p.cpu.Init(p.prometheus, p.clientCRD, system.CPU)
	log.Print("[CHECKED] CPU model initialized")

	log.Print("Profiling setup completed")
	log.Print("")

	return nil
}

func printInitialInformation() {

	log.Print("--------------------------")
	log.Print("|      Job Profiler      |")
	log.Print("--------------------------")

	log.Print(" - Version: v0.1.2")
	log.Print(" - Author: Stefano Galantino")
	log.Println()
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

// StartProfile starts the profiling system. It watches for pod creations and triggers:
//  - ConnectionProfilingModel
//  - CPUProfilingModel
//  - MemoryProfilingModel
// to compute the profiling on each element. Each profiling is executed in a different thread
// and the execution is synchronized using channels
func (p *ProfilingSystem) StartProfile(namespace string) error {

	watch, err := p.client.client.CoreV1().Pods(namespace).Watch( /*context.TODO(), */ metav1.ListOptions{})
	if err != nil {
		return err
	}

	connChan := make(chan string)
	memChan := make(chan string)
	cpuChan := make(chan string)

	for event := range watch.ResultChan() {

		if len(event.Object.(*v1.Pod).Status.Conditions) == 0 {
			log.Print("Received scheduling request for pod: " + event.Object.(*v1.Pod).Name)

			go p.connection.ComputeConnectionsPrediction(*event.Object.(*v1.Pod), connChan)
			go p.memory.ComputePrediction(*event.Object.(*v1.Pod), memChan)
			go p.cpu.ComputePrediction(*event.Object.(*v1.Pod), cpuChan)

			connLabels := <-connChan
			memLabel := <-memChan
			cpuLabel := <-cpuChan

			if err := addPodLabels(p.client.client, connLabels, memLabel, cpuLabel, event.Object.(*v1.Pod)); err != nil {
				log.Print("Cannot add labels to pod " + event.Object.(*v1.Pod).Name)
				log.Print(err)
			}

			log.Print("Profiling of pod " + event.Object.(*v1.Pod).Name + " completed")
		}

	}

	return nil
}

func addPodLabels(c *kubernetes.Clientset, connectionLabels string, memoryLabel string, cpuLabel string, pod *v1.Pod) error {
	addLabel := false

	oJson, err := json.Marshal(pod)
	if err != nil {
		log.Fatalln(err)
		return err
	}

	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}

	// ------------------------------------------------------------------------------
	// IMPORTANT: if anything bad happens in the profiling of RAM, CPU or Connections
	// no label are returned. In these situations are returned strings containing
	// "empty" as value. This is why there is always the check [label != "empty"]
	// ((NEED TO IMPROVE!!))
	// ------------------------------------------------------------------------------

	// add labels for connections
	if connectionLabels != "empty" {
		addLabel = true

		for id, label := range strings.Split(connectionLabels, "\n") {
			if label == "" {
				continue
			}

			pod.Annotations["liqo.io/connectionProfile"+fmt.Sprintf("%d", id)] = label
		}
	}

	// add label for memory
	if memoryLabel != "empty" {
		addLabel = true

		pod.Annotations["liqo.io/memoryProfile"] = memoryLabel
	}

	// add label for cpu
	if memoryLabel != "empty" {
		addLabel = true

		pod.Annotations["liqo.io/cpuProfile"] = cpuLabel
	}

	// if there is at least one label to add, the request to the API server is created
	if addLabel {
		mJson, err := json.Marshal(pod)
		if err != nil {
			log.Fatalln(err)
			return err
		}

		patch, err := jsonpatch.CreatePatch(oJson, mJson)
		if err != nil {
			log.Fatalln(err)
			return err
		}

		pb, err := json.MarshalIndent(patch, "", "  ")
		if err != nil {
			log.Fatalln(err)
			return err
		}

		_, err = c.CoreV1().Pods(pod.Namespace).Patch(pod.Name, types.JSONPatchType, pb)
		if err != nil {
			return err
		}
	}

	return nil
}
