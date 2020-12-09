package profiling

import (
	"context"
	"encoding/json"
	"fmt"
	"k8s.io/apimachinery/pkg/runtime"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.io/Liqo/JobProfiler/internal/system"
	"gomodules.xyz/jsonpatch/v2"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

type kubernetesProvider struct {
	client    *kubernetes.Clientset
	startTime time.Time
	config    *rest.Config
}

type ProfilingSystem struct {
	connection                  ConnectionProfiling
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

// Initialize the profiling system. Returns error if something unexpected happens in the init process.
func (p *ProfilingSystem) Init() error {
	var err error

	p.printInitialInformation()

	p.readEnvironmentVariables()

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

	log.Print("--------------------------")
	log.Print("|      Job Profiler      |")
	log.Print("--------------------------")

	log.Print(" - Version: v0.1.5")
	log.Print(" - Author: Stefano Galantino")
	log.Println()
}

func (p *ProfilingSystem) readEnvironmentVariables() {
	secondsStr := os.Getenv("BACKGROUND_ROUTINE_UPDATE_TIME")
	if nSec, err := strconv.Atoi(secondsStr); err != nil {
		p.backgroundRoutineUpdateTime = 5
	} else {
		p.backgroundRoutineUpdateTime = nSec
	}

	enabled := os.Getenv("BACKGROUND_ROUTINE_ENABLED")
	if enabled == "TRUE" {
		p.backgroundRoutineEnabled = true
	} else {
		p.backgroundRoutineEnabled = false
	}

	enabled = os.Getenv("OPERATING_MODE")
	if enabled == "PROFILING_SCHEDULING" {
		p.enableDeploymentUpdate = true
	} else {
		p.enableDeploymentUpdate = false
	}
}

// StartProfiling starts the profiling system. It watches for pod creations and triggers:
//  - ConnectionProfilingModel
//  - CPUProfilingModel
//  - MemoryProfilingModel
// to compute the profiling on each element. Each profiling is executed in a different thread
// and the execution is synchronized using channels
func (p *ProfilingSystem) StartProfiling(namespace string) error {
	list := runtime.Object()

	p.clientCRD.List(context.TODO(), list, metav1.ListOptions{})

	/*watch, err := p.client.client.CoreV1().Pods(namespace).Watch(metav1.ListOptions{})
	if err != nil {
		return err
	}

	memChan := make(chan ResourceProfilingValue)
	cpuChan := make(chan ResourceProfilingValue)

	for event := range watch.ResultChan() {

		if len(event.Object.(*v1.Pod).Status.Conditions) == 0 {
			pod := event.Object.(*v1.Pod)
			log.Print(" - SCHEDULING -\tpod: " + pod.Name)

			schedulingTime := time.Now()

			go p.memory.ComputePrediction(pod.Name, pod.Namespace, memChan, schedulingTime)
			go p.cpu.ComputePrediction(pod.Name, pod.Namespace, cpuChan, schedulingTime)

			memLabel := <-memChan
			cpuLabel := <-cpuChan


		}
	}*/



	time.Sleep(time.Hour * time.Duration(24))

	return nil
}

func (p *ProfilingSystem) addPodLabels(connectionLabels string, memoryLabel ResourceProfilingValue, cpuLabel ResourceProfilingValue, pod *v1.Pod) error {
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
	if memoryLabel.resourceType != system.None {
		addLabel = true

		pod.Annotations["liqo.io/memoryProfile"] = memoryLabel.label
	}

	// add label for cpu
	if cpuLabel.resourceType != system.None {
		addLabel = true

		pod.Annotations["liqo.io/cpuProfile"] = cpuLabel.label
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

		p.clientMutex.Lock()
		defer p.clientMutex.Unlock()

		// add labels to the pod
		if _, err = p.client.client.CoreV1().Pods(pod.Namespace).Patch(pod.Name, types.JSONPatchType, pb); err != nil {
			return err
		}
	}

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
	return client.New(config.GetConfigOrDie(), client.Options{})
}
