package profiling

import (
	"encoding/json"
	"flag"
	"fmt"
	webappv1 "github.io/SteGala/JobProfiler/api/v1"
	"gomodules.xyz/jsonpatch/v2"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"os"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"strings"

	"github.com/pkg/errors"
	"github.io/SteGala/JobProfiler/controllers"
	"github.io/SteGala/JobProfiler/src/system"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"log"
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
	// cpu ResourceProfiling
	prometheus *system.PrometheusProvider
	client     *kubernetesProvider
	clientCRD  client.Client
}

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

// initialize the profiling system
func (p *ProfilingSystem) Init() error {
	var err error

	printInitialInformation()

	p.prometheus, p.client, err = runPreFlightCheck()
	if err != nil {
		return err
	}

	p.clientCRD, err = initKubernetesConnectionsCRDClient()
	if err != nil {
		return err
	}

	p.connection.Init(p.prometheus, p.clientCRD)
	p.memory.Init(p.prometheus, p.clientCRD, system.Memory)
	// p.cpu.Init(p.prometheus, CPU)

	log.Println("Profiling setup completed")

	return nil
}

func printInitialInformation() {

	log.Print("--------------------------")
	log.Print("|      Job Profiler      |")
	log.Print("--------------------------")

	log.Print(" - Version: v0.1.1")
	log.Print(" - Author: Stefano Galantino")
	log.Println()
}

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

func initKubernetesConnectionsCRDClient() (client.Client, error) {
	var metricsAddr string
	var enableLeaderElection bool
	flag.StringVar(&metricsAddr, "metrics-addr", ":8080", "The address the metric endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "enable-leader-election", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:             scheme,
		MetricsBindAddress: metricsAddr,
		Port:               9443,
		LeaderElection:     enableLeaderElection,
		LeaderElectionID:   "293de33f.liqo.io.connectionprofile",
	})

	if err != nil {
		return nil, err
	}

	if err = (&controllers.ConnectionProfileReconciler{
		Client: mgr.GetClient(),
		Log:    ctrl.Log.WithName("controllers").WithName("ConnectionProfile"),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "ConnectionProfile")
		os.Exit(1)
	}
	// +kubebuilder:scaffold:builder

	log.Print("[CHECKED] Connection clientCRD created")
	return mgr.GetClient(), nil
}

func init() {
	_ = clientgoscheme.AddToScheme(scheme)

	_ = webappv1.AddToScheme(scheme)
	// +kubebuilder:scaffold:scheme
}

// StartProfile starts the profiling system. It watches for pod creations and triggers:
//  - ConnectionProfilingModel
//  - CPUProfilingModel
//  - MemoryProfilingModel
// to compute the profiling on each element
func (p *ProfilingSystem) StartProfile(namespace string) error {

	watch, err := p.client.client.CoreV1().Pods(namespace).Watch( /*context.TODO(), */ metav1.ListOptions{})
	if err != nil {
		return err
	}

	connChan := make(chan string)
	memChan := make(chan string)
	// 	cpuChan := make(chan string)

	for event := range watch.ResultChan() {

		if len(event.Object.(*v1.Pod).Status.Conditions) == 0 {
			log.Print("Received scheduling request for pod: " + event.Object.(*v1.Pod).Name)

			go p.connection.ComputeConnectionsPrediction(*event.Object.(*v1.Pod), connChan)
			go p.memory.ComputePrediction(*event.Object.(*v1.Pod), memChan)
			// go p.cpu.ComputePrediction(*event.Object.(*v1.Pod), cpuChan)

			connLabels := <-connChan
			memLabel := <-memChan

			if err := addPodLabels(p.client.client, connLabels, memLabel, event.Object.(*v1.Pod)); err != nil {
				log.Print("Cannot add labels to pod " + event.Object.(*v1.Pod).Name)
				log.Print(err)
			}

			log.Print("Profiling of pod " + event.Object.(*v1.Pod).Name + " completed")
		}

	}

	return nil
}

func addPodLabels(c *kubernetes.Clientset, connectionLabels string, memoryLabel string, pod *v1.Pod) error {
	addLabel := false

	oJson, err := json.Marshal(pod)
	if err != nil {
		log.Fatalln(err)
		return err
	}

	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}

	// add labels for connections
	if connectionLabels != "empty" {
		addLabel = true

		for id, label := range strings.Split(connectionLabels, "\n") {
			if label == "" {
				continue
			}

			pod.Annotations["liqo.io/connectionProfile"+fmt.Sprintf("%d", id)] = strings.Split(label, " ")[1]
		}
	}

	// add label for memory
	if memoryLabel != "empty" {
		addLabel = true

		pod.Annotations["liqo.io/memoryProfile"] = memoryLabel
	}

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
