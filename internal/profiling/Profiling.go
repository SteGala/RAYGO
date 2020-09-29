package profiling

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	webappv1 "github.io/Liqo/JobProfiler/api/v1"
	"github.io/Liqo/JobProfiler/internal/system"
	"gomodules.xyz/jsonpatch/v2"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"log"
	"os"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"strconv"
	"sync"
	"time"
	// +kubebuilder:scaffold:imports
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
	log.Print("[CHECKED] Connection clientCRD created")

	p.connection.Init(p.prometheus, p.clientCRD)
	log.Print("[CHECKED] Connection graph initialized")

	p.memory.Init(p.prometheus, p.clientCRD, system.Memory)
	log.Print("[CHECKED] Memory model initialized")

	p.cpu.Init(p.prometheus, p.clientCRD, system.CPU)
	log.Print("[CHECKED] CPU model initialized")

	if p.backgroundRoutineEnabled {
		go p.ProfilingBackgroundUpdate()
		log.Print("[CHECKED] Background update routine created")
	}

	log.Print("Profiling setup completed")
	log.Print("")

	return nil
}

func (p *ProfilingSystem) printInitialInformation() {

	log.Print("--------------------------")
	log.Print("|      Job Profiler      |")
	log.Print("--------------------------")

	log.Print(" - Version: v0.1.3")
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

// StartProfiling starts the profiling system. It watches for pod creations and triggers:
//  - ConnectionProfilingModel
//  - CPUProfilingModel
//  - MemoryProfilingModel
// to compute the profiling on each element. Each profiling is executed in a different thread
// and the execution is synchronized using channels
func (p *ProfilingSystem) StartProfiling(namespace string) error {
	watch, err := p.client.client.CoreV1().Pods(namespace).Watch( /*context.TODO(), */ metav1.ListOptions{})
	if err != nil {
		return err
	}

	connChan := make(chan string)
	memChan := make(chan ResourceProfilingValue)
	cpuChan := make(chan ResourceProfilingValue)

	for event := range watch.ResultChan() {

		if len(event.Object.(*v1.Pod).Status.Conditions) == 0 {
			log.Print(" - SCHEDULING -\tpod: " + event.Object.(*v1.Pod).Name)

			schedulingTime := time.Now()

			go p.connection.ComputePrediction(event.Object.(*v1.Pod).Name, event.Object.(*v1.Pod).Namespace, connChan, schedulingTime)
			go p.memory.ComputePrediction(event.Object.(*v1.Pod).Name, event.Object.(*v1.Pod).Namespace, memChan, schedulingTime)
			go p.cpu.ComputePrediction(event.Object.(*v1.Pod).Name, event.Object.(*v1.Pod).Namespace, cpuChan, schedulingTime)

			connLabels := <-connChan
			memLabel := <-memChan
			cpuLabel := <-cpuChan

			if err := p.addPodLabels(connLabels, memLabel, cpuLabel, event.Object.(*v1.Pod)); err != nil {
				log.Print("Cannot add labels to pod " + event.Object.(*v1.Pod).Name)
				log.Print(err)
			}
		}
	}

	return nil
}

// ProfilingBackgroundUpdate should be performed in a background routine
func (p *ProfilingSystem) ProfilingBackgroundUpdate() {
	cpuChan := make(chan ResourceProfilingValues)
	memChan := make(chan ResourceProfilingValues)

	for {
		if job, err := p.memory.data.GetLastUpdatedJob(); err == nil {
			log.Print(" - BACKGROUND -\tpod: " + job.Name)

			profilingTime := time.Now()

			if jobConnections, err := p.connection.GetJobConnections(job, profilingTime); err == nil {
				go p.cpu.UpdatePrediction(jobConnections, cpuChan, profilingTime)
				go p.memory.UpdatePrediction(jobConnections, memChan, profilingTime)

				memValues := <-memChan
				cpuValues := <-cpuChan

				if len(memValues) == len(cpuValues) {
					for i := 0; i < len(memValues); i++ {
						if err := p.updateDeploymentSpec(memValues[i].job, memValues[i], cpuValues[i]); err != nil {
							log.Print(err)
						}
					}
				}

			} else {
				log.Print("Error " + err.Error())
			}
		}

		currTime := time.Now()
		t := currTime.Add(time.Second * time.Duration(p.backgroundRoutineUpdateTime)).Unix()

		time.Sleep(time.Duration(t-currTime.Unix()) * time.Second)
	}
}

func (p *ProfilingSystem) addPodLabels(connectionLabels string, memoryLabel ResourceProfilingValue, cpuLabel ResourceProfilingValue, pod *v1.Pod) error {
	addLabel := false
	var podRequest = make(map[v1.ResourceName]resource.Quantity)
	var podLimit = make(map[v1.ResourceName]resource.Quantity)

	//oJson, err := json.Marshal(pod)
	//if err != nil {
	//	log.Fatalln(err)
	//	return err
	//}

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
	//if connectionLabels != "empty" {
	//	addLabel = true
	//
	//	for id, label := range strings.Split(connectionLabels, "\n") {
	//		if label == "" {
	//			continue
	//		}
	//
	//		pod.Annotations["liqo.io/connectionProfile"+fmt.Sprintf("%d", id)] = label
	//	}
	//}

	// add label for memory
	if memoryLabel.resourceType != system.None {
		addLabel = true

		if s, err := strconv.ParseFloat(memoryLabel.value, 64); err == nil {
			s /= 1000000

			if s < 64 {
				s = 64
			}

			podRequest["memory"] = resource.MustParse(fmt.Sprintf("%.0f", s-0.5*s) + "Mi")
			podLimit["memory"] = resource.MustParse(fmt.Sprintf("%.0f", s) + "Mi")
		}

		pod.Annotations["liqo.io/memoryProfile"] = memoryLabel.label
	}

	// add label for cpu
	if cpuLabel.resourceType != system.None {
		addLabel = true

		if s, err := strconv.ParseFloat(cpuLabel.value, 64); err == nil {
			if s < 0.2 {
				s = 0.2
			}

			podRequest["cpu"] = resource.MustParse(fmt.Sprintf("%f", s-0.5*s))
			podLimit["cpu"] = resource.MustParse(fmt.Sprintf("%f", s))
		}

		pod.Annotations["liqo.io/cpuProfile"] = cpuLabel.label
	}

	//pod.Spec.Containers[0].Resources = v1.ResourceRequirements{
	//	Limits:   podLimit,
	//	Requests: podRequest,
	//}

	// if there is at least one label to add, the request to the API server is created
	if addLabel {
		//mJson, err := json.Marshal(pod)
		//if err != nil {
		//	log.Fatalln(err)
		//	return err
		//}

		//patch, err := jsonpatch.CreatePatch(oJson, mJson)
		//if err != nil {
		//	log.Fatalln(err)
		//	return err
		//}

		//pb, err := json.MarshalIndent(patch, "", "  ")
		//if err != nil {
		//	log.Fatalln(err)
		//	return err
		//}

		p.clientMutex.Lock()
		defer p.clientMutex.Unlock()

		//// add labels to the pod
		//if _, err = p.client.client.CoreV1().Pods(pod.Namespace).Patch(pod.Name, types.JSONPatchType, pb); err != nil {
		//	return err
		//}

		if deploymentList, err := p.client.client.AppsV1().Deployments(pod.Namespace).List(metav1.ListOptions{}); err == nil {
			for _, d := range deploymentList.Items {
				//log.Print(d.Name)
				if d.Name == extractDeploymentFromPodName(pod.Name) {

					memRequest := podRequest["memory"]
					memLimit := podLimit["memory"]
					cpuRequest := podRequest["cpu"]
					cpuLimit := podLimit["cpu"]

					oJson, err := json.Marshal(d)
					if err != nil {
						log.Fatalln(err)
						return err
					}

					if d.Spec.Template.Spec.Containers[0].Resources.Requests.Memory().Value() > int64(float64(memRequest.Value())+0.15*float64(memRequest.Value())) ||
						d.Spec.Template.Spec.Containers[0].Resources.Requests.Memory().Value() < int64(float64(memRequest.Value())-0.15*float64(memRequest.Value())) ||
						d.Spec.Template.Spec.Containers[0].Resources.Limits.Memory().Value() > int64(float64(memLimit.Value())+0.15*float64(memLimit.Value())) ||
						d.Spec.Template.Spec.Containers[0].Resources.Limits.Memory().Value() < int64(float64(memLimit.Value())-0.15*float64(memLimit.Value())) ||
						d.Spec.Template.Spec.Containers[0].Resources.Requests.Cpu().Value() > int64(float64(cpuRequest.Value())+0.15*float64(cpuRequest.Value())) ||
						d.Spec.Template.Spec.Containers[0].Resources.Requests.Cpu().Value() < int64(float64(cpuRequest.Value())-0.15*float64(cpuRequest.Value())) ||
						d.Spec.Template.Spec.Containers[0].Resources.Limits.Cpu().Value() > int64(float64(cpuLimit.Value())+0.15*float64(cpuLimit.Value())) ||
						d.Spec.Template.Spec.Containers[0].Resources.Limits.Cpu().Value() < int64(float64(cpuLimit.Value())-0.15*float64(cpuLimit.Value())) {

						log.Print("Scheduling -> patch " + d.Name)
						d.Spec.Template.Spec.Containers[0].Resources = v1.ResourceRequirements{
							Limits:   podLimit,
							Requests: podRequest,
						}

						mJson, err := json.Marshal(d)
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

						if _, err = p.client.client.AppsV1().Deployments(pod.Namespace).Patch(d.Name, types.JSONPatchType, pb); err != nil {
							return err
						}
					} else {
						break
					}
				}
			}
		} else {
			return err
		}
	}

	return nil
}

func (p *ProfilingSystem) updateDeploymentSpec(job system.Job, memoryLabel ResourceProfilingValue, cpuLabel ResourceProfilingValue) error {
	var podRequest = make(map[v1.ResourceName]resource.Quantity)
	var podLimit = make(map[v1.ResourceName]resource.Quantity)

	// add label for memory
	if memoryLabel.resourceType != system.None {
		if s, err := strconv.ParseFloat(memoryLabel.value, 64); err == nil {
			s /= 1000000

			if s < 64 {
				s = 64
			}

			podRequest["memory"] = resource.MustParse(fmt.Sprintf("%.0f", s-0.5*s) + "Mi")
			podLimit["memory"] = resource.MustParse(fmt.Sprintf("%.0f", s) + "Mi")
		}
	}

	// add label for cpu
	if cpuLabel.resourceType != system.None {
		if s, err := strconv.ParseFloat(cpuLabel.value, 64); err == nil {
			if s < 0.5 {
				s = 0.5
			}

			podRequest["cpu"] = resource.MustParse(fmt.Sprintf("%f", s-0.5*s))
			podLimit["cpu"] = resource.MustParse(fmt.Sprintf("%f", s))
		}
	}

	p.clientMutex.Lock()
	defer p.clientMutex.Unlock()

	if deploymentList, err := p.client.client.AppsV1().Deployments(job.Namespace).List(metav1.ListOptions{}); err == nil {
		for _, d := range deploymentList.Items {
			if d.Name == job.Name {

				memRequest := podRequest["memory"]
				memLimit := podLimit["memory"]
				cpuRequest := podRequest["cpu"]
				cpuLimit := podLimit["cpu"]

				oJson, err := json.Marshal(d)
				if err != nil {
					log.Fatalln(err)
					return err
				}

				if d.Spec.Template.Spec.Containers[0].Resources.Requests.Memory().Value() > int64(float64(memRequest.Value())+0.15*float64(memRequest.Value())) ||
					d.Spec.Template.Spec.Containers[0].Resources.Requests.Memory().Value() < int64(float64(memRequest.Value())-0.15*float64(memRequest.Value())) ||
					d.Spec.Template.Spec.Containers[0].Resources.Limits.Memory().Value() > int64(float64(memLimit.Value())+0.15*float64(memLimit.Value())) ||
					d.Spec.Template.Spec.Containers[0].Resources.Limits.Memory().Value() < int64(float64(memLimit.Value())-0.15*float64(memLimit.Value())) ||
					d.Spec.Template.Spec.Containers[0].Resources.Requests.Cpu().Value() > int64(float64(cpuRequest.Value())+0.15*float64(cpuRequest.Value())) ||
					d.Spec.Template.Spec.Containers[0].Resources.Requests.Cpu().Value() < int64(float64(cpuRequest.Value())-0.15*float64(cpuRequest.Value())) ||
					d.Spec.Template.Spec.Containers[0].Resources.Limits.Cpu().Value() > int64(float64(cpuLimit.Value())+0.15*float64(cpuLimit.Value())) ||
					d.Spec.Template.Spec.Containers[0].Resources.Limits.Cpu().Value() < int64(float64(cpuLimit.Value())-0.15*float64(cpuLimit.Value())) {

					log.Print("Background -> patch " + d.Name)
					d.Spec.Template.Spec.Containers[0].Resources = v1.ResourceRequirements{
						Limits:   podLimit,
						Requests: podRequest,
					}

					mJson, err := json.Marshal(d)
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

					if _, err = p.client.client.AppsV1().Deployments(job.Namespace).Patch(d.Name, types.JSONPatchType, pb); err != nil {
						return err
					}

					break
				} else {
					break
				}

			}
		}
	} else {
		return err
	}

	return nil
}
