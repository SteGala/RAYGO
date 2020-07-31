package profiling

import (
	"context"
	v1 "github.io/Liqo/JobProfiler/api/v1"
	"github.io/Liqo/JobProfiler/internal/datastructure"
	"github.io/Liqo/JobProfiler/internal/system"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"log"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strings"
	"time"
)

type ResourceProfiling struct {
	data       datastructure.ResourceModel
	prometheus *system.PrometheusProvider
	crdClient  client.Client
}

func (rp *ResourceProfiling) Init(provider *system.PrometheusProvider, crdClient client.Client, res system.ResourceType) {
	rp.prometheus = provider

	if res == system.Memory {
		rp.data = datastructure.InitMemoryModel()
	} else if res == system.CPU {
		rp.data = datastructure.InitCPUModel()
	}

	rp.crdClient = crdClient
}

// This function is called every time there is a new pod scheduling request. The behaviour of the
// function is the following:
//  - checks if there are information of the current job in the connection graph
//  - the informations in the connection graph may be out of date, so it checks if they are still valid
//    (the informations are considered out of date if they're older than one day)
//  - if the informations are still valid the prediction is computed, instead if the informetions are not present
//    or if they are out of date an update routine is triggered and the function returns. Next time the function
//    will be called for the same pod the informations will be ready
func (rp *ResourceProfiling) ComputePrediction(pod corev1.Pod, c chan string) {
	dataAvailable := true
	validTime := time.Now().AddDate(0, 0, -1)

	lastUpdate, err := rp.data.GetJobLastUpdate(extractDeploymentFromPodName(pod.Name), pod.Namespace)
	if err == nil {
		// means there is already the corresponding job in the datastructure. Given that the job already exists
		// the time of the last update needs to be checked; only if the records in the datastructure are older than
		// 1 day they are updated

		if lastUpdate.Before(validTime) {
			// if last update is before the last valid date the record in the datastructure needs to be updated

			go updateResourceModel(rp, pod)
			dataAvailable = false
			c <- "empty"
		}

	} else {
		// means that the job is not yet present in the datastructure so it needs to be added

		go updateResourceModel(rp, pod)
		dataAvailable = false
		c <- "empty"
	}

	if dataAvailable {

		prediction, err := rp.data.GetPrediction(extractDeploymentFromPodName(pod.Name), pod.Namespace)
		if err != nil {
			log.Print(err)
			c <- "empty"
			return
		}

		memorySpec := make([]v1.MemorySpec, 0, 1)
		cpuSpec := make([]v1.CPUSpec, 0, 1)

		for id, slot := range strings.Split(prediction, "\n") {
			var timeslot string

			if slot == "" {
				continue
			}

			if id == 0 {
				timeslot = "00:00-06:00"
			} else if id == 1 {
				timeslot = "06:00-12:00"
			} else if id == 2 {
				timeslot = "12:00-18:00"
			} else if id == 3 {
				timeslot = "18:00-24:00"
			}

			switch rp.data.(type) {
			case *datastructure.MemoryModel:
				m := v1.MemorySpec{
					Timezone: timeslot,
					Value:    slot,
				}
				memorySpec = append(memorySpec, m)

			case *datastructure.CPUModel:
				c := v1.CPUSpec{
					Timezone: timeslot,
					Value:    slot,
				}
				cpuSpec = append(cpuSpec, c)

			default:
			}

		}

		time := time.Now().String()
		var crdName string

		switch rp.data.(type) {
		case *datastructure.MemoryModel:
			crdName = "memprofile-" + generateResourceCRDName(pod.Name, pod.Namespace, time)

			resInstance := v1.MemoryProfile{
				ObjectMeta: metav1.ObjectMeta{
					Name:      crdName,
					Namespace: "profiling",
				},
				Spec: v1.MemoryProfileSpec{
					UpdateTime:      time,
					MemoryProfiling: memorySpec,
				},
			}

			err = rp.crdClient.Create(context.TODO(), &resInstance)
			if err != nil {
				log.Print(err)
			}
		case *datastructure.CPUModel:
			crdName = "cpuprofile-" + generateResourceCRDName(pod.Name, pod.Namespace, time)

			resInstance := v1.CPUProfile{
				ObjectMeta: metav1.ObjectMeta{
					Name:      crdName,
					Namespace: "profiling",
				},
				Spec: v1.CPUProfileSpec{
					UpdateTime:      time,
					MemoryProfiling: cpuSpec,
				},
			}

			err = rp.crdClient.Create(context.TODO(), &resInstance)
			if err != nil {
				log.Print(err)
			}
		default:
		}

		c <- crdName
	}

	return
}

func updateResourceModel(rp *ResourceProfiling, pod corev1.Pod) {
	var records []system.ResourceRecord
	var err error

	switch rp.data.(type) {
	case *datastructure.MemoryModel:
		records, err = rp.prometheus.GetResourceRecords(extractDeploymentFromPodName(pod.Name), pod.Namespace, system.Memory)
	case *datastructure.CPUModel:
		records, err = rp.prometheus.GetResourceRecords(extractDeploymentFromPodName(pod.Name), pod.Namespace, system.CPU)
	default:
	}

	if err != nil {
		log.Print(err)
		return
	}

	rp.data.InsertNewJob(extractDeploymentFromPodName(pod.Name), pod.Namespace, records)
}
