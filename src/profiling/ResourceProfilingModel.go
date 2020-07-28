package profiling

import (
	"context"
	v1 "github.io/SteGala/JobProfiler/api/v1"
	"github.io/SteGala/JobProfiler/src/datastructure"
	"github.io/SteGala/JobProfiler/src/system"
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

// l'idea sarebbe che questa funzione viene chiamata ogni volta che si presenta una rischiesta di
// scheduling per un job. La funzione controlla nel modello se esiste gia una predizione per il job richiesto
// e se tale predizione non e' troppo vecchia. In particolare:
//  - se non esiste contatta il sistema di tracing e la crea
//  - se esiste e non e' vecchia non la aggiorna
//  - se esiste ma e' vecchia la tratta come se non esistesse e si comporta come nel primo caso
// In ogni caso ritorna la lista dei Job connessi a quello richiesto
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
			//log.Print("Information of pod " + pod.Name + " are out of date. Update routine triggered")
			dataAvailable = false
			c <- "empty"
		}

	} else {
		// means that the job is not yet present in the datastructure so it needs to be added
		//log.Print(err)
		go updateResourceModel(rp, pod)
		//log.Print("Information of pod " + pod.Name + " are not present in the model. Update routine triggered")
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

			m := v1.MemorySpec{
				Timezone: timeslot,
				Value:    slot,
			}
			memorySpec = append(memorySpec, m)

		}

		time := time.Now().String()
		crdName := "memprofile-" + generateResourceCRDName(pod.Name, pod.Namespace, time)

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
