package profiling

import (
	"bytes"
	"context"
	"fmt"
	v1 "github.io/SteGala/JobProfiler/api/v1"
	graph2 "github.io/SteGala/JobProfiler/src/datastructure"
	"github.io/SteGala/JobProfiler/src/system"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"log"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"
)

type ConnectionProfiling struct {
	graph      *graph2.ConnectionGraph
	prometheus *system.PrometheusProvider
	crdClient  client.Client
}

func (cp *ConnectionProfiling) Init(provider *system.PrometheusProvider, crdClient client.Client) {
	cp.prometheus = provider
	cp.crdClient = crdClient
	cp.graph = graph2.InitConnectionGraph()
}

// l'idea sarebbe che questa funzione viene chiamata ogni volta che si presenta una rischiesta di
// scheduling per un job. La funzione controlla nel modello se esiste gia una predizione per il job richiesto
// e se tale predizione non e' troppo vecchia. In particolare:
//  - se non esiste contatta il sistema di tracing e la crea
//  - se esiste e non e' vecchia non la aggiorna
//  - se esiste ma e' vecchia la tratta come se non esistesse e si comporta come nel primo caso
// In ogni caso ritorna la lista dei Job connessi a quello richiesto
func (cp *ConnectionProfiling) ComputeConnectionsPrediction(pod corev1.Pod, c chan string) {
	namespace := pod.Namespace
	dataAvailable := true
	validTime := time.Now().AddDate(0, 0, -1)

	lastUpdate, err := cp.graph.GetJobLastUpdate(extractDeploymentFromPodName(pod.Name), pod.Namespace)
	if err == nil {
		// means there is already the corresponding job in the datastructure. Given that the job already exists
		// the time of the last update needs to be checked; only if the records in the datastructure are older than
		// 1 day they are updated

		if lastUpdate.Before(validTime) {
			// if last update is before the last valid date the record in the datastructure needs to be updated

			go updateConnectionGraph(cp, pod)
			//log.Print("Information of pod " + pod.Name + " are out of date. Update routine triggered")
			dataAvailable = false
			c <- "empty"
		}

	} else {
		// means that the job is not yet present in the datastructure so it needs to be added
		//log.Print(err)
		go updateConnectionGraph(cp, pod)
		//log.Print("Information of pod " + pod.Name + " are not present in the model. Update routine triggered")
		dataAvailable = false
		c <- "empty"
	}

	if dataAvailable {

		var buffer bytes.Buffer

		connJobs, err := cp.graph.GetJobConnections(extractDeploymentFromPodName(pod.Name), pod.Namespace)
		if err != nil {
			log.Print(err)
			c <- "empty"
			return
		}

		for id, slot := range connJobs {
			var timeslot string

			if id == 0 {
				timeslot = "00:00-06:00"
			} else if id == 1 {
				timeslot = "06:00-12:00"
			} else if id == 2 {
				timeslot = "12:00-18:00"
			} else if id == 3 {
				timeslot = "18:00-24:00"
			}

			for _, con := range slot {
				buffer.WriteString(timeslot + " ")

				crdName := "connprofile-" + generateConnectionCRDName(pod.Name, con.ConnectedTo, timeslot, namespace)

				resInstance := v1.ConnectionProfile{
					ObjectMeta: metav1.ObjectMeta{
						Name:      crdName,
						Namespace: "profiling",
					},
					Spec: v1.ConnectionProfileSpec{
						Source_job:            extractDeploymentFromPodName(pod.Name),
						Destination_job:       con.ConnectedTo,
						Bandwidth_requirement: fmt.Sprintf("%.2f", con.Bandwidth),
						UpdateTime:            time.Now().String(),
						TimeSlot:              timeslot,
					},
				}

				err = cp.crdClient.Create(context.TODO(), &resInstance)
				if err != nil {
					log.Print(err)
				}

				buffer.WriteString(crdName + "\n")

			}

		}

		//log.Print(cp.graph.PrintGraph())
		//log.Print("Profiling of job " + pod.Name + " completed")
		c <- buffer.String()
	}

	return
}

func updateConnectionGraph(cp *ConnectionProfiling, pod corev1.Pod) {
	recordsRequest, err := cp.prometheus.GetConnectionRecords(extractDeploymentFromPodName(pod.Name), pod.Namespace, "request")
	if err != nil {
		log.Print(err)
		return
	}

	recordsResponse, err := cp.prometheus.GetConnectionRecords(extractDeploymentFromPodName(pod.Name), pod.Namespace, "response")
	if err != nil {
		log.Print(err)
		return
	}

	cp.graph.InsertNewJob(extractDeploymentFromPodName(pod.Name), pod.Namespace, mergeRecords(recordsRequest, recordsResponse))
}

func mergeRecords(recordsRequest []system.ConnectionRecord, recordsResponse []system.ConnectionRecord) []system.ConnectionRecord {
	ret := make([]system.ConnectionRecord, 0, len(recordsRequest)+len(recordsRequest)/5)

	for _, r := range recordsRequest {
		ret = append(ret, r)
	}

	diff := make([]system.ConnectionRecord, 0, 10)

	for _, r := range recordsResponse {
		found := false

		for _, r1 := range ret {
			if r.From == r1.From && r.To == r1.To && r.DstNamespace == r1.DstNamespace && r.Date == r1.Date {
				r1.Bandwidth += r.Bandwidth
				found = true
				break
			}
		}

		if !found {
			diff = append(diff, r)
		}
	}

	for _, r := range diff {
		ret = append(ret, r)
	}

	return ret
}
