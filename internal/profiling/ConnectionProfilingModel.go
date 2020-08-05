package profiling

import (
	"bytes"
	"context"
	"fmt"
	v1 "github.io/Liqo/JobProfiler/api/v1"
	graph2 "github.io/Liqo/JobProfiler/internal/datastructure"
	"github.io/Liqo/JobProfiler/internal/system"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"log"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strconv"
	"strings"
	"time"
)

type ConnectionProfiling struct {
	graph                       *graph2.ConnectionGraph
	prometheus                  *system.PrometheusProvider
	crdClient                   client.Client
	backgroundRoutineUpdateTime int
}

func (cp *ConnectionProfiling) Init(provider *system.PrometheusProvider, crdClient client.Client) {
	cp.prometheus = provider
	cp.crdClient = crdClient
	cp.graph = graph2.InitConnectionGraph()

	secondsStr := os.Getenv("BACKGROUND_ROUTINE_UPDATE_TIME")
	nSec, err := strconv.Atoi(secondsStr)
	if err != nil {
		cp.backgroundRoutineUpdateTime = 5
	} else {
		cp.backgroundRoutineUpdateTime = nSec
	}

	go startConnectionUpdateRoutine(cp)
}

// This function is called every time there is a new pod scheduling request. The behaviour of the
// function is the following:
//  - checks if there are information of the current job in the connection graph
//  - the informations in the connection graph may be out of date, so it checks if they are still valid
//    (the informations are considered out of date if they're older than one day)
//  - if the informations are still valid the prediction is computed, instead if the informetions are not present
//    or if they are out of date an update routine is triggered and the function returns. Next time the function
//    will be called for the same pod the informations will be ready
func (cp *ConnectionProfiling) ComputeConnectionsPrediction(pod corev1.Pod, c chan string) {
	dataAvailable := true
	validTime := time.Now().AddDate(0, 0, -1)

	lastUpdate, err := cp.graph.GetJobUpdateTime(extractDeploymentFromPodName(pod.Name), pod.Namespace)
	if err == nil {
		// means there is already the corresponding job in the datastructure. Given that the job already exists
		// the time of the last update needs to be checked; only if the records in the datastructure are older than
		// 1 day they are updated

		if lastUpdate.Before(validTime) {
			// if last update is before the last valid date the record in the datastructure needs to be updated

			go updateConnectionGraph(cp, pod.Name, pod.Namespace)
			dataAvailable = false
			c <- "empty"
		}

	} else {
		// means that the job is not yet present in the datastructure so it needs to be added

		go updateConnectionGraph(cp, pod.Name, pod.Namespace)
		dataAvailable = false
		c <- "empty"
	}

	if dataAvailable {

		podLabels := createConnectionCRD(cp, pod.Name, pod.Namespace)
		c <- podLabels
	}

	return
}

func createConnectionCRD(cp *ConnectionProfiling, jobName string, jobNamespace string) string {
	var buffer bytes.Buffer // stores the labels to add to the pod

	// get the prediction for the given job
	connJobs, err := cp.graph.GetJobConnections(extractDeploymentFromPodName(jobName), jobNamespace)
	if err != nil {
		log.Print(err)
		return "empty"
	}

	// create the CRDs
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

			crdName := "connprofile-" + generateConnectionCRDName(extractDeploymentFromPodName(jobName), con.ConnectedTo, timeslot, jobNamespace)

			resInstance := v1.ConnectionProfile{
				ObjectMeta: metav1.ObjectMeta{
					Name:      crdName,
					Namespace: "profiling",
				},
				Spec: v1.ConnectionProfileSpec{
					Source_job:            extractDeploymentFromPodName(jobName),
					Source_namespace:      jobNamespace,
					Destination_job:       strings.Split(con.ConnectedTo, "{")[0],
					Destination_namespace: strings.Split(strings.Split(con.ConnectedTo, "{")[1], "}")[0], // !!modifica questa oscenita!!
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

	return buffer.String()
}

func startConnectionUpdateRoutine(cp *ConnectionProfiling) {
	for {
		if jobName, jobNamespace, err := cp.graph.GetLastUpdatedJob(); err == nil {
			updateConnectionGraph(cp, jobName, jobNamespace)
			createConnectionCRD(cp, jobName, jobNamespace)
		}

		currTime := time.Now()
		t := currTime.Add(time.Second * time.Duration(cp.backgroundRoutineUpdateTime)).Unix()

		time.Sleep(time.Duration(t-currTime.Unix()) * time.Second)
	}
}

func updateConnectionGraph(cp *ConnectionProfiling, jobName string, jobNamespace string) {
	recordsRequest, err := cp.prometheus.GetConnectionRecords(extractDeploymentFromPodName(jobName), jobNamespace, "request")
	if err != nil {
		log.Print(err)
		return
	}

	recordsResponse, err := cp.prometheus.GetConnectionRecords(extractDeploymentFromPodName(jobName), jobNamespace, "response")
	if err != nil {
		log.Print(err)
		return
	}

	cp.graph.InsertNewJob(extractDeploymentFromPodName(jobName), jobNamespace, mergeRecords(recordsRequest, recordsResponse))

	//log.Print(cp.graph.PrintGraph())
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
