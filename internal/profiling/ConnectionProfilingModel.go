package profiling

import (
	"bytes"
	"context"
	"fmt"
	v1 "github.io/Liqo/JobProfiler/api/v1"
	graph2 "github.io/Liqo/JobProfiler/internal/datastructure"
	"github.io/Liqo/JobProfiler/internal/system"
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

// This function is called every time there is a new pod scheduling request. The behaviour of the
// function is the following:
//  - checks if there are information of the current job in the connection graph
//  - the informations in the connection graph may be out of date, so it checks if they are still valid
//    (the informations are considered out of date if they're older than one day)
//  - if the informations are still valid the prediction is computed, instead if the informetions are not present
//    or if they are out of date an update routine is triggered and the function returns. Next time the function
//    will be called for the same pod the informations will be ready
func (cp *ConnectionProfiling) ComputePrediction(podName string, podNamespace string, c chan string) {
	dataAvailable := true
	validTime := time.Now().AddDate(0, 0, -1)

	lastUpdate, err := cp.graph.GetJobUpdateTime(extractDeploymentFromPodName(podName), podNamespace)
	if err == nil {
		// means there is already the corresponding job in the datastructure. Given that the job already exists
		// the time of the last update needs to be checked; only if the records in the datastructure are older than
		// 1 day they are updated

		if lastUpdate.Before(validTime) {
			// if last update is before the last valid date the record in the datastructure needs to be updated

			go cp.updateConnectionGraph(podName, podNamespace)
			dataAvailable = false
			c <- "empty"
		}

	} else {
		// means that the job is not yet present in the datastructure so it needs to be added

		go cp.updateConnectionGraph(podName, podNamespace)
		dataAvailable = false
		c <- "empty"
	}

	if dataAvailable {

		podLabels := cp.createConnectionCRD(podName, podNamespace)
		c <- podLabels
	}

	return
}

func (cp *ConnectionProfiling) createConnectionCRD(jobName string, jobNamespace string) string {
	var buffer bytes.Buffer // stores the labels to add to the pod

	// get the prediction for the given job
	connJobs, err := cp.graph.GetJobConnections(extractDeploymentFromPodName(jobName), jobNamespace)
	if err != nil {
		log.Print(err)
		return "empty"
	}

	// create the CRDs
	for _, slot := range connJobs {

		for _, con := range slot {
			crdName := "connprofile-" + generateConnectionCRDName(extractDeploymentFromPodName(jobName), con.ConnectedTo.Name, jobNamespace)

			resInstance := &v1.ConnectionProfile{}
			err = cp.crdClient.Get(context.TODO(), client.ObjectKey{
				Namespace: "profiling",
				Name:      crdName}, resInstance)

			// populate the CR
			resInstance.Name = crdName
			resInstance.Namespace = "profiling"
			resInstance.Spec.Source_job = extractDeploymentFromPodName(jobName)
			resInstance.Spec.Source_namespace = jobNamespace
			resInstance.Spec.Destination_job = con.ConnectedTo.Name
			resInstance.Spec.Destination_namespace = con.ConnectedTo.Namespace
			resInstance.Spec.Bandwidth_requirement = fmt.Sprintf("%.2f", con.Bandwidth)
			resInstance.Spec.UpdateTime = time.Now().String()

			// The Get operation returns error if the resource is not present. If not present we create it,
			// if present we update it
			if err != nil {
				err = cp.crdClient.Create(context.TODO(), resInstance)
				if err != nil {
					log.Print(err)
				}
			} else {
				err = cp.crdClient.Update(context.TODO(), resInstance)
				if err != nil {
					log.Print(err)
				}
			}

			buffer.WriteString(crdName + "\n")
		}
	}

	return buffer.String()
}

func (cp *ConnectionProfiling) updateConnectionGraph(jobName string, jobNamespace string) {
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

	if records := mergeRecords(recordsRequest, recordsResponse); len(records) > 0 {
		log.Print(len(records))
		cp.graph.InsertNewJob(extractDeploymentFromPodName(jobName), jobNamespace, records)
	}
}

func (cp *ConnectionProfiling) GetJobConnections(job system.Job) ([]system.Job, error) {
	return cp.graph.FindSCC(extractDeploymentFromPodName(job.Name), job.Namespace)
}

func (cp *ConnectionProfiling) UpdatePrediction(connections []system.Job, connChan chan string) {

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
