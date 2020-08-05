package profiling

import (
	"context"
	v1 "github.io/Liqo/JobProfiler/api/v1"
	"github.io/Liqo/JobProfiler/internal/datastructure"
	"github.io/Liqo/JobProfiler/internal/system"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"log"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strconv"
	"time"
)

type ResourceProfiling struct {
	data                        datastructure.ResourceModel
	prometheus                  *system.PrometheusProvider
	crdClient                   client.Client
	backgroundRoutineUpdateTime int
}

func (rp *ResourceProfiling) Init(provider *system.PrometheusProvider, crdClient client.Client, res system.ResourceType) {
	rp.prometheus = provider
	rp.crdClient = crdClient

	if res == system.Memory {
		rp.data = datastructure.InitMemoryModel()
	} else if res == system.CPU {
		rp.data = datastructure.InitCPUModel()
	}

	secondsStr := os.Getenv("BACKGROUND_ROUTINE_UPDATE_TIME")
	nSec, err := strconv.Atoi(secondsStr)
	if err != nil {
		rp.backgroundRoutineUpdateTime = 5
	} else {
		rp.backgroundRoutineUpdateTime = nSec
	}

	go startResourceUpdateRoutine(rp)

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

			go updateResourceModel(rp, pod.Name, pod.Namespace)
			dataAvailable = false
			c <- "empty"
		}

	} else {
		// means that the job is not yet present in the datastructure so it needs to be added

		go updateResourceModel(rp, pod.Name, pod.Namespace)
		dataAvailable = false
		c <- "empty"
	}

	if dataAvailable {

		podLabel := createResourceCRD(rp, pod.Name, pod.Namespace)

		c <- podLabel
	}

	return
}

func createResourceCRD(rp *ResourceProfiling, jobName string, jobNamespace string) string {
	currTime := time.Now()

	prediction, err := rp.data.GetPrediction(extractDeploymentFromPodName(jobName), jobNamespace, currTime)
	if err != nil {
		log.Print(err)
		return "empty"
	}

	var crdName string
	var myCR runtime.Object

	switch rp.data.(type) {
	case *datastructure.MemoryModel:
		memspec := v1.MemorySpec{
			UpdateTime: currTime.String(),
			Value:      prediction,
		}

		crdName = "memprofile-" + generateResourceCRDName(extractDeploymentFromPodName(jobName), jobNamespace)

		myCR = &v1.MemoryProfile{}

		// the error is checked outside the switch
		err = rp.crdClient.Get(context.TODO(), client.ObjectKey{
			Namespace: "profiling",
			Name:      crdName}, myCR)

		myCR.(*v1.MemoryProfile).Name = crdName
		myCR.(*v1.MemoryProfile).Namespace = "profiling"
		myCR.(*v1.MemoryProfile).Spec.MemoryProfiling = memspec

	case *datastructure.CPUModel:
		cpuspec := v1.CPUSpec{
			UpdateTime: currTime.String(),
			Value:      prediction,
		}

		crdName = "cpuprofile-" + generateResourceCRDName(extractDeploymentFromPodName(jobName), jobNamespace)

		myCR = &v1.CPUProfile{}

		// the error is checked outside the switch
		err = rp.crdClient.Get(context.TODO(), client.ObjectKey{
			Namespace: "profiling",
			Name:      crdName}, myCR)

		myCR.(*v1.CPUProfile).Name = crdName
		myCR.(*v1.CPUProfile).Namespace = "profiling"
		myCR.(*v1.CPUProfile).Spec.MemoryProfiling = cpuspec

	default:
		return "empty"
	}

	// The Get operation returns error if the resource is not present. If not present we create it,
	// if present we update it
	if err != nil {
		err = rp.crdClient.Create(context.TODO(), myCR)
		if err != nil {
			log.Print(err)
		}
	} else {
		err = rp.crdClient.Update(context.TODO(), myCR)
		if err != nil {
			log.Print(err)
		}
	}

	return crdName
}

func startResourceUpdateRoutine(rp *ResourceProfiling) {
	for {
		if jobName, jobNamespace, err := rp.data.GetLastUpdatedJob(); err == nil {
			updateResourceModel(rp, jobName, jobNamespace)
			log.Print("Updated " + createResourceCRD(rp, jobName, jobNamespace))
		}

		currTime := time.Now()
		t := currTime.Add(time.Second * time.Duration(rp.backgroundRoutineUpdateTime)).Unix()

		time.Sleep(time.Duration(t-currTime.Unix()) * time.Second)
	}
}

func updateResourceModel(rp *ResourceProfiling, jobName string, jobNamespace string) {
	var records []system.ResourceRecord
	var err error

	switch rp.data.(type) {
	case *datastructure.MemoryModel:
		records, err = rp.prometheus.GetResourceRecords(extractDeploymentFromPodName(jobName), jobNamespace, system.Memory)
	case *datastructure.CPUModel:
		records, err = rp.prometheus.GetResourceRecords(extractDeploymentFromPodName(jobName), jobNamespace, system.CPU)
	default:
	}

	if err != nil {
		log.Print(err)
		return
	}

	rp.data.InsertNewJob(extractDeploymentFromPodName(jobName), jobNamespace, records)
}
