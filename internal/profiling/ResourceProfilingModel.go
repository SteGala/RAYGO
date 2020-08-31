package profiling

import (
	"context"
	v1 "github.io/Liqo/JobProfiler/api/v1"
	"github.io/Liqo/JobProfiler/internal/datastructure"
	"github.io/Liqo/JobProfiler/internal/system"
	"k8s.io/apimachinery/pkg/runtime"
	"log"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sync"
	"time"
)

type ResourceProfiling struct {
	data        datastructure.ResourceModel
	prometheus  *system.PrometheusProvider
	crdClient   client.Client
	clientMutex sync.Mutex
}

func (rp *ResourceProfiling) Init(provider *system.PrometheusProvider, crdClient client.Client, res system.ResourceType) {
	rp.prometheus = provider
	rp.crdClient = crdClient
	rp.clientMutex = sync.Mutex{}

	if res == system.Memory {
		rp.data = datastructure.InitMemoryModel()
	} else if res == system.CPU {
		rp.data = datastructure.InitCPUModel()
	}
}

// This function is called every time there is a new pod scheduling request. The behaviour of the
// function is the following:
//  - checks if there are information of the current job in the connection graph
//  - the informations in the connection graph may be out of date, so it checks if they are still valid
//    (the informations are considered out of date if they're older than one day)
//  - if the informations are still valid the prediction is computed, instead if the informetions are not present
//    or if they are out of date an update routine is triggered and the function returns. Next time the function
//    will be called for the same pod the informations will be ready
func (rp *ResourceProfiling) ComputePrediction(podName string, podNamespace string, c chan string) {
	dataAvailable := true
	validTime := time.Now().AddDate(0, 0, -1)

	lastUpdate, err := rp.data.GetJobUpdateTime(extractDeploymentFromPodName(podName), podNamespace)
	if err == nil {
		// means there is already the corresponding job in the datastructure. Given that the job already exists
		// the time of the last update needs to be checked; only if the records in the datastructure are older than
		// 1 day they are updated

		if lastUpdate.Before(validTime) {
			// if last update is before the last valid date the record in the datastructure needs to be updated

			go rp.updateResourceModel(podName, podNamespace)
			dataAvailable = false
			c <- "empty"
		}

	} else {
		// means that the job is not yet present in the datastructure so it needs to be added

		go rp.updateResourceModel(podName, podNamespace)
		dataAvailable = false
		c <- "empty"
	}

	if dataAvailable {
		currTime := time.Now()

		rp.clientMutex.Lock()
		defer rp.clientMutex.Unlock()

		prediction, err := rp.data.GetJobPrediction(extractDeploymentFromPodName(podName), podNamespace, currTime)
		if err != nil {
			log.Print(err)
			c <- "empty"
		}

		podLabel := rp.createResourceCRD(podName, podNamespace, prediction, currTime)
		c <- podLabel
	}

	return
}

func (rp *ResourceProfiling) UpdatePrediction(jobs []system.Job, c chan string) {
	//for _, job := range jobs {
	//	if err := rp.updateResourceModel(job); err != nil {
	//		log.Print(err)
	//	}
	//}

	if err := rp.tuneResourceModel(jobs); err != nil {
		log.Print(err)
	}

	rp.clientMutex.Lock()
	defer rp.clientMutex.Unlock()

	for _, job := range jobs {
		currTime := time.Now()

		prediction, err := rp.data.GetJobPrediction(extractDeploymentFromPodName(job.Name), job.Namespace, currTime)
		if err != nil {
			log.Print(err)
			c <- "empty"
			return
		}

		podLabel := rp.createResourceCRD(job.Name, job.Namespace, prediction, currTime)
		c <- podLabel
	}
}

func (rp *ResourceProfiling) createResourceCRD(jobName string, jobNamespace string, prediction string, currTime time.Time) string {
	var crdName string
	var myCR runtime.Object
	var err error

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

func (rp *ResourceProfiling) updateResourceModel(jobName string, jobNamespace string) {
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
		return
	}

	rp.data.InsertJob(extractDeploymentFromPodName(jobName), jobNamespace, records)

	return
}

func (rp *ResourceProfiling) tuneResourceModel(jobs []system.Job) error {
	var records []system.ResourceRecord
	var err error

	log.Print("Get informations from Prometheus")
	switch rp.data.(type) {
	case *datastructure.MemoryModel:
		//records, err = rp.prometheus.GetResourceTuningRecords(extractDeploymentFromPodName(jobName), jobNamespace, system.Memory)
	case *datastructure.CPUModel:
		records, err = rp.prometheus.GetCPUThrottlingRecords(jobs)
	default:
	}

	if err != nil {
		return err
	}

	log.Print("Update model with informations")
	rp.data.UpdateJob(records)

	return nil
}
