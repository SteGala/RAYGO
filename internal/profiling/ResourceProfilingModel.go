package profiling

import (
	"context"
	v1 "github.io/Liqo/JobProfiler/api/v1"
	"github.io/Liqo/JobProfiler/internal/datastructure"
	"github.io/Liqo/JobProfiler/internal/system"
	"k8s.io/apimachinery/pkg/runtime"
	"log"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strconv"
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

	timeslotStr := os.Getenv("TIMESLOTS")
	nTimeslots, err := strconv.Atoi(timeslotStr)
	if err != nil {
		nTimeslots = 4
	}

	if res == system.Memory {
		rp.data = datastructure.InitMemoryModel(nTimeslots)
	} else if res == system.CPU {
		rp.data = datastructure.InitCPUModel(nTimeslots)
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
func (rp *ResourceProfiling) ComputePrediction(podName string, podNamespace string, schedulingTime time.Time) {
	dataAvailable := true
	validTime := schedulingTime.Add(time.Minute * (-3))

	lastUpdate, err := rp.data.GetJobUpdateTime(extractDeploymentFromPodName(podName), podNamespace)
	if err == nil {
		// means there is already the corresponding job in the datastructure. Given that the job already exists
		// the time of the last update needs to be checked; only if the records in the datastructure are older than
		// 1 day they are updated

		if lastUpdate.Before(validTime) {
			// if last update is before the last valid date the record in the datastructure needs to be updated

			rp.updateResourceModel(podName, podNamespace, schedulingTime)
			dataAvailable = false
		}

	} else {
		// means that the job is not yet present in the datastructure so it needs to be added

		rp.updateResourceModel(podName, podNamespace, schedulingTime)
		dataAvailable = false
	}

	if dataAvailable {
		rp.clientMutex.Lock()
		defer rp.clientMutex.Unlock()

		prediction, err := rp.data.GetJobPrediction(extractDeploymentFromPodName(podName), podNamespace, schedulingTime)
		if err != nil {
			log.Print(err)
		}

		log.Print(prediction)
		//_ = rp.createResourceCRD(podName, podNamespace, prediction, schedulingTime)
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

	log.Print(rp.data.PrintModel())

	for _, job := range jobs {
		currTime := time.Now()

		prediction, err := rp.data.GetJobPrediction(job.Name, job.Namespace, currTime)
		if err != nil {
			log.Print(err)
			c <- "empty"
			return
		}

		rp.clientMutex.Lock()
		_ = rp.createResourceCRD(job.Name, job.Namespace, prediction, currTime)
		rp.clientMutex.Unlock()
	}

	c <- "finished"
}

func (rp *ResourceProfiling) createResourceCRD(jobName string, jobNamespace string, prediction string, currTime time.Time) string {
	var crdName string
	var myCR runtime.Object
	var err error

	switch rp.data.(type) {
	case *datastructure.MemoryModel:
		memSpec := v1.MemorySpec{
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
		myCR.(*v1.MemoryProfile).Spec.MemoryProfiling = memSpec

	case *datastructure.CPUModel:
		cpuSpec := v1.CPUSpec{
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
		myCR.(*v1.CPUProfile).Spec.MemoryProfiling = cpuSpec

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

func (rp *ResourceProfiling) updateResourceModel(jobName string, jobNamespace string, schedulingTime time.Time) {
	var records []system.ResourceRecord
	var err error

	switch rp.data.(type) {
	case *datastructure.MemoryModel:
		records, err = rp.prometheus.GetResourceRecords(extractDeploymentFromPodName(jobName), jobNamespace, system.Memory, schedulingTime)
	case *datastructure.CPUModel:
		records, err = rp.prometheus.GetResourceRecords(extractDeploymentFromPodName(jobName), jobNamespace, system.CPU, schedulingTime)
	default:
	}

	if err != nil {
		return
	}

	rp.data.InsertJob(extractDeploymentFromPodName(jobName), jobNamespace, records, schedulingTime)

	prediction, err := rp.data.GetJobPrediction(extractDeploymentFromPodName(jobName), jobNamespace, schedulingTime)
	log.Print(schedulingTime.String() + " " + prediction)
	//log.Print(rp.data.PrintModel())

	return
}

func (rp *ResourceProfiling) tuneResourceModel(jobs []system.Job) error {
	var records []system.ResourceRecord
	var err error

	switch rp.data.(type) {
	case *datastructure.MemoryModel:
		records, err = rp.prometheus.GetMemoryFailRecords(jobs)
	case *datastructure.CPUModel:
		records, err = rp.prometheus.GetCPUThrottlingRecords(jobs)
	default:
	}

	if err != nil {
		return err
	}

	rp.data.UpdateJob(records)

	return nil
}
