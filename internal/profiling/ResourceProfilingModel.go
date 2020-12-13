package profiling

import (
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"crownlabs.com/profiling/internal/datastructure"
	"crownlabs.com/profiling/internal/system"
)

type ResourceProfiling struct {
	data        datastructure.ResourceModel
	prometheus  *system.PrometheusProvider
	clientMutex sync.Mutex
}

type ResourceProfilingValue struct {
	resourceType system.ResourceType
	job          system.Job
	value        string
	label        string
}

type ResourceProfilingValues []ResourceProfilingValue

func (rp *ResourceProfiling) Init(provider *system.PrometheusProvider, res system.ResourceType) {
	rp.prometheus = provider
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
func (rp *ResourceProfiling) ComputePrediction(podName string, podNamespace string, schedulingTime time.Time) ResourceProfilingValue {

	rp.updateResourceModel(podName, podNamespace, schedulingTime)

	rp.clientMutex.Lock()
	defer rp.clientMutex.Unlock()

	prediction, err := rp.data.GetJobPrediction(extractDeploymentFromPodName(podName), podNamespace, schedulingTime)
	if err != nil {
		//log.Print(err)
		return ResourceProfilingValue{
			resourceType: system.None,
			value:        "",
			label:        "",
		}
	}

	switch rp.data.(type) {
	case *datastructure.MemoryModel:
		return ResourceProfilingValue{
			resourceType: system.Memory,
			value:        prediction,
			label:        "",
		}
	case *datastructure.CPUModel:
		return ResourceProfilingValue{
			resourceType: system.CPU,
			value:        prediction,
			label:        "",
		}
	default:
		return ResourceProfilingValue{
			resourceType: system.None,
			value:        "",
			label:        "",
		}
	}
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
		log.Print(err)
		return
	}

	rp.data.InsertJob(extractDeploymentFromPodName(jobName), jobNamespace, records, schedulingTime)
	return
}
