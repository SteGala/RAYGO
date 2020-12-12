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

	cpuThresholdStr := os.Getenv("CPU_THRESHOLD")
	cpuThreshold, err := strconv.ParseFloat(cpuThresholdStr, 64)
	if err != nil {
		cpuThreshold = 0.5
	}

	memoryThresholdStr := os.Getenv("MEMORY_THRESHOLD")
	memoryThreshold, err := strconv.ParseFloat(memoryThresholdStr, 64)
	if err != nil {
		memoryThreshold = 600.0
	}

	cpuLowerThresholdStr := os.Getenv("CPU_LOWER_THRESHOLD")
	cpuLowerThreshold, err := strconv.ParseFloat(cpuLowerThresholdStr, 64)
	if err != nil {
		cpuLowerThreshold = 0.05
	}

	memoryLowerThresholdStr := os.Getenv("MEMORY_LOWER_THRESHOLD")
	memoryLowerThreshold, err := strconv.ParseFloat(memoryLowerThresholdStr, 64)
	if err != nil {
		memoryLowerThreshold = 50.0
	}

	if res == system.Memory {
		rp.data = datastructure.InitMemoryModel(nTimeslots, memoryThreshold, memoryLowerThreshold)
	} else if res == system.CPU {
		rp.data = datastructure.InitCPUModel(nTimeslots, cpuThreshold, cpuLowerThreshold)
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
func (rp *ResourceProfiling) ComputePrediction(podName string, podNamespace string, c chan ResourceProfilingValue, schedulingTime time.Time) {
	dataAvailable := true
	//validTime := schedulingTime.AddDate(0, 0, -1)

	/*lastUpdate, err := rp.data.GetJobUpdateTime(extractDeploymentFromPodName(podName), podNamespace)
	if err == nil {
		// means there is already the corresponding job in the datastructure. Given that the job already exists
		// the time of the last update needs to be checked; only if the records in the datastructure are older than
		// 1 day they are updated

		if lastUpdate.Before(validTime) {
			// if last update is before the last valid date the record in the datastructure needs to be updated

			go rp.updateResourceModel(podName, podNamespace, schedulingTime)
			dataAvailable = false
			c <- ResourceProfilingValue{
				resourceType: system.None,
				value:        "",
				label:        "",
			}
		}

	} else {
		// means that the job is not yet present in the datastructure so it needs to be added

		go rp.updateResourceModel(podName, podNamespace, schedulingTime)
		dataAvailable = false
		c <- ResourceProfilingValue{
			resourceType: system.None,
			value:        "",
			label:        "",
		}
	}*/

	if dataAvailable {
		rp.updateResourceModel(podName, podNamespace, schedulingTime)

		rp.clientMutex.Lock()
		defer rp.clientMutex.Unlock()

		prediction, err := rp.data.GetJobPrediction(extractDeploymentFromPodName(podName), podNamespace, schedulingTime)
		if err != nil {
			log.Print(err)
			c <- ResourceProfilingValue{
				resourceType: system.None,
				value:        "",
				label:        "",
			}
			return
		}

		podLabel := "Hello world"

		switch rp.data.(type) {
		case *datastructure.MemoryModel:
			c <- ResourceProfilingValue{
				resourceType: system.Memory,
				value:        prediction,
				label:        podLabel,
			}
		case *datastructure.CPUModel:
			c <- ResourceProfilingValue{
				resourceType: system.CPU,
				value:        prediction,
				label:        podLabel,
			}
		default:
			c <- ResourceProfilingValue{
				resourceType: system.None,
				value:        "",
				label:        "",
			}
		}
	}

	return
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
