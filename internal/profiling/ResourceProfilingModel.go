package profiling

import (
	"context"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	v1 "github.io/Liqo/JobProfiler/api/v1"
	"github.io/Liqo/JobProfiler/internal/datastructure"
	"github.io/Liqo/JobProfiler/internal/system"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ResourceProfiling struct {
	data        datastructure.ResourceModel
	prometheus  *system.PrometheusProvider
	crdClient   client.Client
	clientMutex sync.Mutex
}

type ResourceProfilingValue struct {
	resourceType system.ResourceType
	job          system.Job
	value        string
	label        string
}

type ResourceProfilingValues []ResourceProfilingValue

func (rp *ResourceProfiling) Init(provider *system.PrometheusProvider, crdClient client.Client, res system.ResourceType) {
	rp.prometheus = provider
	rp.crdClient = crdClient
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
	validTime := schedulingTime.AddDate(0, 0, -1)

	lastUpdate, err := rp.data.GetJobUpdateTime(extractDeploymentFromPodName(podName), podNamespace)
	if err == nil {
		// means there is already the corresponding job in the datastructure. Given that the job already exists
		// the time of the last update needs to be checked; only if the records in the datastructure are older than
		// 1 day they are updated

		if lastUpdate.Before(validTime) {
			// if last update is before the last valid date the record in the datastructure needs to be updated

			rp.updateResourceModel(podName, podNamespace, schedulingTime)
			//c <- ResourceProfilingValue{
			//	resourceType: system.None,
			//	value:        "",
			//	label:        "",
			//}
			//return
		}

	} else {
		// means that the job is not yet present in the datastructure so it needs to be added

		rp.updateResourceModel(podName, podNamespace, schedulingTime)
		/*		c <- ResourceProfilingValue{
					resourceType: system.None,
					value:        "",
					label:        "",
				}
				return

		*/
	}

	rp.clientMutex.Lock()
	defer rp.clientMutex.Unlock()

	prediction, err := rp.data.GetJobPrediction(extractDeploymentFromPodName(podName), podNamespace, schedulingTime)
	if err != nil {
		//log.Print(err)
		c <- ResourceProfilingValue{
			resourceType: system.None,
			value:        "",
			label:        "",
		}
		return
	}

	podLabel := rp.createResourceCRD(podName, podNamespace, prediction, schedulingTime)

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

	return
}

func (rp *ResourceProfiling) UpdatePrediction(jobs []system.Job, c chan ResourceProfilingValues, profilingTime time.Time) {
	result := make(ResourceProfilingValues, 0, 5)

	for _, job := range jobs {
		rp.updateResourceModel(job.Name, job.Namespace, profilingTime)

		//if err := rp.tuneResourceModel(job); err != nil {
		//	log.Print(err)
		//}
	}

	if err := rp.tuneResourceModel(jobs...); err != nil {
		log.Print(err)
	}

	for _, job := range jobs {
		rpv := ResourceProfilingValue{}

		rp.clientMutex.Lock()

		prediction, err := rp.data.GetJobPrediction(extractDeploymentFromPodName(job.Name), job.Namespace, profilingTime)
		if err != nil {
			rpv.value = ""
			rpv.label = ""
			rpv.resourceType = system.None
			rpv.job = job
		} else {
			crdName := rp.createResourceCRD(job.Name, job.Namespace, prediction, profilingTime)

			rpv.value = prediction
			rpv.label = crdName
			rpv.job = job

			switch rp.data.(type) {
			case *datastructure.MemoryModel:
				rpv.resourceType = system.Memory
			case *datastructure.CPUModel:
				rpv.resourceType = system.CPU
			default:
				rpv.resourceType = system.None
			}
		}

		result = append(result, rpv)
		rp.clientMutex.Unlock()
	}

	c <- result
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
		log.Print(err)
		return
	}

	rp.data.InsertJob(extractDeploymentFromPodName(jobName), jobNamespace, records, schedulingTime)
	return
}

func (rp *ResourceProfiling) tuneResourceModel(jobs ...system.Job) error {
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
