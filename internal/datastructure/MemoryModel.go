package datastructure

import (
	"bytes"
	"crownlabs.com/profiling/internal/system"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

type MemoryModel struct {
	jobs      map[string]*memoryInfo
	mutex     sync.Mutex
	timeslots int
}

type memoryInfo struct {
	jobInformation   system.Job
	memoryPrediction []float64
	lastUpdate       time.Time
}

func InitMemoryModel(timeslots int) *MemoryModel {
	return &MemoryModel{
		jobs:      make(map[string]*memoryInfo),
		mutex:     sync.Mutex{},
		timeslots: timeslots,
	}
}

func (mm *MemoryModel) InsertJob(jobName string, namespace string, records []system.ResourceRecord, schedulingTime time.Time) {

	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	job := memoryInfo{
		jobInformation: system.Job{
			Name:      jobName,
			Namespace: namespace,
		},
		memoryPrediction: make([]float64, mm.timeslots),
		lastUpdate:       schedulingTime,
	}

	computeMemoryWeightedSignal(records, mm.timeslots)

	peak := computeKPercentile(records, 100, mm.timeslots)

	if len(records) > 10 {
		job.memoryPrediction = peak
	} else {
		job.memoryPrediction = nil
	}

	mm.jobs[jobName+"{"+namespace+"}"] = &job
}

func (mm *MemoryModel) GetJobUpdateTime(jobName string, namespace string) (time.Time, error) {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	if job, found := mm.jobs[jobName+"{"+namespace+"}"]; found {
		return job.lastUpdate, nil
	} else {
		return time.Now(), errors.New(fmt.Sprintf("Job %s does not exist", jobName))
	}
}

func (mm *MemoryModel) GetLastUpdatedJob() (system.Job, error) {
	lastUpdate := time.Now()
	var jobName, jobNamespace string
	found := false

	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	for _, job := range mm.jobs {
		if job.lastUpdate.Before(lastUpdate) && job.jobInformation.Name != "" { //sometimes happens that emtpy job are added to the model !!INVESTIGATE!!
			lastUpdate = job.lastUpdate
			jobName = job.jobInformation.Name
			jobNamespace = job.jobInformation.Namespace
			found = true
		}
	}

	if found {
		// The string appended to the jobName is there for compatibility reason. !!IMPROVE!!
		return system.Job{
			Name:      jobName + "-xxxxxxx-xxxx",
			Namespace: jobNamespace,
		}, nil
	} else {
		return system.Job{
			Name:      "",
			Namespace: "",
		}, errors.New("the memory model is empty")
	}
}

func computeMemoryCorrectionConstant(i int, timeslots int) float64 {
	decayTime := 720 / timeslots

	return math.Exp2(float64(-i) / float64(decayTime))
}

func computeMemoryWeightedSignal(records []system.ResourceRecord, timeSlots int) {
	numRecords := make([]int, timeSlots)
	var podName string

	if len(records) > 0 {
		podName = records[len(records)-1].PodInformation.Name
	}

	for i := len(records) - 1; i >= 0; i-- {

		if records[i].PodInformation.Name != podName {
			podName = records[i].PodInformation.Name
			numRecords = make([]int, timeSlots)
		}

		id := generateTimeslotIndex(records[i].Date, timeSlots)
		records[i].Value *= computeMemoryCorrectionConstant(numRecords[id], timeSlots)
		numRecords[id]++
	}
}

func (mm *MemoryModel) GetJobPrediction(jobName string, namespace string, predictionTime time.Time) (string, error) {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	if job, found := mm.jobs[jobName+"{"+namespace+"}"]; !found {
		return "", errors.New("The connectionJob " + jobName + " is not present in the memory datastructure")
	} else if job.memoryPrediction == nil {
		return "", errors.New("Not enough informations in the memory model")
	} else {

		id := generateTimeslotIndex(predictionTime, mm.timeslots)
		//prediction := job.memoryPrediction[id] + job.memoryPrediction[id]*0.2
		prediction := job.memoryPrediction[id]
		return fmt.Sprintf("%.0f", prediction), nil
	}
}

func (mm *MemoryModel) PrintModel() string {
	var buffer bytes.Buffer

	buffer.WriteString(" -- MEMORY MODEL -- \n")
	for _, j := range mm.jobs {
		buffer.WriteString(j.jobInformation.Name + "{" + j.jobInformation.Namespace + "}\n")
		buffer.WriteString(fmt.Sprintf("\tPrediction: %.2f\n", j.memoryPrediction))
	}

	return buffer.String()
}
