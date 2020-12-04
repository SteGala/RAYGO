package datastructure

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.io/Liqo/JobProfiler/internal/system"
)

type MemoryModel struct {
	jobs                     map[string]*memoryInfo
	mutex                    sync.Mutex
	timeslots                int
	memoryFailThreshold      float64
	memoryFailLowerThreshold float64
}

type memoryInfo struct {
	jobInformation   system.Job
	memoryPrediction []float64
	lastUpdate       time.Time
}

func InitMemoryModel(timeslots int, threshold float64, lowerThreshold float64) *MemoryModel {
	return &MemoryModel{
		jobs:                     make(map[string]*memoryInfo),
		mutex:                    sync.Mutex{},
		timeslots:                timeslots,
		memoryFailThreshold:      threshold,
		memoryFailLowerThreshold: lowerThreshold,
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
	decayTime := 1440 / timeslots

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
		prediction := job.memoryPrediction[id] + job.memoryPrediction[id]*0.5
		return fmt.Sprintf("%.0f", prediction), nil
	}
}

func (mm *MemoryModel) UpdateJob(records []system.ResourceRecord) {
	type result struct {
		podName      string
		podNamespace string
		avgFail      float64
	}

	var job system.Job
	var sum = 0.0
	var count = 0
	memFailInfo := make([]result, 0, 5)
	var maxThreshold float64
	var minThreshold float64

	if len(records) > 0 {
		job = records[0].PodInformation
	}

	for id, record := range records {
		if record.PodInformation.Name != job.Name || id == len(records)-1 {
			var avg = 0.0

			if count > 0 {
				avg = sum / float64(count)
			}

			split := strings.Split(job.Name, "-")
			l := len(split) - 1

			memFailInfo = append(memFailInfo, result{
				podName:      strings.Join(split[:l], "-"),
				podNamespace: job.Namespace,
				avgFail:      avg,
			})

			sum = 0.0
			count = 0
			job = record.PodInformation
		}

		count++
		sum += record.Value
	}

	var avg = 0.0

	for _, t := range memFailInfo {
		avg += t.avgFail
	}

	if len(memFailInfo) > 0 {
		avg = avg / float64(len(memFailInfo))
	} else {
		return
	}

	if len(memFailInfo) == 1 {
		maxThreshold = mm.memoryFailThreshold + mm.memoryFailThreshold*0.25
		minThreshold = mm.memoryFailThreshold - mm.memoryFailThreshold*0.25
	} else {
		maxThreshold = avg + avg*0.25
		minThreshold = avg - avg*0.25
	}

	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	for _, t := range memFailInfo {
		key := t.podName + "{" + t.podNamespace + "}"

		_, found := mm.jobs[key]

		if t.avgFail > maxThreshold && found && t.avgFail > mm.memoryFailLowerThreshold && mm.jobs[key].memoryPrediction != nil {
			currTime := time.Now()

			id := generateTimeslotIndex(currTime, mm.timeslots)

			mm.jobs[key].memoryPrediction[id] += mm.jobs[key].memoryPrediction[id] * computeResourceIncrease(t.avgFail, maxThreshold)
		}

		if t.avgFail < minThreshold && found && t.avgFail > mm.memoryFailLowerThreshold && mm.jobs[key].memoryPrediction != nil {
			currTime := time.Now()

			id := generateTimeslotIndex(currTime, mm.timeslots)

			mm.jobs[key].memoryPrediction[id] -= mm.jobs[key].memoryPrediction[id] * 0.15
		}

		if found {
			mm.jobs[key].lastUpdate = time.Now()
		}
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
