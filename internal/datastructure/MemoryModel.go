package datastructure

import (
	"bytes"
	"errors"
	"fmt"
	"github.io/Liqo/JobProfiler/internal/system"
	"math"
	"strings"
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

	peak := computePeakSignal(records, mm.timeslots)
	//percentile := computeKPercentile(records, 98)

	job.memoryPrediction = peak

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
		if job.lastUpdate.Before(lastUpdate) {
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

func computeMemoryCorrectionConstant(i int) float64 {
	decayTime := 1140

	return math.Exp2(float64(-i) / float64(decayTime))
}

func computeMemoryWeightedSignal(records []system.ResourceRecord, timeSlots int) {
	numRecords := make([]int, timeSlots)
	var podName string

	if len(records) > 0 {
		podName = records[0].PodInformation.Name
	}

	for i := len(records) - 1 ; i >= 0 ; i-- {

		if records[i].PodInformation.Name != podName {
			podName = records[i].PodInformation.Name
			numRecords = make([]int, timeSlots)
		}

		id := generateTimeslotIndex(records[i].Date, timeSlots)
		records[i].Value *= computeMemoryCorrectionConstant(numRecords[id])
		numRecords[id]++
	}
}

func (mm *MemoryModel) GetJobPrediction(jobName string, namespace string, predictionTime time.Time) (string, error) {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	if job, found := mm.jobs[jobName+"{"+namespace+"}"]; !found {
		return "", errors.New("The connectionJob " + jobName + " is not present in the connection datastructure")
	} else {

		id := generateTimeslotIndex(predictionTime, mm.timeslots)

		return fmt.Sprintf("%.0f\n", job.memoryPrediction[id]), nil
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

			//log.Print(job)
			//log.Print(avg)

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
	}

	maxThreshold := avg + avg*0.4
	minThreshold := avg - avg*0.4

	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	for _, t := range memFailInfo {
		key := t.podName + "{" + t.podNamespace + "}"

		_, found := mm.jobs[key]

		if t.avgFail > maxThreshold && found {
			currTime := time.Now()

			id := generateTimeslotIndex(currTime, mm.timeslots)

			mm.jobs[key].memoryPrediction[id] += mm.jobs[key].memoryPrediction[id] * 0.2
		}

		if t.avgFail < minThreshold && found {
			currTime := time.Now()

			id := generateTimeslotIndex(currTime, mm.timeslots)

			mm.jobs[key].memoryPrediction[id] -= mm.jobs[key].memoryPrediction[id] * 0.1
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
