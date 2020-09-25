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

type CPUModel struct {
	jobs      map[string]*cpuInfo
	mutex     sync.Mutex
	timeslots int
}

type cpuInfo struct {
	jobInformation system.Job
	cpuPrediction  []float64
	lastUpdate     time.Time
}

func InitCPUModel(timeslots int) *CPUModel {
	return &CPUModel{
		jobs:      make(map[string]*cpuInfo),
		mutex:     sync.Mutex{},
		timeslots: timeslots,
	}
}

func (cp *CPUModel) InsertJob(jobName string, namespace string, records []system.ResourceRecord, schedulingTime time.Time) {

	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	job := cpuInfo{
		jobInformation: system.Job{
			Name:      jobName,
			Namespace: namespace,
		},
		cpuPrediction: make([]float64, cp.timeslots),
		lastUpdate:    schedulingTime,
	}

	computeCPUWeightedSignal(records, cp.timeslots)

	//peak := computePeakSignal(records, cp.timeslots)
	percentile := computeKPercentile(records, 98, cp.timeslots)

	job.cpuPrediction = percentile

	cp.jobs[jobName+"{"+namespace+"}"] = &job
}

func (cp *CPUModel) GetJobUpdateTime(jobName string, namespace string) (time.Time, error) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	if job, found := cp.jobs[jobName+"{"+namespace+"}"]; found {
		return job.lastUpdate, nil
	} else {
		return time.Now(), errors.New(fmt.Sprintf("Job %s does not exist", jobName))
	}
}

func (cp *CPUModel) GetLastUpdatedJob() (system.Job, error) {
	lastUpdate := time.Now()
	var jobName, jobNamespace string
	found := false

	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	for _, job := range cp.jobs {
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
		}, errors.New("the connection graph is empty")
	}
}

func computeCPUCorrectionConstant(i int) float64 {
	decayTime := 1140

	return math.Exp2(float64(-i / decayTime))
}

func computeCPUWeightedSignal(records []system.ResourceRecord, timeSlots int) {
	numRecords := make([]int, timeSlots)
	var podName string

	if len(records) > 0 {
		podName = records[0].PodInformation.Name
	}

	for _, record := range records {

		if record.PodInformation.Name != podName {
			podName = record.PodInformation.Name
			numRecords = make([]int, timeSlots)
		}

		id := generateTimeslotIndex(record.Date, timeSlots)

		record.Value *= computeCPUCorrectionConstant(numRecords[id])
		numRecords[id]++
	}
}

func (cp *CPUModel) GetJobPrediction(jobName string, namespace string, predictionTime time.Time) (string, error) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	if job, found := cp.jobs[jobName+"{"+namespace+"}"]; !found {
		return "", errors.New("The connectionJob " + jobName + " is not present in the connection datastructure")
	} else {

		id := generateTimeslotIndex(predictionTime, cp.timeslots)

		return fmt.Sprintf("%.3f\n", job.cpuPrediction[id]), nil
	}
}

func (cp *CPUModel) UpdateJob(records []system.ResourceRecord) {
	type result struct {
		podName       string
		podNamespace  string
		avgThrottling float64
	}

	var job system.Job
	var sum = 0.0
	var count = 0
	throttlingInfo := make([]result, 0, 5)

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

			throttlingInfo = append(throttlingInfo, result{
				podName:       strings.Join(split[:l], "-"),
				podNamespace:  job.Namespace,
				avgThrottling: avg,
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

	for _, t := range throttlingInfo {
		avg += t.avgThrottling
	}

	if len(throttlingInfo) > 0 {
		avg = avg / float64(len(throttlingInfo))
	}

	maxThreshold := avg + avg*0.4
	minThreshold := avg - avg*0.4

	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	for _, t := range throttlingInfo {
		key := t.podName + "{" + t.podNamespace + "}"

		_, found := cp.jobs[key]

		if t.avgThrottling > maxThreshold && found {
			currTime := time.Now()

			id := generateTimeslotIndex(currTime, cp.timeslots)

			cp.jobs[key].cpuPrediction[id] += cp.jobs[key].cpuPrediction[id] * 0.2
		}

		if t.avgThrottling < minThreshold && found {
			currTime := time.Now()

			id := generateTimeslotIndex(currTime, cp.timeslots)

			cp.jobs[key].cpuPrediction[id] -= cp.jobs[key].cpuPrediction[id] * 0.1
		}

		if found {
			cp.jobs[key].lastUpdate = time.Now()
		}
	}
}

func (cp *CPUModel) PrintModel() string {
	var buffer bytes.Buffer

	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	buffer.WriteString(" -- CPU MODEL -- \n")
	for _, j := range cp.jobs {
		buffer.WriteString(j.jobInformation.Name + "{" + j.jobInformation.Namespace + "}\n")
		buffer.WriteString(fmt.Sprintf("\tPrediction: %.5f\n", j.cpuPrediction))
	}

	return buffer.String()
}
