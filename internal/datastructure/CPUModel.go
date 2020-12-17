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

	/*tmp := make([]float64, 10)
	for _, v := range records {
		if v.PodInformation.Name == "virt-launcher-tsr-lab2-alessio-sacco-1412-e8b2-vmi-brp66" {
			tmp = append(tmp, v.Value)
		}
	}
	log.Print(tmp)
	log.Print()
	log.Print()*/

	computeCPUWeightedSignal(records, cp.timeslots)

	/*tmp = make([]float64, 10)
	for _, v := range records {
		if v.PodInformation.Name == "virt-launcher-tsr-lab2-alessio-sacco-1412-e8b2-vmi-brp66" {
			tmp = append(tmp, v.Value)
		}
	}
	log.Print(tmp)*/

	//peak := computePeakSignal(records, cp.timeslots)
	percentile := computeKPercentile(records, 98, cp.timeslots)

	if len(records) > 10 {
		job.cpuPrediction = percentile
	} else {
		job.cpuPrediction = nil
	}

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
		}, errors.New("the connection graph is empty")
	}
}

func computeCPUCorrectionConstant(i int, timeslots int) float64 {
	decayTime := 7200

	return math.Exp2(float64(-i) / float64(decayTime))
}

func computeCPUWeightedSignal(records []system.ResourceRecord, timeSlots int) {
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

		records[i].Value *= computeCPUCorrectionConstant(numRecords[id], timeSlots)
		numRecords[id]++
	}
}

func (cp *CPUModel) GetJobPrediction(jobName string, namespace string, predictionTime time.Time) (string, error) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	if job, found := cp.jobs[jobName+"{"+namespace+"}"]; !found {
		return "", errors.New("The connectionJob " + jobName + " is not present in the cpu datastructure")
	} else if job.cpuPrediction == nil {
		return "", errors.New("Not enough informations in the cpu model")
	} else {
		return fmt.Sprintf("%v", job.cpuPrediction), nil
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
