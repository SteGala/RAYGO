package datastructure

import (
	"bytes"
	"errors"
	"fmt"
	"github.io/Liqo/JobProfiler/internal/system"
	"math"
	"sync"
	"time"
)

type CPUModel struct {
	jobs  map[string]*cpuInfo
	mutex sync.Mutex
}

type cpuInfo struct {
	jobName       string
	jobNamespace  string
	cpuPrediction []float64
	lastUpdate    time.Time
}

func InitCPUModel() *CPUModel {
	return &CPUModel{
		jobs:  make(map[string]*cpuInfo),
		mutex: sync.Mutex{},
	}
}

func (cm *CPUModel) InsertNewJob(jobName string, namespace string, records []system.ResourceRecord) {

	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	job := cpuInfo{
		jobName:       jobName + "{" + namespace + "}",
		cpuPrediction: make([]float64, timeSlots),
		lastUpdate:    time.Now(),
	}

	computeCPUWeightedSignal(records)

	peak := computePeakSignal(records)
	//percentile := computeKPercentile(records, 98)

	job.cpuPrediction = peak

	cm.jobs[jobName+"{"+namespace+"}"] = &job
}

func (cm *CPUModel) GetJobLastUpdate(jobName string, namespace string) (time.Time, error) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	if job, found := cm.jobs[jobName+"{"+namespace+"}"]; found {
		return job.lastUpdate, nil
	} else {
		return time.Now(), errors.New(fmt.Sprintf("Job %s does not exist", jobName))
	}
}

func (cm *CPUModel) GetLastUpdatedJob() (string, string, error) {
	lastUpdate := time.Now()
	var jobName, jobNamespace string
	found := false

	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	for _, job := range cm.jobs {
		if job.lastUpdate.Before(lastUpdate) {
			lastUpdate = job.lastUpdate
			jobName = job.jobName
			jobNamespace = job.jobNamespace
			found = true
		}
	}

	if found {
		// The string appended to the jobName is there for compatibility reason. !!IMPROVE!!
		return jobName + "-xxxxxxx-xxxx", jobNamespace, nil
	} else {
		return "", "", errors.New("the connection graph is empty")
	}
}

func computeCPUCorrectionConstant(i int) float64 {
	decayTime := 1140

	return math.Exp2(float64(-i / decayTime))
}

func computeCPUWeightedSignal(records []system.ResourceRecord) {
	numRecords := make([]int, timeSlots)
	//finalPrediction := make([][]float64, timeSlots)
	for _, record := range records {

		if record.Date.Hour() >= 0 && record.Date.Hour() < 6 {
			record.Value *= computeCPUCorrectionConstant(numRecords[0])
			numRecords[0]++

		} else if record.Date.Hour() >= 6 && record.Date.Hour() < 12 {
			record.Value *= computeCPUCorrectionConstant(numRecords[1])
			numRecords[1]++

		} else if record.Date.Hour() >= 12 && record.Date.Hour() < 18 {
			record.Value *= computeCPUCorrectionConstant(numRecords[2])
			numRecords[2]++

		} else {
			record.Value *= computeCPUCorrectionConstant(numRecords[3])
			numRecords[3]++

		}
	}
}

func (cm *CPUModel) GetPrediction(jobName string, namespace string) (string, error) {
	if job, found := cm.jobs[jobName+"{"+namespace+"}"]; !found {
		return "", errors.New("The connectionJob " + jobName + " is not present in the connection datastructure")
	} else {
		var buff bytes.Buffer

		for _, val := range job.cpuPrediction {
			buff.WriteString(fmt.Sprintf("%.2f\n", val))

		}

		return buff.String(), nil
	}
}
