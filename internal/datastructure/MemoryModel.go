package datastructure

import (
	"errors"
	"fmt"
	"github.io/Liqo/JobProfiler/internal/system"
	"math"
	"sync"
	"time"
)

type MemoryModel struct {
	jobs  map[string]*memoryInfo
	mutex sync.Mutex
}

type memoryInfo struct {
	jobName          string
	jobNamespace     string
	memoryPrediction []float64
	lastUpdate       time.Time
}

func InitMemoryModel() *MemoryModel {
	return &MemoryModel{
		jobs:  make(map[string]*memoryInfo),
		mutex: sync.Mutex{},
	}
}

func (mm *MemoryModel) InsertNewJob(jobName string, namespace string, records []system.ResourceRecord) {

	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	job := memoryInfo{
		jobName:          jobName,
		jobNamespace:     namespace,
		memoryPrediction: make([]float64, timeSlots),
		lastUpdate:       time.Now(),
	}

	computeMemoryWeightedSignal(records)

	peak := computePeakSignal(records)
	//percentile := computeKPercentile(records, 98)

	job.memoryPrediction = peak

	mm.jobs[jobName+"{"+namespace+"}"] = &job
}

func (mm *MemoryModel) GetJobLastUpdate(jobName string, namespace string) (time.Time, error) {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	if job, found := mm.jobs[jobName+"{"+namespace+"}"]; found {
		return job.lastUpdate, nil
	} else {
		return time.Now(), errors.New(fmt.Sprintf("Job %s does not exist", jobName))
	}
}

func (mm *MemoryModel) GetLastUpdatedJob() (string, string, error) {
	lastUpdate := time.Now()
	var jobName, jobNamespace string
	found := false

	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	for _, job := range mm.jobs {
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

func computeMemoryCorrectionConstant(i int) float64 {
	decayTime := 1140

	return math.Exp2(float64(-i / decayTime))
}

func computeMemoryWeightedSignal(records []system.ResourceRecord) {
	numRecords := make([]int, timeSlots)
	//finalPrediction := make([][]float64, timeSlots)
	for _, record := range records {

		if record.Date.Hour() >= 0 && record.Date.Hour() < 6 {
			record.Value *= computeMemoryCorrectionConstant(numRecords[0])
			numRecords[0]++

		} else if record.Date.Hour() >= 6 && record.Date.Hour() < 12 {
			record.Value *= computeMemoryCorrectionConstant(numRecords[1])
			numRecords[1]++

		} else if record.Date.Hour() >= 12 && record.Date.Hour() < 18 {
			record.Value *= computeMemoryCorrectionConstant(numRecords[2])
			numRecords[2]++

		} else {
			record.Value *= computeMemoryCorrectionConstant(numRecords[3])
			numRecords[3]++

		}
	}
}

func (mm *MemoryModel) GetPrediction(jobName string, namespace string, predictionTime time.Time) (string, error) {
	if job, found := mm.jobs[jobName+"{"+namespace+"}"]; !found {
		return "", errors.New("The connectionJob " + jobName + " is not present in the connection datastructure")
	} else {

		if predictionTime.Hour() >= 0 && predictionTime.Hour() < 6 {
			return fmt.Sprintf("%.0f\n", job.memoryPrediction[0]), nil
		} else if predictionTime.Hour() >= 6 && predictionTime.Hour() < 12 {
			return fmt.Sprintf("%.0f\n", job.memoryPrediction[1]), nil
		} else if predictionTime.Hour() >= 12 && predictionTime.Hour() < 18 {
			return fmt.Sprintf("%.0f\n", job.memoryPrediction[2]), nil
		} else {
			return fmt.Sprintf("%.0f\n", job.memoryPrediction[3]), nil
		}
	}
}
