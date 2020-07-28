package datastructure

import (
	"bytes"
	"errors"
	"fmt"
	"github.io/SteGala/JobProfiler/src/system"
	"math"
	"sync"
	"time"
)

type MemoryModel struct {
	jobs  map[string]*memoryInfo
	mutex sync.Mutex
}

type memoryInfo struct {
	name             string
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
		name:             jobName + "{" + namespace + "}",
		memoryPrediction: make([]float64, timeSlots),
		lastUpdate:       time.Now(),
	}

	computeMemoryWeightedSignal(records)

	peak := computePeakSignal(records)
	//percentile := computeKPercentile(records, 98)

	job.memoryPrediction = peak

	//log.Print(peak)
	//log.Print(percentile)

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

func (mm *MemoryModel) GetPrediction(jobName string, namespace string) (string, error) {
	if job, found := mm.jobs[jobName+"{"+namespace+"}"]; !found {
		return "", errors.New("The connectionJob " + jobName + " is not present in the connection datastructure")
	} else {
		var buff bytes.Buffer

		for _, val := range job.memoryPrediction {
			buff.WriteString(fmt.Sprintf("%.2f\n", val))

		}

		return buff.String(), nil
	}
}
