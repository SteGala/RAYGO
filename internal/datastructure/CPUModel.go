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
	name          string
	cpuPrediction []float64
	lastUpdate    time.Time
}

func InitCPUModel() *CPUModel {
	return &CPUModel{
		jobs:  make(map[string]*cpuInfo),
		mutex: sync.Mutex{},
	}
}

func (mm *CPUModel) InsertNewJob(jobName string, namespace string, records []system.ResourceRecord) {

	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	job := cpuInfo{
		name:          jobName + "{" + namespace + "}",
		cpuPrediction: make([]float64, timeSlots),
		lastUpdate:    time.Now(),
	}

	computeCPUWeightedSignal(records)

	peak := computePeakSignal(records)
	//percentile := computeKPercentile(records, 98)

	job.cpuPrediction = peak

	mm.jobs[jobName+"{"+namespace+"}"] = &job
}

func (mm *CPUModel) GetJobLastUpdate(jobName string, namespace string) (time.Time, error) {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	if job, found := mm.jobs[jobName+"{"+namespace+"}"]; found {
		return job.lastUpdate, nil
	} else {
		return time.Now(), errors.New(fmt.Sprintf("Job %s does not exist", jobName))
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

func (mm *CPUModel) GetPrediction(jobName string, namespace string) (string, error) {
	if job, found := mm.jobs[jobName+"{"+namespace+"}"]; !found {
		return "", errors.New("The connectionJob " + jobName + " is not present in the connection datastructure")
	} else {
		var buff bytes.Buffer

		for _, val := range job.cpuPrediction {
			buff.WriteString(fmt.Sprintf("%.2f\n", val))

		}

		return buff.String(), nil
	}
}
