package datastructure

import (
	"bytes"
	"errors"
	"fmt"
	"log"

	//"log"
	"math"
	//"strconv"
	"sync"
	"time"

	"github.io/Liqo/JobProfiler/internal/monitoring"

	"github.io/Liqo/JobProfiler/internal/system"
)

type CPUModel struct {
	jobs                        map[string]*cpuInfo
	mutex                       sync.Mutex
	timeslots                   int
	cpuThrottlingThreshold      float64
	cpuThrottlingLowerThreshold float64
}

type cpuInfo struct {
	jobInformation system.Job
	cpuPrediction  []float64
	lastUpdate     time.Time
}

func InitCPUModel(timeslots int, threshold float64, lowerThreshold float64) *CPUModel {
	return &CPUModel{
		jobs:                        make(map[string]*cpuInfo),
		mutex:                       sync.Mutex{},
		timeslots:                   timeslots,
		cpuThrottlingThreshold:      threshold,
		cpuThrottlingLowerThreshold: lowerThreshold,
	}
}

func (cp *CPUModel) InsertJob(jobName string, namespace string, records []system.ResourceRecord, schedulingTime time.Time) {
	key := jobName + "{" + namespace + "}"

	//log.Print("Gathering historical data fo job " + jobName + ". Collected CPU records: " + strconv.Itoa(countNonZeroRecords(records)))

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
	percentile := computeKPercentile(records, 97, cp.timeslots)
	monitoring.ExposeCPUProfiling(jobName, namespace, "exponential", percentile[generateTimeslotIndex(time.Now(), cp.timeslots)])

	//log.Printf("Received %d CPU records", countNonZeroRecords(records))
	if countNonZeroRecords(records) >= 500 {
		job.cpuPrediction = percentile
	} else {
		job.cpuPrediction = nil
	}

	/*
		if c, found := cp.jobs[key]; found {
			if c.cpuPrediction == nil {
				cp.jobs[key] = &job
			} else {
				for i := 0; i < cp.timeslots; i++ {
					if job.cpuPrediction != nil {
						job.cpuPrediction[i] = c.cpuPrediction[i]*0.8 + job.cpuPrediction[i]*0.2
					}
				}
				cp.jobs[key] = &job
			}
		} else {
			cp.jobs[key] = &job
		}

	*/

	cp.jobs[key] = &job
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
	decayTime := 900 / timeslots

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

		id := generateTimeslotIndex(predictionTime, cp.timeslots)
		//prediction := job.cpuPrediction[id] + job.cpuPrediction[id]*0.2
		prediction := job.cpuPrediction[id]
		return fmt.Sprintf("%.3f", prediction), nil
	}
}

func (cp *CPUModel) UpdateJob(records []system.ResourceRecord) {
	type result struct {
		podName          string
		podNamespace     string
		avgThrottling    float64
		linearPrediction float64
	}

	var job system.Job
	var sum = 0.0
	var count = 0.0
	throttlingInfo := make([]result, 0, 5)
	sumX := 0.0
	sumX2 := 0.0
	sumY := 0.0
	sumXY := 0.0

	if len(records) > 0 {
		job = records[0].PodInformation
	}

	for id, record := range records {
		if record.PodInformation.Name != job.Name || id == len(records)-1 {
			var a, b, avg float64

			if count > 0 {
				b = (count*sumXY - sumX*sumY) / (count*sumX2 - sumX*sumX)
				a = (sumY - b*sumX) / (count)
				avg = sum / count
			}

			throttlingInfo = append(throttlingInfo, result{
				podName:          job.Name,
				podNamespace:     job.Namespace,
				avgThrottling:    avg,
				linearPrediction: b*(count+15.0) + a,
			})

			sum = 0.0
			sumX = 0.0
			sumX2 = 0.0
			sumY = 0.0
			sumXY = 0.0
			count = 0.0
			job = record.PodInformation
		}

		sum += record.Value

		sumX += count
		sumX2 += count * count
		sumY += record.Value
		sumXY += count * record.Value

		count++
	}

	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	for _, t := range throttlingInfo {
		key := t.podName + "{" + t.podNamespace + "}"
		currTime := time.Now()

		_, found := cp.jobs[key]

		maxThreshold := t.avgThrottling + t.avgThrottling*0.2
		minThreshold := t.avgThrottling - t.avgThrottling*0.2

		//log.Print("Pod " + job.Name + " (avg, threshold) (" + strconv.Itoa(int(t.avgThrottling)) + ", " + strconv.Itoa(int(cp.cpuThrottlingLowerThreshold)) + ")")

		if found && t.linearPrediction > maxThreshold && /*t.avgThrottling > cp.cpuThrottlingLowerThreshold &&*/ cp.jobs[key].cpuPrediction != nil {
			id := generateTimeslotIndex(currTime, cp.timeslots)
			cp.jobs[key].cpuPrediction[id] += cp.jobs[key].cpuPrediction[id] * computeResourceIncrease(t.linearPrediction, maxThreshold)
			log.Print("Increasing CPU for pod " + cp.jobs[key].jobInformation.Name + " of " + fmt.Sprintf("%f", computeResourceIncrease(t.linearPrediction, minThreshold)))
		}

		if found && t.linearPrediction < minThreshold && /*t.avgThrottling > cp.cpuThrottlingLowerThreshold &&*/ cp.jobs[key].cpuPrediction != nil {
			id := generateTimeslotIndex(currTime, cp.timeslots)
			cp.jobs[key].cpuPrediction[id] -= cp.jobs[key].cpuPrediction[id] * computeResourceDecrease(t.linearPrediction, minThreshold)
			log.Print("Reducing CPU for pod " + cp.jobs[key].jobInformation.Name + " of " + fmt.Sprintf("%f", computeResourceDecrease(t.linearPrediction, minThreshold)))
		}

		if found {
			cp.jobs[key].lastUpdate = currTime
			//monitoring.ExposeCPUProfiling(cp.jobs[key].jobInformation.Name, cp.jobs[key].jobInformation.Namespace, "runtime", cp.jobs[key].cpuPrediction[generateTimeslotIndex(currTime, cp.timeslots)])
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
