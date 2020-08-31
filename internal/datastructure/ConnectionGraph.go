package datastructure

import (
	"bytes"
	"errors"
	"fmt"
	"github.io/Liqo/JobProfiler/internal/system"
	"strconv"
	"sync"
	"time"
)

const timeSlots = 4

type ConnectionGraph struct {
	jobs  map[string]*connectionJob
	mutex sync.Mutex
}

type connectionJob struct {
	jobInformation system.Job
	connectedJobs  [][]connections
	lastUpdate     time.Time
}

type connections struct {
	ConnectedTo system.Job
	Bandwidth   float64
}

func InitConnectionGraph() *ConnectionGraph {
	return &ConnectionGraph{
		jobs:  make(map[string]*connectionJob),
		mutex: sync.Mutex{},
	}
}

func (cg *ConnectionGraph) InsertNewJob(jobName string, jobNamespace string, records []system.ConnectionRecord) {

	var job *connectionJob

	differentJobs := getDifferentJobNames(records)

	cg.mutex.Lock()
	defer cg.mutex.Unlock()

	if _, found := cg.jobs[generateMapKey(jobName, jobNamespace)]; !found {
		job = &connectionJob{
			jobInformation: system.Job{
				Name:      jobName,
				Namespace: jobNamespace,
			},
			connectedJobs: make([][]connections, timeSlots),
			lastUpdate:    time.Now(),
		}

		for i := 0; i < timeSlots; i++ {
			job.connectedJobs[i] = make([]connections, 0, 5)
		}
	} else {
		job = cg.jobs[generateMapKey(jobName, jobNamespace)]
	}

	for _, jobInfo := range differentJobs {

		numRecords := make([]int, timeSlots)
		finalPrediction := make([]float64, timeSlots)
		for _, record := range records {

			if record.To == jobInfo.Name && record.DstNamespace == jobInfo.Namespace {

				if record.Date.Hour() >= 0 && record.Date.Hour() < 6 {
					numRecords[0]++
					finalPrediction[0] += record.Bandwidth
				} else if record.Date.Hour() >= 6 && record.Date.Hour() < 12 {
					numRecords[1]++
					finalPrediction[1] += record.Bandwidth
				} else if record.Date.Hour() >= 12 && record.Date.Hour() < 18 {
					numRecords[2]++
					finalPrediction[2] += record.Bandwidth
				} else {
					numRecords[3]++
					finalPrediction[3] += record.Bandwidth
				}
			}
		}

		if numRecords[0] != 0 {
			finalPrediction[0] = finalPrediction[0] / float64(numRecords[0])
		}

		if numRecords[1] != 0 {
			finalPrediction[1] = finalPrediction[1] / float64(numRecords[1])
		}

		if numRecords[2] != 0 {
			finalPrediction[2] = finalPrediction[2] / float64(numRecords[2])
		}

		if numRecords[3] != 0 {
			finalPrediction[3] = finalPrediction[3] / float64(numRecords[3])
		}

		// if the connected job is not yet in the model we create it
		if _, found := cg.jobs[generateMapKey(jobInfo.Name, jobInfo.Namespace)]; !found {
			jb := connectionJob{
				jobInformation: system.Job{
					Name:      jobInfo.Name,
					Namespace: jobInfo.Namespace,
				},
				connectedJobs: make([][]connections, timeSlots),
				lastUpdate:    time.Unix(0, 0), //set the date that will trigger a future update
			}

			for j := 0; j < timeSlots; j++ {
				jb.connectedJobs[j] = make([]connections, 0, 5)
			}

			cg.jobs[generateMapKey(jobInfo.Name, jobInfo.Namespace)] = &jb
		}

		for i := 0; i < timeSlots; i++ {

			if finalPrediction[i] == 0 {
				continue
			}

			con := connections{
				ConnectedTo: jobInfo,
				Bandwidth:   finalPrediction[i],
			}

			// append the prediction for the timeslot in the current job (if not present)
			// update the prediction (if present)
			found := false
			for id, j := range job.connectedJobs[i] {
				if j.ConnectedTo == con.ConnectedTo {
					found = true
					job.connectedJobs[i][id] = con
					break
				}
			}
			if !found {
				job.connectedJobs[i] = append(job.connectedJobs[i], con)
			}

			// append the same prediction to the other job (if not present)
			// update the prediction (if present)
			con2 := connections{
				ConnectedTo: system.Job{
					Name:      jobName,
					Namespace: jobNamespace,
				},
				Bandwidth: finalPrediction[i],
			}
			found = false
			for id, j := range cg.jobs[generateMapKey(jobInfo.Name, jobInfo.Namespace)].connectedJobs[i] {
				if j.ConnectedTo.Name == jobName && j.ConnectedTo.Namespace == jobNamespace {
					found = true
					cg.jobs[generateMapKey(jobInfo.Name, jobInfo.Namespace)].connectedJobs[i][id] = con2
					break
				}
			}

			if !found {
				cg.jobs[generateMapKey(jobInfo.Name, jobInfo.Namespace)].connectedJobs[i] = append(cg.jobs[generateMapKey(jobInfo.Name, jobInfo.Namespace)].connectedJobs[i], con2)
			}
		}
	}

	cg.jobs[generateMapKey(jobName, jobNamespace)] = job
}

// The function receive as input the Name and the Namespace of the job and returns
// the date of the last update for that given job
func (cg *ConnectionGraph) GetJobUpdateTime(jobName string, namespace string) (time.Time, error) {
	cg.mutex.Lock()
	defer cg.mutex.Unlock()

	if job, found := cg.jobs[jobName+"{"+namespace+"}"]; found {
		return job.lastUpdate, nil
	} else {
		return time.Now(), errors.New(fmt.Sprintf("Job %s does not exist", jobName))
	}
}

// The function returns the Name and the Namespace of the job with the oldest update time
func (cg *ConnectionGraph) GetLastUpdatedJob() (string, string, error) {

	lastUpdate := time.Now()
	var jobName, jobNamespace string
	found := false

	cg.mutex.Lock()
	defer cg.mutex.Unlock()

	for _, job := range cg.jobs {
		if job.lastUpdate.Before(lastUpdate) {
			lastUpdate = job.lastUpdate
			jobName = job.jobInformation.Name
			jobNamespace = job.jobInformation.Namespace
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

func (cg *ConnectionGraph) GetJobConnections(jobName string, jobNamespace string) ([][]connections, error) {
	cg.mutex.Lock()
	defer cg.mutex.Unlock()

	if job, found := cg.jobs[generateMapKey(jobName, jobNamespace)]; !found {
		return nil, errors.New("Job " + jobName + " is not yet present in the model")
	} else {
		return job.connectedJobs, nil
	}
}

func (cg *ConnectionGraph) FindSCC(jobName string, jobNamespace string) ([]system.Job, error) {
	var visited map[string]bool
	var result []system.Job

	result = make([]system.Job, 0, 5)
	visited = make(map[string]bool, len(cg.jobs))

	if job, found := cg.jobs[generateMapKey(jobName, jobNamespace)]; !found {
		return nil, errors.New("The connectionJob " + jobName + " is not present in the connection datastructure")
	} else {
		cg.mutex.Lock()

		for i := 0; i < timeSlots; i++ {
			for name, _ := range cg.jobs {
				visited[name] = false
			}

			visited[generateMapKey(job.jobInformation.Name, job.jobInformation.Namespace)] = true
			result = append(result, system.Job{
				Name:      job.jobInformation.Name,
				Namespace: job.jobInformation.Namespace,
			})

			for _, connectedJob := range job.connectedJobs[i] {
				if visited[generateMapKey(connectedJob.ConnectedTo.Name, connectedJob.ConnectedTo.Namespace)] == false {
					if err := DFS(cg, connectedJob, visited, result, i); err != nil {
						return nil, err
					}
				}
			}
		}

		cg.mutex.Unlock()
	}

	return result, nil
}

func (cg *ConnectionGraph) PrintGraph() string {

	var buffer bytes.Buffer
	buffer.WriteString("-- Connection datastructure --")

	for _, job := range cg.jobs {
		buffer.WriteString("\nNode: " + job.jobInformation.Name)
		buffer.WriteString("\n\tConnected to:")

		for i := 0; i < timeSlots; i++ {
			buffer.WriteString("\n\t\tTimeslot: " + strconv.Itoa(i) + "  [ ")

			for _, con := range job.connectedJobs[i] {
				buffer.WriteString(con.ConnectedTo.Name + "(")
				buffer.WriteString(fmt.Sprintf("%.2f", con.Bandwidth))
				buffer.WriteString(")  ")
			}

			buffer.WriteString("]")
		}
	}

	return buffer.String()
}

func DFS(cg *ConnectionGraph, connectedJob connections, visited map[string]bool, buffer []system.Job, slot int) error {
	visited[generateMapKey(connectedJob.ConnectedTo.Name, connectedJob.ConnectedTo.Namespace)] = true
	buffer = append(buffer, system.Job{
		Name:      connectedJob.ConnectedTo.Name,
		Namespace: connectedJob.ConnectedTo.Namespace,
	})

	if job, found := cg.jobs[generateMapKey(connectedJob.ConnectedTo.Name, connectedJob.ConnectedTo.Namespace)]; found {
		for _, conn := range job.connectedJobs[slot] {
			if visited[generateMapKey(connectedJob.ConnectedTo.Name, connectedJob.ConnectedTo.Namespace)] == false {
				if err := DFS(cg, conn, visited, buffer, slot); err != nil {
					return err
				}
			}
		}
	} else {
		return errors.New("Job " + connectedJob.ConnectedTo.Name + "not present")
	}

	return nil
}

func getDifferentJobNames(records []system.ConnectionRecord) []system.Job {
	differentJobs := make([]system.Job, 0, 5)

	// creates a list with all the different jobs connected to the current connectionJob
	for _, record := range records {
		found := false

		for _, j := range differentJobs {
			if j.Name == record.To && j.Namespace == record.DstNamespace {
				found = true
			}
		}

		if !found && record.To != "" && record.DstNamespace != "" {
			differentJobs = append(differentJobs, system.Job{
				Name:      record.To,
				Namespace: record.DstNamespace,
			})
		}
	}

	return differentJobs
}
