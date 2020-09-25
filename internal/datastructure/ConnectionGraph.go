package datastructure

import (
	"bytes"
	"errors"
	"fmt"
	"github.io/Liqo/JobProfiler/internal/system"
	"log"
	"strconv"
	"sync"
	"time"
)

type ConnectionGraph struct {
	jobs      map[string]*connectionJob
	mutex     sync.Mutex
	timeslots int
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

func InitConnectionGraph(timeslots int) *ConnectionGraph {
	return &ConnectionGraph{
		jobs:      make(map[string]*connectionJob),
		mutex:     sync.Mutex{},
		timeslots: timeslots,
	}
}

func (cg *ConnectionGraph) InsertNewJob(jobName string, jobNamespace string, records []system.ConnectionRecord, schedulingTime time.Time) {
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
			connectedJobs: make([][]connections, cg.timeslots),
			lastUpdate:    schedulingTime,
		}

		for i := 0; i < cg.timeslots; i++ {
			job.connectedJobs[i] = make([]connections, 0, 5)
		}
	} else {
		job = cg.jobs[generateMapKey(jobName, jobNamespace)]
	}

	for _, jobInfo := range differentJobs {

		numRecords := make([]int, cg.timeslots)
		finalPrediction := make([]float64, cg.timeslots)
		for _, record := range records {

			if record.To == jobInfo.Name && record.DstNamespace == jobInfo.Namespace {

				id := generateTimeslotIndex(record.Date, cg.timeslots)

				numRecords[id]++
				finalPrediction[id] += record.Bandwidth
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
				connectedJobs: make([][]connections, cg.timeslots),
				lastUpdate:    time.Unix(0, 0), //set the date that will trigger a future update
			}

			for j := 0; j < cg.timeslots; j++ {
				jb.connectedJobs[j] = make([]connections, 0, 5)
			}

			cg.jobs[generateMapKey(jobInfo.Name, jobInfo.Namespace)] = &jb
		}

		for i := 0; i < cg.timeslots; i++ {

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

func (cg *ConnectionGraph) GetJobConnections(jobName string, jobNamespace string, schedulingTime time.Time) ([]connections, error) {
	cg.mutex.Lock()
	defer cg.mutex.Unlock()

	if job, found := cg.jobs[generateMapKey(jobName, jobNamespace)]; !found {
		return nil, errors.New("Job " + jobName + " is not yet present in the model")
	} else {
		id := generateTimeslotIndex(schedulingTime, cg.timeslots)
		return job.connectedJobs[id], nil
	}
}

func (cg *ConnectionGraph) FindSCC(jobName string, jobNamespace string, time time.Time) ([]system.Job, error) {
	result := make([]system.Job, 0, 5)
	visited := make(map[string]bool, len(cg.jobs))

	if job, found := cg.jobs[generateMapKey(jobName, jobNamespace)]; !found {
		return nil, errors.New("The connectionJob " + jobName + " is not present in the connection datastructure")
	} else {
		cg.mutex.Lock()
		defer cg.mutex.Unlock()

		i := generateTimeslotIndex(time, cg.timeslots)

		for name := range cg.jobs {
			visited[name] = false
		}
		log.Print("")

		visited[generateMapKey(job.jobInformation.Name, job.jobInformation.Namespace)] = true
		result = append(result, system.Job{
			Name:      job.jobInformation.Name,
			Namespace: job.jobInformation.Namespace,
		})

		for _, connectedJob := range job.connectedJobs[i] {
			if visited[generateMapKey(connectedJob.ConnectedTo.Name, connectedJob.ConnectedTo.Namespace)] == false {
				if err := DFS(cg, connectedJob, visited, &result, i); err != nil {
					return nil, err
				}
			}
		}
	}

	return result, nil
}

func DFS(cg *ConnectionGraph, connectedJob connections, visited map[string]bool, buffer *[]system.Job, slot int) error {
	visited[generateMapKey(connectedJob.ConnectedTo.Name, connectedJob.ConnectedTo.Namespace)] = true
	*buffer = append(*buffer, system.Job{
		Name:      connectedJob.ConnectedTo.Name,
		Namespace: connectedJob.ConnectedTo.Namespace,
	})
	//log.Print(*buffer)

	if job, found := cg.jobs[generateMapKey(connectedJob.ConnectedTo.Name, connectedJob.ConnectedTo.Namespace)]; found {
		for _, conn := range job.connectedJobs[slot] {
			if visited[generateMapKey(conn.ConnectedTo.Name, conn.ConnectedTo.Namespace)] == false {
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

func (cg *ConnectionGraph) PrintGraph() string {

	var buffer bytes.Buffer
	buffer.WriteString("-- Connection datastructure --")

	for _, job := range cg.jobs {
		buffer.WriteString("\nNode: " + job.jobInformation.Name)
		buffer.WriteString("\n\tConnected to:")

		for i := 0; i < cg.timeslots; i++ {
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
