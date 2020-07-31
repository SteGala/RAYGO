package datastructure

import (
	"bytes"
	"errors"
	"fmt"
	"github.io/Liqo/JobProfiler/internal/system"
	"sync"

	//metav1 "k8s.io/apimachinery/internal/apis/meta/v1"
	"strconv"
	"time"
)

const timeSlots = 4

type ConnectionGraph struct {
	jobs  map[string]*connectionJob
	mutex sync.Mutex
}

type connectionJob struct {
	name          string
	connectedJobs [][]Connections
	lastUpdate    time.Time
}

type Connections struct {
	ConnectedTo string
	Bandwidth   float64
}

func InitConnectionGraph() *ConnectionGraph {
	return &ConnectionGraph{
		jobs:  make(map[string]*connectionJob),
		mutex: sync.Mutex{},
	}
}

func (cg *ConnectionGraph) InsertNewJob(jobName string, namespace string, records []system.ConnectionRecord) {

	var job *connectionJob

	differentJobs := getDifferentJobNames(records)

	cg.mutex.Lock()
	defer cg.mutex.Unlock()


	if _, found := cg.jobs[jobName+"{"+namespace+"}"]; !found {
		job = &connectionJob{
			name:          jobName + "{" + namespace + "}",
			connectedJobs: make([][]Connections, timeSlots),
			lastUpdate:    time.Now(),
		}

		for i := 0; i < timeSlots; i++ {
			job.connectedJobs[i] = make([]Connections, 0, 5)
		}
	} else {
		job = cg.jobs[jobName+"{"+namespace+"}"]
	}

	// per ognuno dei connectionJob che ho scoperto mi calcolo una media per fascia oraria
	for _, name := range differentJobs {

		numRecords := make([]int, timeSlots)
		finalPrediction := make([]float64, timeSlots)
		for _, record := range records {

			if record.To+"{"+record.DstNamespace+"}" == name {

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
		if _, found := cg.jobs[name]; !found {
			jb := connectionJob{
				name:          name,
				connectedJobs: make([][]Connections, timeSlots),
				lastUpdate:    time.Unix(0, 0), //set the date that will trigger a future update
			}

			for j := 0; j < timeSlots; j++ {
				jb.connectedJobs[j] = make([]Connections, 0, 5)
			}

			cg.jobs[name] = &jb
		}

		for i := 0; i < timeSlots; i++ {

			if finalPrediction[i] == 0 {
				continue
			}

			con := Connections{
				ConnectedTo: name,
				Bandwidth:   finalPrediction[i],
			}

			// append the prediction for the timeslot in the current job
			job.connectedJobs[i] = append(job.connectedJobs[i], con)

			// append the same prediction to the other job (if not present)
			found := false
			for _, j := range cg.jobs[name].connectedJobs[i] {
				if j.ConnectedTo == jobName+"{"+namespace+"}" {
					found = true
					break
				}
			}

			if !found {
				con2 := Connections{
					ConnectedTo: jobName + "{" + namespace + "}",
					Bandwidth:   finalPrediction[i],
				}

				cg.jobs[name].connectedJobs[i] = append(cg.jobs[name].connectedJobs[i], con2)
			}
		}
	}

	cg.jobs[jobName+"{"+namespace+"}"] = job
}

func (cg *ConnectionGraph) GetJobLastUpdate(jobName string, namespace string) (time.Time, error) {
	cg.mutex.Lock()
	defer cg.mutex.Unlock()

	if job, found := cg.jobs[jobName+"{"+namespace+"}"]; found {
		return job.lastUpdate, nil
	} else {
		return time.Now(), errors.New(fmt.Sprintf("Job %s does not exist", jobName))
	}
}

func (cg *ConnectionGraph) GetJobConnections(jobName string, namespace string) ([][]Connections, error) {
	cg.mutex.Lock()
	defer cg.mutex.Unlock()

	if job, found := cg.jobs[jobName+"{"+namespace+"}"]; !found {
		return nil, errors.New("Job " + jobName + " is not yet present in the model")
	} else {
		return job.connectedJobs, nil
	}
}

func (cg *ConnectionGraph) FindSCC(jobName string, namespace string) (string, error) {
	var visited map[string]bool
	var buffer bytes.Buffer

	visited = make(map[string]bool, len(cg.jobs))

	if job, found := cg.jobs[jobName+"{"+namespace+"}"]; !found {
		return "", errors.New("The connectionJob " + jobName + " is not present in the connection datastructure")
	} else {
		cg.mutex.Lock()

		for i := 0; i < timeSlots; i++ {
			for name, _ := range cg.jobs {
				visited[name] = false
			}

			visited[job.name] = true
			buffer.WriteString(jobName + "{" + namespace + "}")

			for _, connectedJob := range job.connectedJobs[i] {
				if visited[connectedJob.ConnectedTo] == false {
					buffer.WriteString(" ")
					if err := DFS(cg, connectedJob, visited, &buffer, i); err != nil {
						return "", err
					}
				}
			}

			if i != timeSlots-1 {
				buffer.WriteString("\n")
			}
		}

		cg.mutex.Unlock()
	}

	return buffer.String(), nil
}

func (cg *ConnectionGraph) PrintGraph() string {

	var buffer bytes.Buffer
	buffer.WriteString("-- Connection datastructure --")

	for _, job := range cg.jobs {
		buffer.WriteString("\nNode: " + job.name)
		buffer.WriteString("\n\tConnected to:")

		for i := 0; i < timeSlots; i++ {
			buffer.WriteString("\n\t\tTimeslot: " + strconv.Itoa(i) + "  [ ")

			for _, con := range job.connectedJobs[i] {
				buffer.WriteString(con.ConnectedTo + "(")
				buffer.WriteString(fmt.Sprintf("%.2f", con.Bandwidth))
				buffer.WriteString(")  ")
			}

			buffer.WriteString("]")
		}
	}

	return buffer.String()
}

func DFS(cg *ConnectionGraph, connectedJob Connections, visited map[string]bool, buffer *bytes.Buffer, slot int) error {
	visited[connectedJob.ConnectedTo] = true
	buffer.WriteString(connectedJob.ConnectedTo)

	if job, found := cg.jobs[connectedJob.ConnectedTo]; found {
		for _, conn := range job.connectedJobs[slot] {
			if visited[conn.ConnectedTo] == false {
				buffer.WriteString(" ")
				if err := DFS(cg, conn, visited, buffer, slot); err != nil {
					return err
				}
			}
		}
	} else {
		return errors.New("Job " + connectedJob.ConnectedTo + "not present")
	}

	return nil
}

func getDifferentJobNames(records []system.ConnectionRecord) []string {
	differentJobs := make([]string, 0, 5)

	// creates a list with all the different jobs connected to the current connectionJob
	for _, record := range records {
		found := false

		for _, name := range differentJobs {
			if name == record.To+"{"+record.DstNamespace+"}" {
				found = true
			}
		}

		if !found {
			differentJobs = append(differentJobs, record.To+"{"+record.DstNamespace+"}")
		}
	}

	return differentJobs
}