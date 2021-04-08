package datastructure

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.io/Liqo/JobProfiler/internal/system"
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
	executionType := os.Getenv("EXECUTION_TYPE")
	if executionType == "test" {
		return &ConnectionGraph{
			jobs:      populateFakeConnectionGraph(timeslots),
			mutex:     sync.Mutex{},
			timeslots: timeslots,
		}
	} else {
		return &ConnectionGraph{
			jobs:      make(map[string]*connectionJob),
			mutex:     sync.Mutex{},
			timeslots: timeslots,
		}
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

		for i := 0; i < cg.timeslots; i++ {
			if numRecords[i] != 0 {
				finalPrediction[i] = finalPrediction[i] / float64(numRecords[i])
			}
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
		result = append(result, system.Job{
			Name:      jobName + "-xxxxxxxx-xxxxx",
			Namespace: jobNamespace,
		})
		//log.Print(cg.jobs)
		//log.Print(generateMapKey(jobName, jobNamespace))
		return result, nil
	} else {
		cg.mutex.Lock()
		defer cg.mutex.Unlock()

		i := generateTimeslotIndex(time, cg.timeslots)

		for name := range cg.jobs {
			visited[name] = false
		}

		visited[generateMapKey(job.jobInformation.Name, job.jobInformation.Namespace)] = true
		result = append(result, system.Job{
			Name:      job.jobInformation.Name + "-xxxxxxxx-xxxxx",
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
		Name:      connectedJob.ConnectedTo.Name + "-xxxxxxxx-xxxxx",
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

func populateFakeConnectionGraph(timeslots int) map[string]*connectionJob {
	ret := make(map[string]*connectionJob)
	connJobs := make([][]connections, timeslots)

	for i := 0; i < timeslots; i++ {
		connJobs[i] = make([]connections, 0, 5)
	}

	con1 := connections{
		ConnectedTo: system.Job{
			Name:      "emailservice",
			Namespace: "test-stefano",
		},
		Bandwidth: 145,
	}
	con2 := connections{
		ConnectedTo: system.Job{
			Name:      "checkoutservice",
			Namespace: "test-stefano",
		},
		Bandwidth: 145,
	}
	con3 := connections{
		ConnectedTo: system.Job{
			Name:      "recommendationservice",
			Namespace: "test-stefano",
		},
		Bandwidth: 145,
	}
	con4 := connections{
		ConnectedTo: system.Job{
			Name:      "frontend",
			Namespace: "test-stefano",
		},
		Bandwidth: 145,
	}
	con5 := connections{
		ConnectedTo: system.Job{
			Name:      "paymentservice",
			Namespace: "test-stefano",
		},
		Bandwidth: 145,
	}

	con6 := connections{
		ConnectedTo: system.Job{
			Name:      "productcatalogservice",
			Namespace: "test-stefano",
		},
		Bandwidth: 145,
	}
	con7 := connections{
		ConnectedTo: system.Job{
			Name:      "cartservice",
			Namespace: "test-stefano",
		},
		Bandwidth: 145,
	}
	con8 := connections{
		ConnectedTo: system.Job{
			Name:      "currencyservice",
			Namespace: "test-stefano",
		},
		Bandwidth: 145,
	}
	con9 := connections{
		ConnectedTo: system.Job{
			Name:      "shippingservice",
			Namespace: "test-stefano",
		},
		Bandwidth: 145,
	}
	con10 := connections{
		ConnectedTo: system.Job{
			Name:      "redis-cart",
			Namespace: "test-stefano",
		},
		Bandwidth: 145,
	}
	con11 := connections{
		ConnectedTo: system.Job{
			Name:      "adservice",
			Namespace: "test-stefano",
		},
		Bandwidth: 145,
	}

	for i := 0; i < timeslots; i++ {
		connJobs[i] = append(connJobs[i], con1)
		connJobs[i] = append(connJobs[i], con2)
		connJobs[i] = append(connJobs[i], con3)
		connJobs[i] = append(connJobs[i], con4)
		connJobs[i] = append(connJobs[i], con5)
		connJobs[i] = append(connJobs[i], con6)
		connJobs[i] = append(connJobs[i], con7)
		connJobs[i] = append(connJobs[i], con8)
		connJobs[i] = append(connJobs[i], con9)
		connJobs[i] = append(connJobs[i], con10)
		connJobs[i] = append(connJobs[i], con11)
	}

	emailservice_cj := connectionJob{
		jobInformation: system.Job{
			Name:      "emailservice",
			Namespace: "test-stefano",
		},
		connectedJobs: connJobs,
		lastUpdate:    time.Now(),
	}

	checkoutservice_cj := connectionJob{
		jobInformation: system.Job{
			Name:      "checkoutservice",
			Namespace: "test-stefano",
		},
		connectedJobs: connJobs,
		lastUpdate:    time.Now(),
	}

	recommendationservice_cj := connectionJob{
		jobInformation: system.Job{
			Name:      "recommendationservice",
			Namespace: "test-stefano",
		},
		connectedJobs: connJobs,
		lastUpdate:    time.Now(),
	}

	frontend_cj := connectionJob{
		jobInformation: system.Job{
			Name:      "frontend",
			Namespace: "test-stefano",
		},
		connectedJobs: connJobs,
		lastUpdate:    time.Now(),
	}

	paymentservice_cj := connectionJob{
		jobInformation: system.Job{
			Name:      "paymentservice",
			Namespace: "test-stefano",
		},
		connectedJobs: connJobs,
		lastUpdate:    time.Now(),
	}

	productcatalogservice_cj := connectionJob{
		jobInformation: system.Job{
			Name:      "productcatalogservice",
			Namespace: "test-stefano",
		},
		connectedJobs: connJobs,
		lastUpdate:    time.Now(),
	}

	cartservice_cj := connectionJob{
		jobInformation: system.Job{
			Name:      "cartservice",
			Namespace: "test-stefano",
		},
		connectedJobs: connJobs,
		lastUpdate:    time.Now(),
	}

	currencyservice_cj := connectionJob{
		jobInformation: system.Job{
			Name:      "currencyservice",
			Namespace: "test-stefano",
		},
		connectedJobs: connJobs,
		lastUpdate:    time.Now(),
	}

	shippingservice_cj := connectionJob{
		jobInformation: system.Job{
			Name:      "shippingservice",
			Namespace: "test-stefano",
		},
		connectedJobs: connJobs,
		lastUpdate:    time.Now(),
	}

	redis_cart_cj := connectionJob{
		jobInformation: system.Job{
			Name:      "redis-cart",
			Namespace: "test-stefano",
		},
		connectedJobs: connJobs,
		lastUpdate:    time.Now(),
	}

	adservice_cj := connectionJob{
		jobInformation: system.Job{
			Name:      "adservice",
			Namespace: "test-stefano",
		},
		connectedJobs: connJobs,
		lastUpdate:    time.Now(),
	}

	ret[generateMapKey(adservice_cj.jobInformation.Name, adservice_cj.jobInformation.Namespace)] = &adservice_cj
	ret[generateMapKey(cartservice_cj.jobInformation.Name, cartservice_cj.jobInformation.Namespace)] = &cartservice_cj
	ret[generateMapKey(checkoutservice_cj.jobInformation.Name, checkoutservice_cj.jobInformation.Namespace)] = &checkoutservice_cj
	ret[generateMapKey(currencyservice_cj.jobInformation.Name, currencyservice_cj.jobInformation.Namespace)] = &currencyservice_cj
	ret[generateMapKey(emailservice_cj.jobInformation.Name, emailservice_cj.jobInformation.Namespace)] = &emailservice_cj
	ret[generateMapKey(frontend_cj.jobInformation.Name, frontend_cj.jobInformation.Namespace)] = &frontend_cj
	ret[generateMapKey(paymentservice_cj.jobInformation.Name, paymentservice_cj.jobInformation.Namespace)] = &paymentservice_cj
	ret[generateMapKey(productcatalogservice_cj.jobInformation.Name, productcatalogservice_cj.jobInformation.Namespace)] = &productcatalogservice_cj
	ret[generateMapKey(recommendationservice_cj.jobInformation.Name, recommendationservice_cj.jobInformation.Namespace)] = &recommendationservice_cj
	ret[generateMapKey(redis_cart_cj.jobInformation.Name, redis_cart_cj.jobInformation.Namespace)] = &redis_cart_cj
	ret[generateMapKey(shippingservice_cj.jobInformation.Name, shippingservice_cj.jobInformation.Namespace)] = &shippingservice_cj

	return ret
}
