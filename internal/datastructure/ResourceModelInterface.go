package datastructure

import (
	"crownlabs.com/profiling/internal/system"
	"time"
)

type ResourceModel interface {
	InsertJob(jobName string, namespace string, records []system.ResourceRecord, schedulingTime time.Time)
	GetJobUpdateTime(jobName string, namespace string) (time.Time, error)
	GetLastUpdatedJob() (system.Job, error)
	GetJobPrediction(name string, namespace string, predictionTime time.Time) (string, error)
	PrintModel() string
}
