package datastructure

import (
	"github.io/SteGala/JobProfiler/src/system"
	"time"
)

type ResourceModel interface {
	InsertNewJob(jobName string, namespace string, records []system.ResourceRecord)
	GetJobLastUpdate(jobName string, namespace string) (time.Time, error)
	GetPrediction(name string, namespace string) (string, error)
}
