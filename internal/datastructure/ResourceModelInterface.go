package datastructure

import (
	"github.io/Liqo/JobProfiler/internal/system"
	"time"
)

type ResourceModel interface {
	InsertNewJob(jobName string, namespace string, records []system.ResourceRecord)
	GetJobLastUpdate(jobName string, namespace string) (time.Time, error)
	GetPrediction(name string, namespace string, predictionTime time.Time) (string, error)
	GetLastUpdatedJob() (string, string, error)
}
