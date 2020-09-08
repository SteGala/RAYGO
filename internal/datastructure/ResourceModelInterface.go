package datastructure

import (
	"github.io/Liqo/JobProfiler/internal/system"
	"time"
)

type ResourceModel interface {
	InsertJob(jobName string, namespace string, records []system.ResourceRecord)
	UpdateJob(records []system.ResourceRecord)
	GetJobUpdateTime(jobName string, namespace string) (time.Time, error)
	GetLastUpdatedJob() (system.Job, error)
	GetJobPrediction(name string, namespace string, predictionTime time.Time) (string, error)
	PrintModel() string
}
