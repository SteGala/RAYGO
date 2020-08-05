package profiling

import (
	"crypto/sha1"
	"fmt"
	"strings"
)

func generateConnectionCRDName(sjob string, djob string, namespace string) string {
	return hash(sjob + djob + namespace)
}

func generateResourceCRDName(jobName string, namespace string) string {
	return hash(jobName + namespace)
}

func hash(s string) string {
	h := sha1.New()
	h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func extractDeploymentFromPodName(podName string) string {
	split := strings.Split(podName, "-")
	l := len(split) - 2
	return strings.Join(split[:l], "-")
}
