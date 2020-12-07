package system

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type PrometheusProvider struct {
	URLService  string
	PortService string
}

type ResourceType int

const (
	Memory ResourceType = 1
	CPU    ResourceType = 2
	None   ResourceType = 3
)

// ----------------------------------------

type prometheusQueryResultConnection struct {
	Status string
	Data   prometheusDataConnection
}

type prometheusDataConnection struct {
	ResultType string
	Result     []prometheusResultConnection
}

type prometheusResultConnection struct {
	Metric prometheusMetricConnection
	Values []prometheusValuesConnection
}

type prometheusMetricConnection struct {
	Source_workload                string
	Destination_workload           string
	Destination_workload_namespace string
	Namespace                      string
}

type prometheusValuesConnection struct {
	TimeStamp float64
	Value     string
}

func (tp *prometheusValuesConnection) UnmarshalJSON(data []byte) error {
	var v []interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		fmt.Printf("Error whilde decoding %v\n", err)
		return err
	}

	tp.Value = v[1].(string)
	tp.TimeStamp = v[0].(float64)

	return nil
}

// ----------------------------------------

type prometheusQueryResultResource struct {
	Status string
	Data   prometheusDataResource
}

type prometheusDataResource struct {
	ResultType string
	Result     []prometheusResultResource
}

type prometheusResultResource struct {
	Metric prometheusMetricResource
	Values []prometheusValuesResource
}

type prometheusMetricResource struct {
	Namespace string
	Pod       string
}

type prometheusValuesResource struct {
	TimeStamp float64
	Value     string
}

func (tp *prometheusValuesResource) UnmarshalJSON(data []byte) error {
	var v []interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		fmt.Printf("Error whilde decoding %v\n", err)
		return err
	}

	tp.Value = v[1].(string)
	tp.TimeStamp = v[0].(float64)

	return nil
}

// ----------------------------------------

type ConnectionRecord struct {
	Date         time.Time
	From         string
	To           string
	DstNamespace string
	Bandwidth    float64
}

type ResourceRecord struct {
	PodInformation Job
	Date           time.Time
	Value          float64
}

type Job struct {
	Name      string
	Namespace string
}

func (p *PrometheusProvider) InitPrometheusSystem() error {

	err := readEnvironmentVariable(p)

	if err != nil {
		return err
	}

	err = checkServiceAvailability(p)

	return err
}

func readEnvironmentVariable(p *PrometheusProvider) error {
	p.URLService = os.Getenv("PROMETHEUS_URL")

	if p.URLService == "" {
		return errors.New("PROMETHEUS_URL environment variable not set")
	}

	p.PortService = os.Getenv("PROMETHEUS_PORT")

	if p.PortService == "" {
		return errors.New("PROMETHEUS_PORT environment variable not set")
	}

	return nil
}

func checkServiceAvailability(p *PrometheusProvider) error {
	resp, err := http.Get("http://" + p.URLService + ":" + p.PortService)

	if err != nil {
		return errors.New(fmt.Sprintf("Prometheus is not reachable at %s:%s", p.URLService, p.PortService))
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return errors.New("Prometheus is not reachable")
	}

	return nil
}

func (p *PrometheusProvider) GetConnectionRecords(jobName string, namespace string, requestType string, schedulingTime time.Time) ([]ConnectionRecord, error) {
	var res prometheusQueryResultConnection

	end := schedulingTime.Unix()
	start := schedulingTime.AddDate(0, 0, -7).Unix()

	url := generateConnectionURL(p.URLService, p.PortService, jobName, namespace, requestType, start, end)

	resp, err := http.Get(url)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Prometheus is not reachable at %s", url))
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(body, &res)
	if err != nil {
		return nil, err
	}

	records := make([]ConnectionRecord, 0, 100)

	for _, pd := range res.Data.Result {

		for _, m := range pd.Values {
			var record ConnectionRecord

			record.From = pd.Metric.Source_workload
			record.To = pd.Metric.Destination_workload
			record.DstNamespace = pd.Metric.Destination_workload_namespace

			val, err := strconv.Atoi(strings.Split(m.Value, ".")[0])
			if err != nil {
				return nil, err
			}

			record.Bandwidth = float64(val)
			record.Date = time.Unix(int64(m.TimeStamp), 0)

			records = append(records, record)
		}
	}

	return records, nil
}

func (p *PrometheusProvider) GetResourceRecords(jobName string, namespace string, recordType ResourceType, schedulingTime time.Time) ([]ResourceRecord, error) {
	var res prometheusQueryResultResource
	var records []ResourceRecord

	end := schedulingTime.Unix()
	start := schedulingTime.AddDate(0, 0, -7).Unix()

	url := generateResourceURL(p.URLService, p.PortService, jobName, namespace, start, end, recordType)
	resp, err := http.Get(url)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Prometheus is not reachable at %s:%s", p.URLService, p.PortService))
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(body, &res)
	if err != nil {
		return nil, err
	}

	records = make([]ResourceRecord, 0, 100)

	for _, pd := range res.Data.Result {

		for _, m := range pd.Values {
			var record ResourceRecord

			record.PodInformation.Name = pd.Metric.Pod
			record.PodInformation.Namespace = pd.Metric.Namespace

			val, err := strconv.ParseFloat(m.Value, 64)
			if err != nil {
				return nil, err
			}

			record.Value = val
			record.Date = time.Unix(int64(m.TimeStamp), 0)

			records = append(records, record)
		}
	}

	return records, nil
}

func (p *PrometheusProvider) GetCPUThrottlingRecords(jobs []Job) ([]ResourceRecord, error) {
	var res prometheusQueryResultResource
	var records []ResourceRecord

	for _, j := range jobs {
		split := strings.Split(j.Name, "-")
		l := len(split) - 2

		j.Name = strings.Join(split[:l], "-")
	}

	end := time.Now().Unix()
	start := time.Now().Add(time.Second * (-120)).Unix()

	url := generateCPUThrottleURL(p.URLService, p.PortService, jobs, start, end)
	//log.Print(url)

	resp, err := http.Get(url)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Prometheus is not reachable at %s:%s", p.URLService, p.PortService))
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(body, &res)
	if err != nil {
		return nil, err
	}

	records = make([]ResourceRecord, 0, 100)

	for _, pd := range res.Data.Result {

		for _, m := range pd.Values {
			var record ResourceRecord

			record.PodInformation.Name = pd.Metric.Pod
			record.PodInformation.Namespace = pd.Metric.Namespace

			val, err := strconv.ParseFloat(m.Value, 64)
			if err != nil {
				return nil, err
			}

			record.Value = val
			record.Date = time.Unix(int64(m.TimeStamp), 0)

			records = append(records, record)
		}
	}

	return records, nil
}

func (p *PrometheusProvider) GetMemoryFailRecords(jobs []Job) ([]ResourceRecord, error) {
	var res prometheusQueryResultResource
	var records []ResourceRecord

	for _, j := range jobs {
		split := strings.Split(j.Name, "-")
		l := len(split) - 2

		j.Name = strings.Join(split[:l], "-")
	}

	end := time.Now().Unix()
	start := time.Now().Add(time.Second * (-120)).Unix()

	url := generateMemoryFailURL(p.URLService, p.PortService, jobs, start, end)
	//log.Print(url)

	resp, err := http.Get(url)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Prometheus is not reachable at %s:%s", p.URLService, p.PortService))
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(body, &res)
	if err != nil {
		return nil, err
	}

	records = make([]ResourceRecord, 0, 100)

	for _, pd := range res.Data.Result {

		for _, m := range pd.Values {
			var record ResourceRecord

			record.PodInformation.Name = pd.Metric.Pod
			record.PodInformation.Namespace = pd.Metric.Namespace

			val, err := strconv.ParseFloat(m.Value, 64)
			if err != nil {
				return nil, err
			}

			record.Value = val
			record.Date = time.Unix(int64(m.TimeStamp), 0)

			records = append(records, record)
		}
	}

	return records, nil
}

func generateMemoryFailURL(ip string, port string, jobs []Job, start int64, end int64) string {
	var differentNamespaces bytes.Buffer
	var differentPod bytes.Buffer

	for id, job := range jobs {
		differentNamespaces.WriteString(job.Namespace)
		if id != len(jobs)-1 {
			differentNamespaces.WriteString("%7C")
		} else {
			differentNamespaces.WriteString("%22%2C%20")
		}
	}

	for id, job := range jobs {
		differentPod.WriteString(extractDeploymentFromPodName(job.Name))
		if id != len(jobs)-1 {
			differentPod.WriteString(".*%7C")
		} else {
			differentPod.WriteString(".*%22%2C%20")
		}
	}

	return "http://" + ip + ":" + port +
		"/api/v1/query_range?query=sum%20by%20(pod%2C%20namespace)%20(label_replace(rate(container_memory_failures_total%7B" +
		"namespace%3D~%22" + differentNamespaces.String() +
		"pod%3D~%22" + differentPod.String() +
		"container!%3D%22%22%7D%5B1m%5D)%2C%20%22pod%22%2C%20%22%241%22%2C%20%22pod%22%2C%20%22(.*)-.%7B5%7D%22))" +
		"&start=" + strconv.Itoa(int(start)) +
		"&end=" + strconv.Itoa(int(end)) +
		"&step=1"
}

// sum by (pod, namespace) (label_replace(rate(container_memory_failures_total{namespace=~"default|default|default", pod=~"reviews-v1.*|reviews-v2.*|reviews-v3.*", container!=""}[1m]), "pod", "$1", "pod", "(.*)-.{5}"))

func generateCPUThrottleURL(ip string, port string, jobs []Job, start int64, end int64) string {
	var differentNamespaces bytes.Buffer
	var differentPod bytes.Buffer

	for id, job := range jobs {
		differentNamespaces.WriteString(job.Namespace)
		if id != len(jobs)-1 {
			differentNamespaces.WriteString("%7C")
		} else {
			differentNamespaces.WriteString("%22%2C%20")
		}
	}

	for id, job := range jobs {
		differentPod.WriteString(extractDeploymentFromPodName(job.Name))
		if id != len(jobs)-1 {
			differentPod.WriteString(".*%7C")
		} else {
			differentPod.WriteString(".*%22%7D%5B1m%5D)")
		}
	}

	return "http://" + ip + ":" + port +
		"/api/v1/query_range?query=sum%20by%20(pod%2C%20namespace)%20(label_replace(rate(container_cpu_cfs_throttled_seconds_total%7Bnamespace%3D~%22" +
		differentNamespaces.String() +
		"pod%3D~%22" +
		differentPod.String() +
		"%2C%20%22pod%22%2C%20%22%241%22%2C%20%22pod%22%2C%20%22(.*)-.%7B5%7D%22))" +
		"&start=" + strconv.Itoa(int(start)) +
		"&end=" + strconv.Itoa(int(end)) +
		"&step=1"
}

// sum by (pod, namespace) (label_replace(rate(container_cpu_cfs_throttled_seconds_total{}[1m]), "pod", "$1", "pod", "(.*)-.{5}"))

func generateConnectionURL(ip string, port string, podName string, namespace string, requestType string, start int64, end int64) string {
	return "http://" + ip + ":" + port +
		"/api/v1/query_range?query=" +
		"sum%20by(namespace%2C%20source_workload%2C%20destination_workload%2C%20destination_workload_namespace)%20(" +
		"increase(istio_" + requestType + "_bytes_sum%7B" +
		"namespace%3D%22" + namespace +
		"%22%2C%20source_workload%3D%22" + podName +
		"%22%2C%20destination_workload!%3D%22unknown" +
		"%22%7D%5B1m%5D))" +
		"&start=" + strconv.Itoa(int(start)) +
		"&end=" + strconv.Itoa(int(end)) +
		"&step=60"

	// sum by(namespace, source_workload, destination_workload, destination_workload_namespace) (increase(istio_request_bytes_sum{namespace="default", source_workload="productpage-v1"}[1m]))
}

func generateResourceURL(ip string, port string, podName string, namespace string, start int64, end int64, recordType ResourceType) string {

	if recordType == Memory {
		return "http://" + ip + ":" + port +
			"/api/v1/query_range?query=sum%20by%20(pod%2C%20namespace)%20(container_memory_usage_bytes%7Bnamespace%3D%22" + namespace +
			"%22%2C%20container!%3D%22POD%22%2C%20container!%3D%22%22%2C%20" +
			"pod%3D~%22" + podName + ".*%22%7D)" +
			"&start=" + strconv.Itoa(int(start)) +
			"&end=" + strconv.Itoa(int(end)) +
			"&step=60"
	} else {
		return "http://" + ip + ":" + port +
			"/api/v1/query_range?query=sum%20by%20(pod%2C%20namespace)%20(rate%20" +
			"(container_cpu_usage_seconds_total%7Bimage!%3D%22%22%2C%20pod%3D~%22" + podName + ".*%22%2C%20namespace%3D%22" + namespace + "%22%7D%5B1m%5D))" +
			"&start=" + strconv.Itoa(int(start)) +
			"&end=" + strconv.Itoa(int(end)) +
			"&step=60"
	}
	// sum by (pod, namespace) (container_memory_usage_bytes{namespace="default", container!="POD", container!="", pod=~"ad.*"})
	// sum by (pod, namespace) (rate (container_cpu_usage_seconds_total{image!="", pod!=""}[1m]))
}

func extractDeploymentFromPodName(podName string) string {
	split := strings.Split(podName, "-")
	l := len(split) - 2
	return strings.Join(split[:l], "-")
}
