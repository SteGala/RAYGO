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
	URLService      string
	PortService     string
	NetworkProvider string
}

type ResourceType int

const (
	Memory ResourceType = 1
	CPU    ResourceType = 2
	None   ResourceType = 3
)

// internal representation of connection records
type ConnectionRecord struct {
	Date         time.Time
	From         string
	To           string
	DstNamespace string
	Bandwidth    float64
}

// internal representation of resource records
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

	err := p.readEnvironmentVariable()

	if err != nil {
		return err
	}

	err = p.checkServiceAvailability()

	return err
}

func (p *PrometheusProvider) readEnvironmentVariable() error {
	p.URLService = os.Getenv("PROMETHEUS_URL")
	if p.URLService == "" {
		return errors.New("PROMETHEUS_URL environment variable not set")
	}

	p.PortService = os.Getenv("PROMETHEUS_PORT")
	if p.PortService == "" {
		return errors.New("PROMETHEUS_PORT environment variable not set")
	}

	p.NetworkProvider = os.Getenv("NETWORK_METRIC_PROVIDER")
	return nil
}

func (p *PrometheusProvider) checkServiceAvailability() error {
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
	var res prometheusIstioQueryResultConnection
	var res2 prometheusLinkerdQueryResultConnection

	end := schedulingTime.Unix()
	start := schedulingTime.Add(time.Minute * (-30)).Unix()

	url := p.generateConnectionURL(jobName, namespace, requestType, start, end)

	resp, err := http.Get(url)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Prometheus is not reachable at %s", url))
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if p.NetworkProvider == "istio" {
		err = json.Unmarshal(body, &res)
		if err != nil {
			return nil, err
		}
	} else {
		err = json.Unmarshal(body, &res2)
		if err != nil {
			return nil, err
		}
	}

	records := make([]ConnectionRecord, 0, 100)

	if p.NetworkProvider == "istio" {
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
	} else {
		for _, pd := range res2.Data.Result {
			for _, m := range pd.Values {
				var record ConnectionRecord

				record.From = pd.Metric.Deployment
				record.To = pd.Metric.Dst_deployment
				record.DstNamespace = pd.Metric.Dst_namespace

				val, err := strconv.Atoi(strings.Split(m.Value, ".")[0])
				if err != nil {
					return nil, err
				}

				record.Bandwidth = float64(val)
				record.Date = time.Unix(int64(m.TimeStamp), 0)

				records = append(records, record)
			}
		}
	}
	return records, nil
}

func (p *PrometheusProvider) GetResourceRecords(jobName string, namespace string, recordType ResourceType, schedulingTime time.Time) ([]ResourceRecord, error) {
	var res prometheusQueryResultResource
	var records []ResourceRecord

	end := schedulingTime.Unix()
	start := schedulingTime.Add(time.Minute * (-30)).Unix()

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

	currDate := start

	for _, pd := range res.Data.Result {
		var record ResourceRecord

		record.PodInformation.Name = pd.Metric.Pod
		record.PodInformation.Namespace = pd.Metric.Namespace
		currDate = start

		for _, m := range pd.Values {

			for ; currDate <= end; currDate += 1 {

				if int64(m.TimeStamp) == currDate {
					val, err := strconv.ParseFloat(m.Value, 64)
					if err != nil {
						return nil, err
					}

					record.Value = val
					record.Date = time.Unix(int64(m.TimeStamp), 0)

					records = append(records, record)

					break
				}

				record.Value = 0
				record.Date = time.Unix(currDate, 0)

				records = append(records, record)
			}

			// prometheus returns a sample every second
			currDate += 1
		}

		if currDate != end {
			for ; currDate <= end; currDate += 1 {
				record.Value = 0
				record.Date = time.Unix(currDate, 0)

				records = append(records, record)
			}
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
		"container!%3D%22POD%22%2C%20container!%3D%22%22%7D%5B5m%5D)%2C%20%22pod%22%2C%20%22%241%22%2C%20%22pod%22%2C%20%22(.*)-(.%7B7%2C10%7D-.%7B5%7D)%24%22))" +
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
			//differentNamespaces.WriteString("%22%2C")
		}
	}

	for id, job := range jobs {
		differentPod.WriteString(extractDeploymentFromPodName(job.Name))
		if id != len(jobs)-1 {
			differentPod.WriteString(".*%7C")
		} else {
			differentPod.WriteString(".*%22%2C%20%20")
			//differentPod.WriteString(".*%22%2C")
		}
	}

	
	return "http://" + ip + ":" + port +
		"/api/v1/query_range?query=sum%20by%20(pod%2C%20namespace)%20(label_replace(rate(container_cpu_cfs_throttled_seconds_total%7Bnamespace%3D~%22" +
		differentNamespaces.String() +
		"pod%3D~%22" +
		differentPod.String() +
		"container!%3D%22POD%22%2C%20container!%3D%22%22%7D%5B5m%5D)%2C%20%22pod%22%2C%20%22%241%22%2C%20%22pod%22%2C%20%22(.*)-(.%7B7%2C10%7D-.%7B5%7D)%24%22))" +
		"&start=" + strconv.Itoa(int(start)) +
		"&end=" + strconv.Itoa(int(end)) +
		"&step=1"
	/*
	return "http://" + ip + ":" + port +
		"/api/v1/query_range?query=sum%28container_cpu_cfs_throttled_periods_total%7Bnamespace%3D%7E%22" + differentNamespaces.String() +
		"+pod%3D%7E%22" + differentPod.String() +
		"+container%21%3D%22%22%7D%29+by+%28pod%2C+namespace%29+%2Fsum%28container_cpu_cfs_periods_total%7Bnamespace%3D%7E%22" + differentNamespaces.String() +
		"+pod%3D%7E%22" + differentPod.String() +
		"+container%21%3D%22%22%7D%29+by+%28pod%2C+namespace%29%0A" +
		"&start=" + strconv.Itoa(int(start)) +
		"&end=" + strconv.Itoa(int(end)) +
		"&step=1"
		*/
}

// sum by (pod, namespace) (label_replace(rate(container_cpu_cfs_throttled_seconds_total{}[1m]), "pod", "$1", "pod", "(.*)-.{5}"))
// sum(container_cpu_cfs_throttled_periods_total{namespace=~"test-stefano", pod=~"fr.*", container!=""}) by (pod, namespace) /sum(container_cpu_cfs_periods_total{namespace=~"test-stefano", pod=~"fr.*", container!=""}) by (pod, namespace)

func (p *PrometheusProvider) generateConnectionURL(podName string, namespace string, requestType string, start int64, end int64) string {
	if p.NetworkProvider == "istio" {
		return "http://" + p.URLService + ":" + p.PortService +
			"/api/v1/query_range?query=" +
			"sum%20by(namespace%2C%20source_workload%2C%20destination_workload%2C%20destination_workload_namespace)%20(" +
			"increase(istio_" + requestType + "_bytes_sum%7B" +
			"namespace%3D%22" + namespace +
			"%22%2C%20source_workload%3D%22" + podName +
			"%22%2C%20destination_workload!%3D%22unknown" +
			"%22%7D%5B1m%5D))" +
			"&start=" + strconv.Itoa(int(start)) +
			"&end=" + strconv.Itoa(int(end)) +
			"&step=1"
		// sum by(namespace, source_workload, destination_workload, destination_workload_namespace) (increase(istio_request_bytes_sum{namespace="default", source_workload="productpage-v1"}[1m]))
	} else {
		return "http://" + p.URLService + ":" + p.PortService +
			"/api/v1/query_range?query=" +
			"sum%20by%20(namespace%2C%20deployment%2C%20dst_deployment%2C%20dst_namespace)" +
			"%20(increase(" + requestType + "_total%7Bdirection%3D%22outbound%22%2C%20" +
			"dst_deployment!%3D%22%22%2C%20namespace%3D%22" + namespace +
			"%22%2C%20deployment%3D%22" + podName + "%22%7D%5B1m%5D))" +
			"&start=" + strconv.Itoa(int(start)) +
			"&end=" + strconv.Itoa(int(end)) +
			"&step=1"
		//sum by (namespace, deployment, dst_deployment, dst_namespace) (request_total{direction="outbound", dst_deployment!="", namespace="test-stefano"})

	}

}

func generateResourceURL(ip string, port string, podName string, namespace string, start int64, end int64, recordType ResourceType) string {

	if recordType == Memory {
		return "http://" + ip + ":" + port +
			"/api/v1/query_range?query=sum%28container_memory_working_set_bytes%7Bpod%3D%7E%22" + podName +
			".*%22%2C+namespace%3D%22" + namespace +
			"%22%2C+container%21%3D%22%22%2C+image%21%3D%22%22%7D%29+by+%28pod%2C+namespace%29" +
			"&start=" + strconv.Itoa(int(start)) +
			"&end=" + strconv.Itoa(int(end)) +
			"&step=1"
	} else {
		return "http://" + ip + ":" + port +
			"/api/v1/query_range?query=sum(node_namespace_pod_container%3Acontainer_cpu_usage_seconds_total%3Asum_rate%7B" +
			"namespace%3D%22" + namespace + "%22%2C%20pod%3D~%22" + podName + ".*%22%7D)%20by%20(pod%2C%20namespace)" +
			"&start=" + strconv.Itoa(int(start)) +
			"&end=" + strconv.Itoa(int(end)) +
			"&step=1"
	}

	// sum(container_memory_working_set_bytes{pod=~"fr.*", namespace="test-stefano", container!="", image!=""}) by (pod, namespace)
	// sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_rate{namespace="test-stefano", pod=~"frontend.*"}) by (pod)
}

func extractDeploymentFromPodName(podName string) string {
	split := strings.Split(podName, "-")
	l := len(split) - 2
	return strings.Join(split[:l], "-")
}
