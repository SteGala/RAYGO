package system

import (
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
	__name__                string
	Beta_kubernetes_io_arch string
	Beta_kubernetes_io_os   string
	Container               string
	Id                      string
	Image                   string
	Instance                string
	Job                     string
	Kubernetes_io_arch      string
	Kubernetes_io_hostname  string
	Kubernetes_io_os        string
	Name                    string
	Namespace               string
	Pod                     string
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
	Date  time.Time
	Value float64
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

func (p *PrometheusProvider) GetConnectionRecords(jobName string, namespace string, requestType string) ([]ConnectionRecord, error) {
	var res prometheusQueryResultConnection

	end := time.Now().Unix()
	start := time.Now().AddDate(0, 0, -7).Unix()

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

func (p *PrometheusProvider) GetResourceRecords(jobName string, namespace string, recordType ResourceType) ([]ResourceRecord, error) {
	var res prometheusQueryResultResource
	var records []ResourceRecord

	end := time.Now().Unix()
	start := time.Now().AddDate(0, 0, -7).Unix()

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

func generateConnectionURL(ip string, port string, podName string, namespace string, requestType string, start int64, end int64) string {
	return "http://" + ip + ":" + port +
		"/api/v1/query_range?query=" +
		"sum%20by%20(namespace%2C%20source_workload%2C%20destination_workload%2C%20dst_namespace%2C%20destination_workload_namespace)%20(" +
		"increase(istio_" + requestType + "_bytes_sum%7B" +
		"namespace%3D%22" + namespace +
		"%22%2C%20source_workload%3D%22" + podName +
		"%22%7D%5B1m%5D))" +
		"&start=" + strconv.Itoa(int(start)) +
		"&end=" + strconv.Itoa(int(end)) +
		"&step=60"

	// sum by(namespace, source_workload, destination_workload, dst_namespace) (increase(istio_request_bytes_sum{namespace="default", source_workload="productpage-v1"}[5m]))
}

func generateResourceURL(ip string, port string, podName string, namespace string, start int64, end int64, recordType ResourceType) string {

	if recordType == Memory {
		return "http://" + ip + ":" + port +
			"/api/v1/query_range?query=container_memory_usage_bytes%7Bnamespace%3D%22" + namespace +
			"%22%2C%20container!%3D%22%22%2C%20" +
			"id%3D~%22%2Fdocker.*%22%2C%20" +
			"pod%3D~%22" + podName + ".*%22%7D" +
			"&start=" + strconv.Itoa(int(start)) +
			"&end=" + strconv.Itoa(int(end)) +
			"&step=60"
	} else {
		return "http://" + ip + ":" + port +
			"/api/v1/query_range?query=sum%20by%20(pod%2C%20namespace)%20(rate%20" +
			"(container_cpu_usage_seconds_total%7Bimage!%3D\"\"%2C%20pod%3D~\"" + podName + ".*\"%7D%5B1m%5D))" +
			"&start=" + strconv.Itoa(int(start)) +
			"&end=" + strconv.Itoa(int(end)) +
			"&step=60"
	}

}

// sum by (pod) (rate (container_cpu_usage_seconds_total{image!="", pod!=""}[1m]))
