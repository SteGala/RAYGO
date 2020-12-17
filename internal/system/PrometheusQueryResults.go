package system

import (
	"encoding/json"
	"fmt"
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
		fmt.Printf("Error while decoding %v\n", err)
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
		fmt.Printf("Error while decoding %v\n", err)
		return err
	}

	tp.Value = v[1].(string)
	tp.TimeStamp = v[0].(float64)

	return nil
}
