package datastructure

import (
	"github.io/Liqo/JobProfiler/internal/system"
	"sort"
	"time"
)

func computePeakSignal(records []system.ResourceRecord, timeSlots int) []float64 {
	peaksValues := make([]float64, timeSlots)
	resultValues := make([]float64, timeSlots)
	countValues := make([]int, timeSlots)
	var podName string

	if len(records) > 0 {
		podName = records[0].PodInformation.Name
	}

	for id, record := range records {
		if record.PodInformation.Name != podName || id == len(records) - 1 {
			for i := 0; i < timeSlots; i++ {
				if peaksValues[i] > 0 {
					resultValues[i] += peaksValues[i]
					countValues[i]++
				}
			}

			podName = record.PodInformation.Name
			peaksValues = make([]float64, timeSlots)
		}

		id := generateTimeslotIndex(record.Date, timeSlots)

		if record.Value > peaksValues[id] {
			peaksValues[id] = record.Value
		}
	}

	for i := 0; i < timeSlots; i++ {
		if countValues[i] > 0 {
			resultValues[i] = resultValues[i] / float64(countValues[i])
		}
	}

	return resultValues
}

// To calculate the kth percentile (where k is any number between zero and one hundred), do the following steps:
//  - 1 Order all the values in the data set from smallest to largest.
//  - 2 Multiply k percent by the total number of values, n. This number is called the index.
//  - 3 If the index obtained in Step 2 is not a whole number, round it up to the nearest whole number and go to Step 4a. If the index obtained in Step 2 is a whole number, go to Step 4b.
//  - 4a Count the values in your data set from left to right (from the smallest to the largest value) until you reach the number indicated by Step 3. The corresponding value in your data set is the kth percentile.
//  - 4b Count the values in your data set from left to right until you reach the number indicated by Step 2. The kth percentile is the average of that corresponding value in your data set and the value that directly follows it.
func computeKPercentile(records []system.ResourceRecord, K int, timeSlots int) []float64 {
	sortedRecords := make([][]float64, timeSlots)

	for i := 0; i < timeSlots; i++ {
		sortedRecords[i] = make([]float64, 10)
	}

	for _, record := range records {

		id := generateTimeslotIndex(record.Date, timeSlots)

		sortedRecords[id] = append(sortedRecords[id], record.Value)
	}

	for i := 0; i < timeSlots; i++ {
		sort.Float64s(sortedRecords[i])
	}
	
	if K == 100 {
		result := make([]float64, timeSlots)

		for i := 0; i < timeSlots; i++ {
			result[i] = sortedRecords[i][len(sortedRecords[i]) - 1]
		}

		return result
	}

	index := make([]int, timeSlots)
	rounded := make([]bool, timeSlots)

	for i := 0; i < timeSlots; i++ {
		val := (float64(K) / 100) * float64(len(sortedRecords[i]))

		if needsNumberToBeRounded(val) {
			rounded[i] = true
			index[i] = int(val + 1)
		} else {
			rounded[i] = false
			index[i] = int(val)
		}
	}

	result := make([]float64, timeSlots)

	for i := 0; i < timeSlots; i++ {
		if rounded[i] {
			result[i] = sortedRecords[i][index[i]-1]
		} else {
			result[i] = (sortedRecords[i][index[i]-1] + sortedRecords[i][index[i]]) / 2
		}
	}

	return result
}

func needsNumberToBeRounded(i float64) bool {
	if i == float64(int(i)) {
		return false

	} else {
		return true
	}
}

func generateMapKey(jobName string, jobNamespace string) string {
	return jobName + "{" + jobNamespace + "}"
}

func generateTimeslotIndex(date time.Time, nTimeslots int) int {
	nHours := 24 / nTimeslots
	id := 0

	for count := nHours; count <= 24; count += nHours {
		if date.Hour() < count {
			return id
		}

		id++
	}

	// this return statement should never be reached. It's been added
	// because otherwise the compiler complains
	return -1
}
