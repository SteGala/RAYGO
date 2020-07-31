package datastructure

import (
	"github.io/Liqo/JobProfiler/internal/system"
	"sort"
)

func computePeakSignal(records []system.ResourceRecord) []float64 {
	peaksValues := make([]float64, timeSlots)

	for _, record := range records {
		if record.Date.Hour() >= 0 && record.Date.Hour() < 6 {
			if record.Value > peaksValues[0] {
				peaksValues[0] = record.Value
			}

		} else if record.Date.Hour() >= 6 && record.Date.Hour() < 12 {
			if record.Value > peaksValues[1] {
				peaksValues[1] = record.Value
			}

		} else if record.Date.Hour() >= 12 && record.Date.Hour() < 18 {
			if record.Value > peaksValues[2] {
				peaksValues[2] = record.Value
			}

		} else {
			if record.Value > peaksValues[3] {
				peaksValues[3] = record.Value
			}

		}
	}

	return peaksValues
}

// To calculate the kth percentile (where k is any number between zero and one hundred), do the following steps:
//  - 1 Order all the values in the data set from smallest to largest.
//  - 2 Multiply k percent by the total number of values, n. This number is called the index.
//  - 3 If the index obtained in Step 2 is not a whole number, round it up to the nearest whole number and go to Step 4a. If the index obtained in Step 2 is a whole number, go to Step 4b.
//  - 4a Count the values in your data set from left to right (from the smallest to the largest value) until you reach the number indicated by Step 3. The corresponding value in your data set is the kth percentile.
//  - 4b Count the values in your data set from left to right until you reach the number indicated by Step 2. The kth percentile is the average of that corresponding value in your data set and the value that directly follows it.
func computeKPercentile(records []system.ResourceRecord, K int) []float64 {
	sortedRecords := make([][]float64, timeSlots)

	for i := 0; i < timeSlots; i++ {
		sortedRecords[i] = make([]float64, 10)
	}

	//finalPrediction := make([][]float64, timeSlots)
	for _, record := range records {

		if record.Date.Hour() >= 0 && record.Date.Hour() < 6 {
			sortedRecords[0] = append(sortedRecords[0], record.Value)

		} else if record.Date.Hour() >= 6 && record.Date.Hour() < 12 {
			sortedRecords[1] = append(sortedRecords[1], record.Value)

		} else if record.Date.Hour() >= 12 && record.Date.Hour() < 18 {
			sortedRecords[2] = append(sortedRecords[2], record.Value)

		} else {
			sortedRecords[3] = append(sortedRecords[3], record.Value)

		}
	}

	for i := 0; i < timeSlots; i++ {
		sort.Float64s(sortedRecords[i])
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
