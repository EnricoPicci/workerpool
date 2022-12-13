package workerpool_test

import (
	"context"
	"errors"
	"strconv"
)

// this is the function that is passed to the worker pool to perform the processing
var numGeneratingError = 5
var convError = errors.New("Error occurred while processing")

func mapStringToInt(ctx context.Context, input string) (int, error) {
	n, _ := strconv.Atoi(input)
	if n == numGeneratingError {
		return 0, convError
	}
	return n, nil
}

// define the reducer function
func sumNumbers(acc int, val int) int {
	acc = acc + val
	return acc
}

func sliceOfIntegersAsStrings(numOfValuesSentToPool int) []string {
	inputValues := make([]string, numOfValuesSentToPool)
	for i := range inputValues {
		inputValues[i] = strconv.Itoa(i)
	}
	return inputValues
}
