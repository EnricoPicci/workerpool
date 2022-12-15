package workerpool

import (
	"context"
	"errors"
	"strconv"
)

// this is the function that is passed to the worker pool to perform the processing
var NumGeneratingError = 5
var ConvError = errors.New("Error occurred while processing")

func MapStringToInt(ctx context.Context, input string) (int, error) {
	n, _ := strconv.Atoi(input)
	if n == NumGeneratingError {
		return 0, ConvError
	}
	return n, nil
}

// define the reducer function
func SumNumbers(acc int, val int) int {
	acc = acc + val
	return acc
}

func SliceOfIntegersAsStrings(numOfValuesSentToPool int) []string {
	inputValues := make([]string, numOfValuesSentToPool)
	for i := range inputValues {
		inputValues[i] = strconv.Itoa(i)
	}
	return inputValues
}
