package mapreduce_test

import (
	"errors"
	"fmt"
	"strconv"
)

// this is the function that is passed to the worker pool to perform the processing
var NumGeneratingError = 5
var ConvError = errors.New("Error occurred while processing")

func MapStringToInt(input string) (int, error) {
	n, _ := strconv.Atoi(input)
	if n == NumGeneratingError {
		return 0, ConvError
	}
	return n, nil
}

// this is the function that is passed to the worker pool to perform the processing
var numberGeneratingError = 4
var conversionError = errors.New("Error occurred while processing")

func mapIntToString(input int) (string, error) {
	if input == numberGeneratingError {
		return "", conversionError
	}
	return fmt.Sprintf("%v", input), nil
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

// this is the function that is passed to the worker pool to perform the processing
// raise an error if the string can not be converted to an integer
func mapStringToIntErr(input string) (int, error) {
	n, err := strconv.Atoi(input)
	if err != nil {
		return 0, ConvError
	}
	return n, nil
}

func sliceOfEmptyStrings(numOfValuesSentToPool int) []string {
	inputValues := make([]string, numOfValuesSentToPool)
	for i := 0; i < numOfValuesSentToPool; i++ {
		inputValues[i] = ""
	}
	return inputValues
}
