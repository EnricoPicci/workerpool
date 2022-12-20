package mapreduce_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/EnricoPicci/workerpool/mapreduce"
)

// define the reducer function
func reducer(acc []string, res string) []string {
	acc = append(acc, res)
	return acc
}

// In this test a slice of integers is reduced to a slice of strings using the MapReduce function
// MapReduce executes the transformation concurrently using a pool. If we want as result a single slice containing
// all the strings, we have to reduce the results produced by the various workers, hence the use of MapReduce
func TestMapReduce(t *testing.T) {
	// values that have to be reduced
	numOfValuesToReduce := 1000000
	valuesToReduce := inputValues(numOfValuesToReduce)

	concurrent := 1000
	// initial value of the accumulator to pass to the Reduce function
	accInitialValue := []string{}
	// Reduce the results into an accumulator
	resultsReceived, err := mapreduce.MapReduce(context.Background(), concurrent, valuesToReduce, mapIntToString, reducer, accInitialValue)

	// check the results of the test
	expectedNumOfErrors := 1
	reduceErr := err.(mapreduce.ReduceError)
	errors := reduceErr.Errors
	gotNumOfErrors := len(errors)
	expectedNumOfResults := numOfValuesToReduce - expectedNumOfErrors
	gotNumOfResults := len(resultsReceived)
	if expectedNumOfErrors != gotNumOfErrors {
		t.Errorf("Expected number of errors %v - got %v", expectedNumOfErrors, gotNumOfErrors)
	}
	if expectedNumOfResults != gotNumOfResults {
		t.Errorf("Expected number of results %v - got %v", expectedNumOfResults, gotNumOfResults)
	}

	// chech that the sum of the results received (once each result is converted back to a number) is right
	// https://www.vedantu.com/question-answer/the-formula-of-the-sum-of-first-n-natural-class-11-maths-cbse-5ee862757190f464f77a1c68
	expectedSum := numOfValuesToReduce*(numOfValuesToReduce-1)/2 - numberGeneratingError
	gotSum := 0
	for _, v := range resultsReceived {
		n, err := strconv.Atoi(v)
		if err != nil {
			panic(err)
		}
		gotSum = gotSum + n
	}
	if expectedSum != gotSum {
		t.Errorf("Expected sum of the numbers received %v - got %v", expectedSum, gotSum)
	}
}

func inputValues(numOfInputSentToPool int) []int {
	inputValues := make([]int, numOfInputSentToPool)
	for i := range inputValues {
		inputValues[i] = i
	}
	return inputValues
}
