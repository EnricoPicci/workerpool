package workerpool_test

import (
	"context"
	"testing"

	"github.com/EnricoPicci/workerpool"
)

// In this test a slice of strings representing integers is reduced to a number which is the sum of all the integers using the MapReduce function
func TestReduceableMapReduceSumOfNumbers(t *testing.T) {
	// values that have to be reduced
	numOfValuesToReduce := 10
	values := sliceOfIntegersAsStrings(numOfValuesToReduce)
	reduceable := workerpool.Reducable[string, int, int](values)

	// parameters
	concurrent := 1
	accInitialValue := 0

	// Reduce the results into an accumulator
	sum, err := reduceable.MapReduce(context.Background(), mapStringToInt, sumNumbers, accInitialValue, concurrent)

	// check the results of the test
	expectedNumOfErrors := 1
	reduceErr := err.(workerpool.ReduceError)
	errors := reduceErr.Errors
	gotNumOfErrors := len(errors)
	if expectedNumOfErrors != gotNumOfErrors {
		t.Errorf("Expected number of errors %v - got %v", expectedNumOfErrors, gotNumOfErrors)
	}

	// chech that the sum of the results received (once each result is converted back to a number) is right
	// https://www.vedantu.com/question-answer/the-formula-of-the-sum-of-first-n-natural-class-11-maths-cbse-5ee862757190f464f77a1c68
	expectedSum := numOfValuesToReduce*(numOfValuesToReduce-1)/2 - numGeneratingError
	gotSum := sum
	if expectedSum != gotSum {
		t.Errorf("Expected sum %v - got %v", expectedSum, gotSum)
	}
}
