package mapreduce_test

import (
	"context"
	"testing"

	"github.com/EnricoPicci/workerpool/mapreduce"
)

// In this test a slice of strings representing integers is reduced to a number which is the sum of all the integers using the MapReduce function
func TestMapReduceSumOfNumbers(t *testing.T) {
	// values that have to be reduced
	numOfValuesToReduce := 10
	valuesToReduce := SliceOfIntegersAsStrings(numOfValuesToReduce)

	concurrent := 1
	// initial value of the accumulator to pass to the Reduce function
	accInitialValue := 0
	// Reduce the results into an accumulator
	sum, err := mapreduce.MapReduce(context.Background(), concurrent, valuesToReduce, MapStringToInt, SumNumbers, accInitialValue)

	// check the results of the test
	expectedNumOfErrors := 1
	reduceErr := err.(mapreduce.ReduceError)
	errors := reduceErr.Errors
	gotNumOfErrors := len(errors)
	if expectedNumOfErrors != gotNumOfErrors {
		t.Errorf("Expected number of errors %v - got %v", expectedNumOfErrors, gotNumOfErrors)
	}

	// chech that the sum of the results received (once each result is converted back to a number) is right
	// https://www.vedantu.com/question-answer/the-formula-of-the-sum-of-first-n-natural-class-11-maths-cbse-5ee862757190f464f77a1c68
	expectedSum := numOfValuesToReduce*(numOfValuesToReduce-1)/2 - NumGeneratingError
	gotSum := sum
	if expectedSum != gotSum {
		t.Errorf("Expected sum %v - got %v", expectedSum, gotSum)
	}
}

// In this test a series of empty strings is passed to a reducer which expects string that should be converted to an integer.
// Therefore all the values passed to the reducer generate errors
func TestMapReduceAllErrors(t *testing.T) {
	// values that have to be reduced
	numOfValuesToReduce := 100000
	valuesToReduce := sliceOfEmptyStrings(numOfValuesToReduce)

	concurrent := 1000
	// initial value of the accumulator to pass to the Reduce function
	accInitialValue := 0
	// Reduce the results into an accumulator
	sum, err := mapreduce.MapReduce(context.Background(), concurrent, valuesToReduce, mapStringToIntErr, SumNumbers, accInitialValue)

	// check the results of the test
	expectedNumOfErrors := numOfValuesToReduce
	reduceErr := err.(mapreduce.ReduceError)
	errors := reduceErr.Errors
	gotNumOfErrors := len(errors)
	if expectedNumOfErrors != gotNumOfErrors {
		t.Errorf("Expected number of errors %v - got %v", expectedNumOfErrors, gotNumOfErrors)
	}

	expectedSum := 0
	gotSum := sum
	if expectedSum != gotSum {
		t.Errorf("Expected sum %v - got %v", expectedSum, gotSum)
	}
}

// In this test a series of empty strings is passed to a reducer which expects string that should be converted to an integer.
// Therefore all the values passed to the reducer generate errors.
// The pool size is 1
func TestMapReduceAllErrorsPoolSize_1(t *testing.T) {
	// values that have to be reduced
	numOfValuesToReduce := 1000
	valuesToReduce := sliceOfEmptyStrings(numOfValuesToReduce)

	concurrent := 1
	// initial value of the accumulator to pass to the Reduce function
	accInitialValue := 0
	// Reduce the results into an accumulator
	sum, err := mapreduce.MapReduce(context.Background(), concurrent, valuesToReduce, mapStringToIntErr, SumNumbers, accInitialValue)

	// check the results of the test
	expectedNumOfErrors := numOfValuesToReduce
	reduceErr := err.(mapreduce.ReduceError)
	errors := reduceErr.Errors
	gotNumOfErrors := len(errors)
	if expectedNumOfErrors != gotNumOfErrors {
		t.Errorf("Expected number of errors %v - got %v", expectedNumOfErrors, gotNumOfErrors)
	}

	expectedSum := 0
	gotSum := sum
	if expectedSum != gotSum {
		t.Errorf("Expected sum %v - got %v", expectedSum, gotSum)
	}
}
