package mapreduce_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/EnricoPicci/workerpool"
	"github.com/EnricoPicci/workerpool/mapreduce"
)

func TestReduce(t *testing.T) {
	// this is the error value sent if an error occurs
	conversionError := errors.New("Error occurred while processing")
	// the error is generated when the
	numberGeneratingError := 1

	// parameters to constuct the pool
	poolSize := 1000
	do := func(in int) (string, error) {
		// simulates that for certain values an error is returned
		if in == numberGeneratingError {
			return "", conversionError
		}
		return fmt.Sprintf("%v", in), nil
	}
	// construct the pool
	pool := workerpool.New(poolSize, do)

	// start the pool
	ctx := context.Background()
	pool.Start(ctx)

	// number of input values that will be sent to the pool to be processed
	numOfInputSentToPool := 3
	// launch a goroutine that sends the input values to the pool. When all the values have been sent the pool is stopped
	go func() {
		defer pool.Stop()
		for i := 0; i < numOfInputSentToPool; i++ {
			pool.Process(i)
		}
	}()

	// initial value of the accumulator to pass to the Reduce function
	accInitialValue := []string{}
	// define the reducer function
	reducer := func(acc []string, res string) []string {
		acc = append(acc, res)
		return acc
	}
	// Reduce the results into an accumulator
	resultsReceived, err := mapreduce.Reduce(context.Background(), pool, reducer, accInitialValue)

	// check the results of the test
	expectedNumOfErrors := 1
	reduceErr := err.(mapreduce.ReduceError)
	gotNumOfErrors := len(reduceErr.Errors)
	expectedNumOfResults := numOfInputSentToPool - expectedNumOfErrors
	gotNumOfResults := len(resultsReceived)
	if expectedNumOfResults != gotNumOfResults {
		t.Errorf("Expected number of results %v - got %v", expectedNumOfResults, gotNumOfResults)
	}
	if expectedNumOfErrors != gotNumOfErrors {
		t.Errorf("Expected number of errors %v - got %v", expectedNumOfResults, gotNumOfErrors)
	}

	// chech that the sum of the results received (once each result is converted back to a number) is right
	// https://www.vedantu.com/question-answer/the-formula-of-the-sum-of-first-n-natural-class-11-maths-cbse-5ee862757190f464f77a1c68
	expectedSum := numOfInputSentToPool*(numOfInputSentToPool-1)/2 - numberGeneratingError
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
