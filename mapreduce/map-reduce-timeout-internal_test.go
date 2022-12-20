// this test is in the workerpool package since we want to test the internal status of the pool
package mapreduce

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/EnricoPicci/workerpool"
)

// In this test a timeout is triggered and the reduce is stopped
// We check that the status of the pool is "stopped".
// This is an internal test to make sure thare are no pending goroutines held by the pool
func TestMapReduceWithTimeoutPoolStatus(t *testing.T) {
	timeout := time.Duration(100)
	// a context with a timeout that is triggered with a short delay
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// parameters to constuct the pool
	poolSize := 10
	// construct the pool
	pool := workerpool.New(poolSize, MapStringToInt)
	pool.Start(ctx)

	// values that have to be reduced
	numOfValuesToReduce := 200000
	valuesToReduce := SliceOfIntegersAsStrings(numOfValuesToReduce)
	// launch a goroutine that sends the input values to the pool. When all the values have been sent the pool is stopped
	go func(_ctx context.Context) {
		defer pool.Stop()
		for _, v := range valuesToReduce {
			select {
			default:
				pool.Process(v)
			case <-ctx.Done():
				return
			}
		}
	}(ctx)

	// initial value of the accumulator to pass to the Reduce function
	accInitialValue := 0
	// Reduce the results into an accumulator
	// Since the timeout is fired before the end of the processing, the errors slice should contain 1 error
	_, err := reduce(ctx, pool, SumNumbers, accInitialValue)

	// the context signal is triggered very soon in the test, so we wait for some time to give the pool the possibility to shut down all the workers
	// and get to a stopped state
	time.Sleep(time.Millisecond)

	// check the results of the test
	expectedError := context.DeadlineExceeded
	gotError := err
	if expectedError != gotError {
		t.Errorf("Expected error %v - got %v", expectedError, gotError)
	}
	expectedPoolStatus := workerpool.Stopped
	gotStatus := pool.GetStatus()
	if expectedPoolStatus != gotStatus {
		t.Errorf("Expected pool status (the pool should have been closed as consequence of the context timeout triggering) %v - got %v",
			expectedPoolStatus, gotStatus)
	}
}

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
