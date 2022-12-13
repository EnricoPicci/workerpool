// this test is in the workerpool package since we want to test the internal status of the pool
package workerpool

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"
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

// In this test a timeout is triggered and the MapReduce is stopped
func TestMapReduceWithTimeout(t *testing.T) {
	timeout := time.Duration(100)
	// a context with a timeout that is triggered with a short delay
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// values that have to be reduced
	numOfValuesToReduce := 200000
	valuesToReduce := sliceOfIntegersAsStrings(numOfValuesToReduce)

	concurrent := 10
	// initial value of the accumulator to pass to the Reduce function
	accInitialValue := 0
	// Reduce the results into an accumulator
	// Since the timeout is fired before the end of the processing, the errors slice should contain 1 error
	sum, err := MapReduce(ctx, concurrent, valuesToReduce, mapStringToInt, sumNumbers, accInitialValue)
	t.Log(">>>>>>>>> Value of sum when reduce returns after deadline is triggered", sum)

	// check the results of the test
	expectedError := context.DeadlineExceeded
	gotError := err
	if expectedError != gotError {
		t.Errorf("Expected error %v - got %v", expectedError, gotError)
	}
}

func sliceOfIntegersAsStrings(numOfValuesSentToPool int) []string {
	inputValues := make([]string, numOfValuesSentToPool)
	for i := range inputValues {
		inputValues[i] = strconv.Itoa(i)
	}
	return inputValues
}

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
	pool := New(poolSize, mapStringToInt)
	pool.Start(ctx)

	// values that have to be reduced
	numOfValuesToReduce := 200000
	valuesToReduce := sliceOfIntegersAsStrings(numOfValuesToReduce)
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
	_, err := reduce(ctx, pool, sumNumbers, accInitialValue)

	// the context signal is triggered very soon in the test, so we wait for some time to give the pool the possibility to shut down all the workers
	// and get to a stopped state
	time.Sleep(time.Millisecond)

	// check the results of the test
	expectedError := context.DeadlineExceeded
	gotError := err
	if expectedError != gotError {
		t.Errorf("Expected error %v - got %v", expectedError, gotError)
	}
	expectedPoolStatus := stopped
	gotStatus := pool.status
	if expectedPoolStatus != gotStatus {
		t.Errorf("Expected pool status (the pool should have been closed as consequence of the context timeout triggering) %v - got %v",
			expectedPoolStatus, gotStatus)
	}
}

// This tests checks that, if a context timeout is triggered, all workers of the worker pool are terminated
// (as long as the mapper function they call is able to handle a context timeour signal and terminate).
// Each worker simulates to have work to do that lasts a certain duration (workDuration).
// The work performed by the workers is defined by the function mapper.
// A context timeout is triggered after a period (timeout) where timeout < workDuration. Since timeout < workDuration the workers receive the timeout signal
// before they are actually able to complete their work.
// When a worker starts, it starts processing the tasks calling the mapper function.
// When the mapper function starts, it sets the flag mapperStarted to true. When the mapper finishes its job it sets the flag taskComplete to true.
// The flags mapperStarted and taskComplete are shared among all workers for simplicity. We do not care that they are shared since we want to test that
// at least one worker actually started launching one mapper execution, but no mapper ever completed.
// When the MapReduce function returns, which occurs after all the workers of the pool have been closed
// (which means that all the channels internal to the pool have been closed), we wait for a period (testDelay) where testDelay > workDuration.
// Since testDelay > workDuration, if the worker has not been stopped, then the worker would be able to set the flag taskComplete to true.
// After testDelay is passed, the test checks that the flag mapperStarted is true (to ensure that the workers actually started) and that the flag
// taskComplete is false (to ensure that they have been terminated)
func TestMapReduceWithTimeoutWorkersRunning(t *testing.T) {
	workDuration := 10 * time.Millisecond
	timeout := workDuration / 10
	testDelay := workDuration * 10

	mapperStarted := false
	taskComplete := false

	mapper := func(ctx context.Context, input int) (int, error) {
		mapperStarted = true
		timer := time.NewTimer(workDuration)
		select {
		// we simulate the work of the worker with a timer
		case <-timer.C: // timer fired, i.e. the worker has performed its task
			taskComplete = true
			mapResult := input * 10 // just do an operation - the result of the mapping is not important for the test
			return mapResult, nil
		case <-ctx.Done(): // the timeout signal is received
			return 0, ctx.Err()
		}
	}

	// a context with a timeout that is triggered with a not so short delay
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	concurrent := 3
	// initial value of the accumulator to pass to the Reduce function
	accInitialValue := 0
	// Call MapReduce - since the context timeout is fired before the mapper is able to complete its mapping work, there should be a non nil error
	_, err := MapReduce(ctx, concurrent, []int{0, 1, 2, 3, 4, 5, 6}, mapper, sumNumbers, accInitialValue)

	// wait to make sure that, if the mappers have not been terminated by the context timeout, they have the time to set the taskComplete to true
	time.Sleep(testDelay)

	// check the results of the test
	if !mapperStarted {
		t.Error("The mapper function has never started, i.e. the workerpool did not start its work")
	}
	if taskComplete {
		t.Error("The mapper function was not terminated by the context timeout")
	}
	if err == nil {
		t.Error("MapReduce has not returned an error after the context timeout was triggered")
	}
	if err != ctx.Err() {
		t.Errorf("MapReduce should have returned a context error - got %v", err)
	}
}
