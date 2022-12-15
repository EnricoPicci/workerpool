/*
# Reduce the results into an accumulator
A client can reduce the results sent by the pool into an accumulator using the function Reduce.

# MapReduce
The MapReduce function implements the processing and the reduce operations in one function.

*/

package workerpool

import (
	"context"
	"fmt"
)

type ReduceError struct {
	Errors []error
}

func (err ReduceError) Error() string {
	return fmt.Sprintf("%v errors while reducing", len(err.Errors))
}

// Reduce the results returned by the processing of the pool into an accumulator. Returns the accumulator and a slice of errors, if errors occur
func Reduce[I, O, R any](ctx context.Context, pool *Pool[I, O], reducer func(R, O) R, initialValue R) (R, error) {
	acc, err := reduce(ctx, pool, reducer, initialValue)

	return acc, err
}

// MapReduce process all the input values and returns a reduced result.
// If errors occur, an error wrapping all the errors is returned.
func MapReduce[I, O, R any](
	ctx context.Context,
	concurrent int,
	inputValues []I,
	mapper func(context.Context, I) (O, error),
	reducer func(R, O) R,
	initialValue R,
) (R, error) {
	// create and start the pool
	pool := New(concurrent, mapper)
	pool.Start(ctx)

	// launch a goroutine that sends the input values to the pool. When all the values have been sent or the context signals, the pool is stopped
	go func() {
		defer pool.Stop()
		for _, v := range inputValues {
			pool.Process(v)
			if ctx.Err() != nil {
				return
			}
		}
	}()

	acc, err := reduce(ctx, pool, reducer, initialValue)

	return acc, err
}

func reduce[I, O, R any](ctx context.Context, pool *Pool[I, O], reducer func(R, O) R, acc R) (R, error) {
	errors := []error{}
	var err error

	for {
		closed := false
		select {
		case res, more := <-pool.OutCh:
			closed = !more
			if closed {
				break
			}
			acc = reducer(acc, res)
		case err, more := <-pool.ErrCh:
			if more {
				errors = append(errors, err)
			}
		case <-ctx.Done():
			return acc, ctx.Err()
		}
		if closed {
			break
		}
	}

	if len(errors) > 0 {
		err = ReduceError{errors}
	}

	return acc, err
}
