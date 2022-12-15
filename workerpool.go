/*
Package workerpool implements a worker pool, i.e. a pool of goroutines which perform a specific processing on some input.

# Usage

Create the pool using the New function. The New function expects the size of the pool, i.e. the number of goroutines processing the input concurrently,
and a function which expects an input of type I and returns an output of type O or an error.

Once the pool has been created it can be started with the method Start().

A client can send a value (of type I) to the pool to be processed using the method Process(input I).

Once all values to be processed have been sent to the pool, the client can stop the pool using the method Stop().

There are different ways to process the results that the pool has produced.

# Process the results reading from the pool channels
A client can read the results produced by the pool from the channel OutCh and the errors from the channel ErrCh.

# Reduce the results into an accumulator
A client can reduce the results sent by the pool into an accumulator using the function Reduce.

# MapReduce
The MapReduce function implements the processing and the reduce operations in one function.

*/

package workerpool

import (
	"context"
	"sync"
)

// Pool implements a worker pool
type Pool[I, O any] struct {
	inCh          chan processInput[I]
	OutCh         chan O
	ErrCh         chan error
	doneWithInput *sync.WaitGroup
	size          int
	do            func(context.Context, I) (O, error)
	mu            *sync.Mutex
	status        PoolStatus
}
type PoolStatus string

const new = PoolStatus("New")
const started = PoolStatus("Started")
const stopped = PoolStatus("Stopped")

// processInput packs the input value and a context
type processInput[I any] struct {
	input I
	ctx   context.Context
}

// New creates a Pool and returns a pointer to it
func New[I, O any](size int, do func(ctx context.Context, input I) (O, error)) *Pool[I, O] {
	inCh := make(chan processInput[I])
	outCh := make(chan O)
	errCh := make(chan error)
	var doneWithInput sync.WaitGroup
	doneWithInput.Add(size)
	var mu sync.Mutex
	pool := Pool[I, O]{inCh, outCh, errCh, &doneWithInput, size, do, &mu, new}
	return &pool
}

// Start starts the pool
func (pool *Pool[I, O]) Start(ctx context.Context) {
	pool.mu.Lock()
	if pool.status == started {
		return
	}
	pool.status = started
	pool.mu.Unlock()
	for i := 0; i < pool.size; i++ {
		go func() { // these workers complete when pool.inCh is closed
			defer pool.doneWithInput.Done()
			for {
				select {
				case input, more := <-pool.inCh:
					if !more {
						return
					}
					output, e := pool.do(input.ctx, input.input)
					if e != nil {
						if ctx.Err() != nil {
							// it the context has signalled a termination signal, exit the worker
							return
						}
						pool.ErrCh <- e
						continue
					}
					pool.OutCh <- output
				case <-ctx.Done():
					return
				}
			}
		}()
	}
}

// Process sends one value to the pool to be processed by the first available worker.
func (pool *Pool[I, O]) Process(ctx context.Context, input I) {
	pool.inCh <- processInput[I]{input, ctx}
}

// Stop stops the pool
// After the pool is stopped no other input value can be processed
func (pool *Pool[I, O]) Stop() {
	pool.mu.Lock()
	if pool.status == stopped {
		return
	}
	pool.status = stopped
	pool.mu.Unlock()
	// close the input channel
	close(pool.inCh)
	// wait for all the values sent to the input channel to go through the processing made by the pool
	pool.doneWithInput.Wait()
	// close the output and the error channels
	close(pool.OutCh)
	close(pool.ErrCh)
}

// GetStatus returns the status of the pool
func (pool *Pool[I, O]) GetStatus() PoolStatus {
	var st PoolStatus
	pool.mu.Lock()
	st = pool.status
	pool.mu.Unlock()
	return st
}
