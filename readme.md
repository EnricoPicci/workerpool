# Workerpool

Package workerpool implements a worker pool, i.e. a pool of goroutines (workers) which perform a specific processing on some input.

The workers read the input they have to process from a channel and write the result of the processing on an output channel. If an error occurs, the error is written to an error channel.

# Usage

Create the pool using the New function. The New function expects the size of the pool, i.e. the number of goroutines processing the input concurrently,
and a function which expects a context.Context and an input of type I and returns an output of type O or an error.

Once the pool has been created it can be started with the method Start(context.Context). The context is used to terminate the workerpool if the context is cancelled or timeouts.

A client can send a value (of type I) to the pool to be processed using the method Process(input I).

Once all values to be processed have been sent to the pool, the client can stop the pool using the method Stop().

# Process the results reading from the pool channels

A client can read the results produced by the pool from the channel OutCh and the errors from the channel ErrCh.

# Reduce and MapReduce

The [mapreduce](./mapreduce/) package provides two functions, Reduce and MapReduce, that use a workerpool to implement the typical reduce and mapReduce logic in a concurrent way.
