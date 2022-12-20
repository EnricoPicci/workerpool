# MapReduce

This package implements a concurrent version of the Map-Reduce logic.

The MapReduce function receives a slice of values, a mapper function and a reducer function. All values are transformed using the mapper function and then, the results of the transformations are reduced to a single value by the reducer function.

MapReduce returns the result of the reducing logic or an error, if an error occurs.

The map logic leverage a [workerpool](../workerpool.go) to run concurrently.

A context is passed to the MapReduce function. If the context is cancelled or if it timeouts, then the execution of the MapReduce logic is gracefully terminated and an error is returned.

This package implements also a Reduce function that is passed a reducer function and a [workerpool](../workerpool.go). The Reduce function reduces the results channeled by the workerpool to a single value.
