package mapreduce

import "context"

type Reducable[I, O, R any] []I

func (values Reducable[I, O, R]) MapReduce(
	ctx context.Context,
	mapper func(I) (O, error),
	reducer func(R, O) R,
	seed R,
	concurrent int,
) (R, error) {
	if concurrent < 1 {
		panic("concurrent must be greater than 0")
	}
	res, err := MapReduce(ctx, concurrent, values, mapper, reducer, seed)
	return res, err
}
