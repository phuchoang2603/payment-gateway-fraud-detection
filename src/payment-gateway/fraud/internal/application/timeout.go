package application

import (
	"context"
	"errors"
	"net"
)

// TimedOut reports whether a failed call ran out of time, by its context's deadline or by a timeout error.
func TimedOut(ctx context.Context, err error) bool {
	var timeout net.Error
	return errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) || errors.As(err, &timeout) && timeout.Timeout()
}
