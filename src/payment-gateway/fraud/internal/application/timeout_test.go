package application

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

type netFailure struct{ timeout bool }

func (e netFailure) Error() string   { return "network failure" }
func (e netFailure) Timeout() bool   { return e.timeout }
func (e netFailure) Temporary() bool { return false }

func TestTimedOutRecognisesEveryWayACallRunsOutOfTime(t *testing.T) {
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	cancelled, stop := context.WithCancel(context.Background())
	stop()
	for name, test := range map[string]struct {
		ctx  context.Context
		err  error
		want bool
	}{
		"deadline passed":       {expired, errors.New("failed"), true},
		"wrapped deadline":      {context.Background(), fmt.Errorf("call: %w", context.DeadlineExceeded), true},
		"network timeout":       {context.Background(), fmt.Errorf("call: %w", netFailure{timeout: true}), true},
		"network error":         {context.Background(), netFailure{}, false},
		"other error":           {context.Background(), errors.New("failed"), false},
		"cancelled not timeout": {cancelled, context.Canceled, false},
	} {
		if got := TimedOut(test.ctx, test.err); got != test.want {
			t.Errorf("%s: TimedOut = %v, want %v", name, got, test.want)
		}
	}
}
