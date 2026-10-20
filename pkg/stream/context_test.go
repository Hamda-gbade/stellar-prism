package stream_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func TestIsCancellation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"canceled", context.Canceled, true},
		{"deadline", context.DeadlineExceeded, true},
		{"wrapped canceled", errors.Join(errors.New("stream"), context.Canceled), true},
		{"ordinary error", errors.New("connection reset"), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := stream.IsCancellation(tc.err); got != tc.want {
				t.Errorf("IsCancellation(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestTaskGroupCancelsSiblingsOnError(t *testing.T) {
	t.Parallel()

	group := stream.NewTaskGroup(context.Background())
	wantErr := errors.New("worker failed")
	siblingDone := make(chan struct{})

	group.Go(func(context.Context) error { return wantErr })
	group.Go(func(ctx context.Context) error {
		<-ctx.Done()
		close(siblingDone)
		return ctx.Err()
	})

	if err := group.Wait(); !errors.Is(err, wantErr) {
		t.Fatalf("Wait() error = %v, want %v", err, wantErr)
	}

	select {
	case <-siblingDone:
	case <-time.After(time.Second):
		t.Fatal("sibling task was not cancelled")
	}
}

func TestTaskGroupCancellationIsNotAnError(t *testing.T) {
	t.Parallel()

	parent, cancel := context.WithCancel(context.Background())
	group := stream.NewTaskGroup(parent)
	done := make(chan struct{})
	group.Go(func(ctx context.Context) error {
		defer close(done)
		<-ctx.Done()
		return ctx.Err()
	})

	cancel()
	if err := group.Wait(); err != nil {
		t.Fatalf("Wait() after parent cancellation = %v, want nil", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("task did not observe parent cancellation")
	}
}

func TestTaskGroupCancelStopsTasks(t *testing.T) {
	t.Parallel()

	group := stream.NewTaskGroup(context.Background())
	done := make(chan struct{})
	group.Go(func(ctx context.Context) error {
		defer close(done)
		<-ctx.Done()
		return ctx.Err()
	})

	group.Cancel()
	if err := group.Wait(); err != nil {
		t.Fatalf("Wait() = %v, want nil", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Cancel() did not stop the task")
	}
	if group.Context().Err() == nil {
		t.Error("group context was not cancelled after Wait")
	}
}

func TestNewTaskGroupHandlesNilParent(t *testing.T) {
	t.Parallel()

	var parent context.Context
	group := stream.NewTaskGroup(parent)
	if group.Context() == nil {
		t.Fatal("Context() = nil, want a background-derived context")
	}
	done := make(chan struct{})
	group.Go(func(ctx context.Context) error {
		defer close(done)
		if ctx == nil {
			t.Error("task context = nil")
		}
		return nil
	})
	if err := group.Wait(); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	<-done
}
