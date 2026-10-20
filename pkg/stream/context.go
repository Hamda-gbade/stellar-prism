package stream

import (
	"context"
	"errors"
	"sync"
)

// IsCancellation reports whether err means the caller asked the stream to
// stop. Cancellation is a clean shutdown, not a failure.
func IsCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// TaskGroup runs a set of goroutines under one context and guarantees that
// none of them outlive Wait.
//
// It fills the same role as errgroup.Group, without adding a dependency for
// the one caller that needs it. The first non-cancellation error cancels the
// group's context; Wait then returns that error after every goroutine has
// returned. A parent cancellation is deliberately not reported as an error:
// it is an operator action, and callers distinguish it with ctx.Err.
type TaskGroup struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	failOnce sync.Once
	mu       sync.Mutex
	err      error
}

// NewTaskGroup returns a TaskGroup derived from parent.
func NewTaskGroup(parent context.Context) *TaskGroup {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	return &TaskGroup{ctx: ctx, cancel: cancel}
}

// Context returns the context shared by every task in the group.
func (g *TaskGroup) Context() context.Context { return g.ctx }

// Go starts fn in a new goroutine. The context passed to fn is cancelled as
// soon as any task fails or Cancel is called.
func (g *TaskGroup) Go(fn func(context.Context) error) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		if err := fn(g.ctx); err != nil && !IsCancellation(err) {
			g.fail(err)
		}
	}()
}

// Cancel stops the group without recording an error. It is safe to call more
// than once.
func (g *TaskGroup) Cancel() { g.cancel() }

// Wait blocks until every task has returned, then cancels the shared context
// so any context-aware resources held by the tasks are released. It returns
// the first non-cancellation error, or nil for a clean finish.
func (g *TaskGroup) Wait() error {
	g.wg.Wait()
	g.cancel()

	g.mu.Lock()
	defer g.mu.Unlock()
	return g.err
}

func (g *TaskGroup) fail(err error) {
	g.failOnce.Do(func() {
		g.mu.Lock()
		g.err = err
		g.mu.Unlock()
		g.cancel()
	})
}
