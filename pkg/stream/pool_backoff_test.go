package stream

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

// recordingClock stands in for time so the health-check tests can assert on
// the backoff without spending it. Sleeping in a test is a flake waiting to
// happen; recording the requested delay is exact and instant.
type recordingClock struct {
	mu    sync.Mutex
	slept []time.Duration
}

func (c *recordingClock) Now() time.Time { return time.Now() }

func (c *recordingClock) Sleep(_ context.Context, d time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.slept = append(c.slept, d)
	return nil
}

func (c *recordingClock) delays() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.slept...)
}

// partitionedClient is an rpc.Client that is still unreachable, so a health
// probe against it fails with a retryable error the way a partitioned
// endpoint would.
type partitionedClient struct {
	rpc.Client
}

func (partitionedClient) GetLatestLedger(context.Context) (protocol.GetLatestLedgerResponse, error) {
	return protocol.GetLatestLedgerResponse{}, errors.New("dial tcp: connection refused")
}

// newBackoffPool builds a Pool with only the fields the delay arithmetic and
// the never-dialling health-check paths read, so these tests stay pure unit
// tests rather than quietly depending on a client.
func newBackoffPool(clock Clock, initial, maxDelay time.Duration, factor float64, jitter bool) *Pool {
	return &Pool{
		backoffInitial: initial,
		backoffMax:     maxDelay,
		backoffFactor:  factor,
		backoffJitter:  jitter,
		clock:          clock,
	}
}

// TestBackoffDelayGrowsExponentiallyAndCaps pins the shape of the delay: it
// doubles per failure and stops at the ceiling, so a long partition does not
// park an endpoint out of rotation for minutes.
func TestBackoffDelayGrowsExponentiallyAndCaps(t *testing.T) {
	t.Parallel()

	p := newBackoffPool(RealClock{}, 100*time.Millisecond, 800*time.Millisecond, 2, false)

	want := []time.Duration{
		100 * time.Millisecond, // failure 0
		200 * time.Millisecond, // failure 1
		400 * time.Millisecond, // failure 2
		800 * time.Millisecond, // failure 3
		800 * time.Millisecond, // failure 4 — capped
	}
	for attempt, expected := range want {
		if got := p.backoffDelay(attempt); got != expected {
			t.Errorf("backoffDelay(%d) = %v, want %v", attempt, got, expected)
		}
	}
}

// TestBackoffDelayJitterSpreadsReconnects checks the anti-thundering-herd
// property: jitter keeps every delay inside [d/2, d] — enough variance that
// pools recovering from the same partition do not reconnect in lockstep, but
// never longer than the unjittered delay.
func TestBackoffDelayJitterSpreadsReconnects(t *testing.T) {
	t.Parallel()

	plain := newBackoffPool(RealClock{}, time.Second, time.Minute, 2, false)
	jittered := newBackoffPool(RealClock{}, time.Second, time.Minute, 2, true)

	for attempt := range 5 {
		want := plain.backoffDelay(attempt)
		for range 200 {
			got := jittered.backoffDelay(attempt)
			if got < want/2 || got > want {
				t.Fatalf("backoffDelay(%d) with jitter = %v, want within [%v, %v]",
					attempt, got, want/2, want)
			}
		}
	}
}

// TestBackoffDelaySurvivesBadConfiguration proves the guards on the knobs: a
// zero or nonsensical configuration falls back to the defaults rather than
// producing a zero (or negative) delay that never backs off at all.
func TestBackoffDelaySurvivesBadConfiguration(t *testing.T) {
	t.Parallel()

	empty := &Pool{}
	if got := empty.backoffDelay(3); got <= 0 {
		t.Errorf("backoffDelay() on a zero-value Pool = %v, want a positive default", got)
	}

	badFactor := newBackoffPool(RealClock{}, time.Second, time.Minute, 0.5, false)
	if got, want := badFactor.backoffDelay(2), 4*time.Second; got != want {
		t.Errorf("backoffDelay() with factor < 1 = %v, want the default factor applied (%v)", got, want)
	}

	negative := newBackoffPool(RealClock{}, -time.Second, -time.Minute, 2, false)
	if got := negative.backoffDelay(2); got <= 0 {
		t.Errorf("backoffDelay() with negative configuration = %v, want a positive default", got)
	}
}

// TestWithBackoffAppliesToPool keeps the option honest about which fields it
// configures, since a mismatch here would silently disable the backoff.
func TestWithBackoffAppliesToPool(t *testing.T) {
	t.Parallel()

	p, err := NewPool([]string{"https://a.example"},
		WithHealthInterval(0),
		WithClientFactory(func(string) rpc.Client { return nil }),
		WithBackoff(time.Second, time.Minute, 3, false))
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	if p.backoffInitial != time.Second || p.backoffMax != time.Minute ||
		p.backoffFactor != 3 || p.backoffJitter {
		t.Errorf("WithBackoff() left backoff=(%v, %v, %v, %v), want the supplied configuration",
			p.backoffInitial, p.backoffMax, p.backoffFactor, p.backoffJitter)
	}
	if got, want := p.backoffDelay(1), 3*time.Second; got != want {
		t.Errorf("backoffDelay(1) = %v, want %v from the configured factor", got, want)
	}
}

// TestHealthCheckSkipsBackoffWhenPoolIsHealthy: there is nothing to spread out
// once every endpoint is serving, so a health check over a healthy pool must
// not sit through a backoff delay.
func TestHealthCheckSkipsBackoffWhenPoolIsHealthy(t *testing.T) {
	t.Parallel()

	clock := &recordingClock{}
	p := newBackoffPool(clock, 30*time.Second, time.Minute, 2, true)
	p.endpoints = []*endpoint{
		{url: "https://a.example", healthy: true},
		{url: "https://b.example", healthy: true},
	}

	p.HealthCheck(context.Background())

	if slept := clock.delays(); len(slept) != 0 {
		t.Errorf("HealthCheck() over a healthy pool slept %v, want no backoff", slept)
	}
}

// TestHealthCheckBacksOffWhileEndpointsAreDown: with an endpoint still down
// the probe cycle waits out the backoff, which is what stops every pool that
// lost the same partition from reconnecting in lockstep.
func TestHealthCheckBacksOffWhileEndpointsAreDown(t *testing.T) {
	t.Parallel()

	clock := &recordingClock{}
	p := newBackoffPool(clock, 100*time.Millisecond, time.Second, 2, false)
	p.endpoints = []*endpoint{
		{url: "https://a.example", client: partitionedClient{}, healthy: false, failures: 1},
	}

	p.HealthCheck(context.Background())

	// The failed probe takes the endpoint to two failures, so the next wait is
	// initial * factor^2 = 400ms.
	slept := clock.delays()
	if len(slept) != 1 {
		t.Fatalf("HealthCheck() slept %d times, want exactly one backoff after the cycle", len(slept))
	}
	if want := 400 * time.Millisecond; slept[0] != want {
		t.Errorf("backoff = %v, want %v", slept[0], want)
	}
	if p.endpoints[0].healthy {
		t.Error("endpoint was marked healthy after a failed probe")
	}
}

// TestFailureStateSummarisesThePool keeps the backoff input honest: the delay
// is driven by the worst endpoint, and no sleep happens once every endpoint
// has recovered.
func TestFailureStateSummarisesThePool(t *testing.T) {
	t.Parallel()

	p := newBackoffPool(&recordingClock{}, time.Second, time.Minute, 2, false)
	p.endpoints = []*endpoint{
		{url: "https://a.example", healthy: true},
		{url: "https://b.example", healthy: false, failures: 4},
	}

	failures, down := p.failureState()
	if !down {
		t.Error("failureState() down = false, want true while an endpoint is unhealthy")
	}
	if failures != 4 {
		t.Errorf("failureState() failures = %d, want 4 from the worst endpoint", failures)
	}

	p.endpoints[1].healthy = true
	if _, down := p.failureState(); down {
		t.Error("failureState() down = true after recovery, want false")
	}
}
