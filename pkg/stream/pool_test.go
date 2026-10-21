package stream_test

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// poolFake is a minimal rpc.Client whose behaviour can be changed while a
// test runs. Only the methods the pool tests exercise are implemented;
// embedding the interface keeps the rest out of the way.
type poolFake struct {
	rpc.Client

	mu         sync.Mutex
	err        error
	latest     uint32
	healthResp protocol.GetHealthResponse
	calls      int
	closed     int
}

func (f *poolFake) GetLatestLedger(ctx context.Context) (protocol.GetLatestLedgerResponse, error) {
	if err := ctx.Err(); err != nil {
		return protocol.GetLatestLedgerResponse{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return protocol.GetLatestLedgerResponse{}, f.err
	}
	return protocol.GetLatestLedgerResponse{Sequence: f.latest}, nil
}

func (f *poolFake) GetHealth(ctx context.Context) (protocol.GetHealthResponse, error) {
	if err := ctx.Err(); err != nil {
		return protocol.GetHealthResponse{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return protocol.GetHealthResponse{}, f.err
	}
	return f.healthResp, nil
}

func (f *poolFake) GetEvents(ctx context.Context, _ protocol.GetEventsRequest) (protocol.GetEventsResponse, error) {
	if err := ctx.Err(); err != nil {
		return protocol.GetEventsResponse{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return protocol.GetEventsResponse{}, f.err
	}
	return protocol.GetEventsResponse{LatestLedger: f.latest}, nil
}

func (f *poolFake) GetEventsBatch(ctx context.Context, reqs []protocol.GetEventsRequest) ([]protocol.GetEventsResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	out := make([]protocol.GetEventsResponse, 0, len(reqs))
	for range reqs {
		out = append(out, protocol.GetEventsResponse{LatestLedger: f.latest})
	}
	return out, nil
}

func (f *poolFake) GetLedgers(ctx context.Context, _ protocol.GetLedgersRequest) (protocol.GetLedgersResponse, error) {
	if err := ctx.Err(); err != nil {
		return protocol.GetLedgersResponse{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return protocol.GetLedgersResponse{}, f.err
	}
	return protocol.GetLedgersResponse{LatestLedger: f.latest}, nil
}

func (f *poolFake) GetTransaction(ctx context.Context, _ protocol.GetTransactionRequest) (protocol.GetTransactionResponse, error) {
	if err := ctx.Err(); err != nil {
		return protocol.GetTransactionResponse{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return protocol.GetTransactionResponse{}, f.err
	}
	return protocol.GetTransactionResponse{LatestLedger: f.latest}, nil
}

func (f *poolFake) GetTransactions(ctx context.Context, _ protocol.GetTransactionsRequest) (protocol.GetTransactionsResponse, error) {
	if err := ctx.Err(); err != nil {
		return protocol.GetTransactionsResponse{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return protocol.GetTransactionsResponse{}, f.err
	}
	return protocol.GetTransactionsResponse{LatestLedger: f.latest}, nil
}

func (f *poolFake) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed++
	return nil
}

func (f *poolFake) setError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *poolFake) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *poolFake) closeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

// newFakePool builds a pool whose URL maps to the supplied fake. The
// background probe is disabled unless a test opts back in, keeping the suite
// deterministic and free of timing races.
func newFakePool(t *testing.T, fakes map[string]*poolFake, opts ...stream.PoolOption) *stream.Pool {
	t.Helper()

	urls := make([]string, 0, len(fakes))
	for url := range fakes {
		urls = append(urls, url)
	}
	// Sort so the first endpoint is deterministic: several tests rely on the
	// failing endpoint being tried before the healthy one.
	sort.Strings(urls)
	opts = append([]stream.PoolOption{
		stream.WithHealthInterval(0),
		stream.WithClientFactory(func(url string) rpc.Client {
			return fakes[url]
		}),
	}, opts...)

	p, err := stream.NewPool(urls, opts...)
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func TestPoolRejectsNoEndpoints(t *testing.T) {
	t.Parallel()

	if _, err := stream.NewPool(nil); err == nil {
		t.Fatal("NewPool(nil) error = nil, want a validation failure")
	}
	if _, err := stream.NewPool([]string{"", "   "}); err == nil {
		t.Fatal("NewPool(blank URLs) error = nil, want a validation failure")
	}
}

func TestPoolDeduplicatesURLs(t *testing.T) {
	t.Parallel()

	fake := &poolFake{latest: 7}
	p, err := stream.NewPool(
		[]string{"https://a.example", " https://a.example ", "https://a.example"},
		stream.WithHealthInterval(0),
		stream.WithClientFactory(func(string) rpc.Client { return fake }))
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	if got := p.URLs(); len(got) != 1 {
		t.Fatalf("URLs() = %v, want one deduplicated endpoint", got)
	}
}

func TestPoolRoundRobins(t *testing.T) {
	t.Parallel()

	fakes := map[string]*poolFake{
		"https://a.example": {latest: 1},
		"https://b.example": {latest: 2},
		"https://c.example": {latest: 3},
	}
	p, err := stream.NewPool([]string{"https://a.example", "https://b.example", "https://c.example"},
		stream.WithHealthInterval(0),
		stream.WithClientFactory(func(url string) rpc.Client { return fakes[url] }))
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	for range 9 {
		if _, err := p.GetLatestLedger(context.Background()); err != nil {
			t.Fatalf("GetLatestLedger() error = %v", err)
		}
	}
	for url, fake := range fakes {
		if got := fake.callCount(); got != 3 {
			t.Errorf("%s handled %d calls, want 3", url, got)
		}
	}
}

func TestPoolFailsOverAndSkipsUnhealthyEndpoint(t *testing.T) {
	t.Parallel()

	bad := &poolFake{err: errors.New("dial tcp: connection refused")}
	good := &poolFake{latest: 42}
	p := newFakePool(t, map[string]*poolFake{
		"https://bad.example":  bad,
		"https://good.example": good,
	})

	resp, err := p.GetLatestLedger(context.Background())
	if err != nil {
		t.Fatalf("GetLatestLedger() error = %v, want failover to succeed", err)
	}
	if resp.Sequence != 42 {
		t.Errorf("Sequence = %d, want 42 from the healthy endpoint", resp.Sequence)
	}
	if got := p.Healthy(); got != 1 {
		t.Errorf("Healthy() = %d, want 1 after the failure", got)
	}

	// The failed endpoint is in cooldown, so subsequent calls should not even
	// try it. This is what keeps a dead backend from doubling every request.
	badCalls := bad.callCount()
	for range 3 {
		if _, err := p.GetLatestLedger(context.Background()); err != nil {
			t.Fatalf("GetLatestLedger() error = %v", err)
		}
	}
	if bad.callCount() != badCalls {
		t.Errorf("unhealthy endpoint was retried %d time(s) during cooldown", bad.callCount()-badCalls)
	}
}

func TestPoolRecoversEndpointThroughHealthCheck(t *testing.T) {
	t.Parallel()

	bad := &poolFake{err: errors.New("connection reset by peer")}
	good := &poolFake{latest: 9}
	p := newFakePool(t, map[string]*poolFake{
		"https://bad.example":  bad,
		"https://good.example": good,
	})

	if _, err := p.GetLatestLedger(context.Background()); err != nil {
		t.Fatalf("GetLatestLedger() error = %v", err)
	}
	if got := p.Healthy(); got != 1 {
		t.Fatalf("Healthy() = %d, want 1 before recovery", got)
	}

	bad.setError(nil)
	p.HealthCheck(context.Background())

	if got := p.Healthy(); got != 2 {
		t.Errorf("Healthy() = %d, want 2 after the probe succeeded", got)
	}
}

func TestPoolHealthCheckLeavesHealthyEndpointsAlone(t *testing.T) {
	t.Parallel()

	fake := &poolFake{latest: 1}
	p := newFakePool(t, map[string]*poolFake{"https://a.example": fake})

	p.HealthCheck(context.Background())
	if got := fake.callCount(); got != 0 {
		t.Errorf("probed a healthy endpoint %d time(s), want 0", got)
	}
}

func TestPoolPermanentErrorStopsFailover(t *testing.T) {
	t.Parallel()

	bad := &poolFake{err: rpc.Permanent(errors.New("invalid cursor"))}
	good := &poolFake{latest: 1}
	p := newFakePool(t, map[string]*poolFake{
		"https://bad.example":  bad,
		"https://good.example": good,
	})

	_, err := p.GetLatestLedger(context.Background())
	if !rpc.IsPermanent(err) {
		t.Fatalf("error = %v, want a permanent failure", err)
	}
	if got := good.callCount(); got != 0 {
		t.Errorf("tried another endpoint %d time(s) after a permanent error", got)
	}
}

func TestPoolReportsEveryEndpointFailure(t *testing.T) {
	t.Parallel()

	p := newFakePool(t, map[string]*poolFake{
		"https://a.example": {err: errors.New("503 Service Unavailable")},
		"https://b.example": {err: errors.New("504 Gateway Timeout")},
	})

	_, err := p.GetEvents(context.Background(), protocol.GetEventsRequest{})
	if err == nil {
		t.Fatal("GetEvents() error = nil, want an error")
	}
	for _, want := range []string{"https://a.example", "https://b.example"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %s", err, want)
		}
	}
	if !rpc.IsRetryable(err) {
		t.Errorf("error = %v, want it to remain retryable for the tailer", err)
	}
}

func TestPoolReturnsCancellationWithoutTryingEndpoints(t *testing.T) {
	t.Parallel()

	fake := &poolFake{latest: 1}
	p := newFakePool(t, map[string]*poolFake{"https://a.example": fake})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := p.GetLatestLedger(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if got := fake.callCount(); got != 0 {
		t.Errorf("called the endpoint %d time(s) with a cancelled context", got)
	}
}

func TestPoolImplementsEveryClientMethod(t *testing.T) {
	t.Parallel()

	fake := &poolFake{latest: 11}
	p := newFakePool(t, map[string]*poolFake{"https://a.example": fake})

	if _, err := p.GetLedgers(context.Background(), protocol.GetLedgersRequest{}); err != nil {
		t.Errorf("GetLedgers() error = %v", err)
	}
	if _, err := p.GetTransaction(context.Background(), protocol.GetTransactionRequest{}); err != nil {
		t.Errorf("GetTransaction() error = %v", err)
	}
	if _, err := p.GetTransactions(context.Background(), protocol.GetTransactionsRequest{}); err != nil {
		t.Errorf("GetTransactions() error = %v", err)
	}
	if _, err := p.GetEventsBatch(context.Background(), make([]protocol.GetEventsRequest, 2)); err != nil {
		t.Errorf("GetEventsBatch() error = %v", err)
	}
	if got := fake.callCount(); got != 4 {
		t.Errorf("endpoint handled %d calls, want 4", got)
	}
}

// TestPoolGetEventsBatchFailsOverAndKeepsOrder: a batch is one call as far as
// the pool is concerned, so an unhealthy first endpoint is skipped and every
// requested entry still comes back, in order.
func TestPoolGetEventsBatchFailsOverAndKeepsOrder(t *testing.T) {
	t.Parallel()

	bad := &poolFake{err: errors.New("dial tcp: connection refused")}
	good := &poolFake{latest: 7}
	p := newFakePool(t, map[string]*poolFake{
		"https://bad.example":  bad,
		"https://good.example": good,
	})

	responses, err := p.GetEventsBatch(context.Background(), make([]protocol.GetEventsRequest, 3))
	if err != nil {
		t.Fatalf("GetEventsBatch() error = %v, want failover to succeed", err)
	}
	if len(responses) != 3 {
		t.Fatalf("GetEventsBatch() returned %d responses, want one per request", len(responses))
	}
	for i, resp := range responses {
		if resp.LatestLedger != 7 {
			t.Errorf("responses[%d].LatestLedger = %d, want 7 from the healthy endpoint", i, resp.LatestLedger)
		}
	}
	if got := bad.callCount(); got != 1 {
		t.Errorf("unhealthy endpoint handled %d calls, want 1 before failover", got)
	}
}

// TestPoolGetEventsBatchIsRetryable: the tailer decides whether to back off
// based on this, so a batch failure must not be misclassified as permanent.
func TestPoolGetEventsBatchIsRetryable(t *testing.T) {
	t.Parallel()

	p := newFakePool(t, map[string]*poolFake{
		"https://a.example": {err: errors.New("503 Service Unavailable")},
	})

	_, err := p.GetEventsBatch(context.Background(), make([]protocol.GetEventsRequest, 1))
	if err == nil {
		t.Fatal("GetEventsBatch() error = nil, want an error")
	}
	if !rpc.IsRetryable(err) {
		t.Errorf("error = %v, want it to remain retryable for the tailer", err)
	}
}

func TestPoolGetHealthFailsOver(t *testing.T) {
	t.Parallel()

	bad := &poolFake{err: errors.New("connection refused")}
	good := &poolFake{healthResp: protocol.GetHealthResponse{
		Status:       "healthy",
		LatestLedger: 99,
	}}
	p := newFakePool(t, map[string]*poolFake{
		"https://bad.example":  bad,
		"https://good.example": good,
	})

	var _ rpc.HealthClient = p

	resp, err := p.GetHealth(context.Background())
	if err != nil {
		t.Fatalf("GetHealth() error = %v, want failover to succeed", err)
	}
	if resp.Status != "healthy" || resp.LatestLedger != 99 {
		t.Errorf("GetHealth() = %+v, want the healthy endpoint's response", resp)
	}
	if got := p.Healthy(); got != 1 {
		t.Errorf("Healthy() = %d, want 1 after the failure", got)
	}
}

func TestPoolGetHealthRejectsClientWithoutCapability(t *testing.T) {
	t.Parallel()

	p, err := stream.NewPool([]string{"https://a.example"},
		stream.WithHealthInterval(0),
		stream.WithClientFactory(func(string) rpc.Client { return &closeErrFake{} }))
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	_, err = p.GetHealth(context.Background())
	if !errors.Is(err, rpc.ErrHealthUnsupported) {
		t.Fatalf("GetHealth() error = %v, want ErrHealthUnsupported", err)
	}
	if !rpc.IsPermanent(err) {
		t.Errorf("error = %v, want the missing capability to be permanent", err)
	}
}

func TestPoolOptionsIgnoreNonPositiveValues(t *testing.T) {
	t.Parallel()

	fake := &poolFake{latest: 1}
	p, err := stream.NewPool([]string{"https://a.example"},
		stream.WithHealthInterval(0),
		stream.WithHealthTimeout(0),
		stream.WithFailureCooldown(0),
		stream.WithClientFactory(func(string) rpc.Client { return fake }))
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	// Zero-value options must leave the defaults intact, so a probe still
	// times out rather than hanging forever.
	if _, err := p.GetLatestLedger(context.Background()); err != nil {
		t.Fatalf("GetLatestLedger() error = %v", err)
	}
}

func TestPoolTriesAnEndpointWhenAllAreCoolingDown(t *testing.T) {
	t.Parallel()

	bad := &poolFake{err: errors.New("connection refused")}
	p := newFakePool(t, map[string]*poolFake{"https://only.example": bad})

	// The first failure puts the only endpoint into cooldown. A second call
	// must still issue a request rather than failing without trying: a stale
	// health mark is more likely than a total outage.
	if _, err := p.GetLatestLedger(context.Background()); err == nil {
		t.Fatal("first GetLatestLedger() error = nil, want a failure")
	}
	if got := bad.callCount(); got != 1 {
		t.Fatalf("endpoint handled %d calls after the first failure, want 1", got)
	}

	if _, err := p.GetLatestLedger(context.Background()); err == nil {
		t.Fatal("second GetLatestLedger() error = nil, want a failure")
	}
	if got := bad.callCount(); got != 2 {
		t.Errorf("endpoint handled %d calls while cooling down, want 2 (must still be tried)", got)
	}
}

func TestPoolCloseReportsClientCloseErrors(t *testing.T) {
	t.Parallel()

	failing := &closeErrFake{err: errors.New("boom")}
	p, err := stream.NewPool([]string{"https://a.example"},
		stream.WithHealthInterval(0),
		stream.WithClientFactory(func(string) rpc.Client { return failing }))
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}

	if err := p.Close(); err == nil {
		t.Fatal("Close() error = nil, want the client close failure")
	} else if !strings.Contains(err.Error(), "boom") {
		t.Errorf("error = %v, want it to name the underlying failure", err)
	}
}

// closeErrFake is a client whose Close always fails, so Pool.Close can be
// checked to propagate the failure.
type closeErrFake struct {
	rpc.Client
	err error
}

func (f *closeErrFake) Close() error { return f.err }

func TestPoolCloseStopsProbeAndClosesClients(t *testing.T) {
	t.Parallel()

	fake := &poolFake{latest: 1}
	p, err := stream.NewPool([]string{"https://a.example"},
		stream.WithHealthInterval(time.Millisecond),
		stream.WithClientFactory(func(string) rpc.Client { return fake }))
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second Close() error = %v, want idempotence", err)
	}
	if got := fake.closeCount(); got != 1 {
		t.Errorf("underlying client closed %d time(s), want 1", got)
	}
}
