package cli

import (
	"errors"
	"strings"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// ExitCodeError is the error type carried by prism exit paths.
// The code maps directly to UNIX conventions: 0=success, 1=generic,
// 2=gap (records lost behind retention window), 3=auth, 4=network.
type ExitCodeError struct {
	Code    int
	Message string
}

func (e *ExitCodeError) Error() string { return e.Message }

// ExitCode maps an error to a process exit code following UNIX conventions.
//
//	0 - success (no error)
//	1 - generic error / usage error
//	2 - gap (records lost behind retention window)
//	3 - authentication error
//	4 - network error (RPC unreachable, timeout, etc.)
func ExitCode(err error) (code int, printable bool) {
	if err == nil {
		return 0, false
	}

	var ee *ExitCodeError
	if errors.As(err, &ee) {
		return ee.Code, false
	}

	// Check for specific error types to map to exit codes
	switch {
	case isAuthError(err):
		return 3, false
	case isNetworkError(err):
		return 4, false
	default:
		return 1, true
	}
}

// IsQuiet checks whether the given error was produced in quiet mode,
// meaning its output was suppressed and should not be printed.
func IsQuiet(err error) bool {
	var ee *ExitCodeError
	if !errors.As(err, &ee) {
		return false
	}
	return ee.Code != 0 // quiet mode suppresses all non-zero exit codes
}

// error helpers for categorisation

func isAuthError(err error) bool {
	errStr := err.Error()
	lower := strings.ToLower(errStr)
	return strings.Contains(lower, "auth") ||
		strings.Contains(lower, "unauthorized") ||
		strings.Contains(lower, "credentials") ||
		strings.Contains(lower, "token") ||
		strings.Contains(lower, "api key")
}

func isNetworkError(err error) bool {
	errStr := err.Error()
	lower := strings.ToLower(errStr)
	return strings.Contains(lower, "network") ||
		strings.Contains(lower, "connection") ||
		strings.Contains(lower, "rpc") ||
		strings.Contains(lower, "timeout") ||
		strings.Contains(lower, "dial") ||
		strings.Contains(lower, "fetch") ||
		strings.Contains(lower, "tls") ||
		strings.Contains(lower, "transport")
}

// QuietMode suppresses diagnostic output (reconnection attempts, gap events,
// batch flush failures) so the CLI can be used in pipelines or daemon
// contexts where only the final exit code matters.
type QuietMode struct{}

func (q *QuietMode) OnRetry(_ int, _ time.Duration, _ error) {
	// no-op: silence reconnection attempts
}

func (q *QuietMode) OnGap(_ stream.GapEvent) {
	// no-op: silence gap recovery announcements
}

func (q *QuietMode) OnBatchError(_ error) {
	// no-op: silence batch flush failures
}

// Exit codes for reference:
//   0 = success (stream completed normally)
//   1 = generic error (validation, unexpected failure)
//   2 = gap (records lost; printed with --from guidance)
//   3 = authentication error (bad RPC credentials)
//   4 = network error (RPC unreachable, timeout)
