package cli

import (
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
)

var sigQuitOnce sync.Once

// handleSigQUIT installs the SIGQUIT handler so that when the process receives
// it, it prints goroutine stacks to stderr instead of aborting. SIGUSR1 is not
// defined by the Windows syscall package, so SIGQUIT is the portable hook.
func handleSigQUIT() {
	sigQuitOnce.Do(func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGQUIT)

		go func() {
			for range sigChan {
				fmt.Fprintln(os.Stderr, "SIGQUIT received:")
				fmt.Fprintln(os.Stderr, getAllGoroutineStacks())
			}
		}()
	})
}

// getAllGoroutineStacks returns a string containing stack traces of all goroutines.
func getAllGoroutineStacks() string {
	var buf strings.Builder
	buf.WriteString("Goroutine stack traces (best-effort):\n")
	buf.WriteRune('\n')
	buf.Write(debug.Stack())
	return buf.String()
}

// InitSIGQUIT installs the SIGQUIT/SIGUSR1 handler so that when the process
// receives either signal it prints goroutine stacks to stderr instead of aborting.
func InitSIGQUIT() {
	handleSigQUIT()
}
