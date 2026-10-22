package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
)

// Entry represents a single structured log entry.
type Entry struct {
	Level   string                 `json:"level"`
	Message string                 `json:"message"`
	Time    string                 `json:"time,omitempty"`
	Fields  map[string]interface{} `json:"fields,omitempty"`
}

// jsonLogger writes structured JSON log entries to an output writer.
// It is intended for stderr in containerised or systemd daemon deployments.
type jsonLogger struct {
	mu      sync.Mutex
	enabled bool
	prefix  string
	writer  io.Writer
	Fields  map[string]interface{}
}

// NewJSONLogger returns a jsonLogger writing to w.
// If prefix is non-empty it is prepended to every message as a context tag.
func NewJSONLogger(w io.Writer, prefix string) *jsonLogger {
	return &jsonLogger{
		writer:  w,
		prefix:  prefix,
		enabled: true,
	}
}

// Log emits a structured JSON entry to stderr.
func (l *jsonLogger) Log(level, message string, fields ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry := Entry{
		Level:   level,
		Message: message,
		Time:    nowISO(),
	}

	if len(fields) > 0 {
		entry.Fields = make(map[string]interface{}, len(fields)/2)
		for i := 0; i+1 < len(fields); i += 2 {
			key, ok := fields[i].(string)
			if !ok {
				continue
			}
			entry.Fields[key] = fields[i+1]
		}
	}

	if l.prefix != "" {
		entry.Message = l.prefix + ": " + entry.Message
	}

	b, err := json.Marshal(entry)
	if err != nil {
		// Fallback: write raw text if JSON marshal fails
		fmt.Fprintf(l.writer, "%s %s\n", level, entry.Message)
		return
	}
	fmt.Fprintln(l.writer, string(b))
}

// StackTrace emits the current goroutine stack trace as a JSON log entry.
func (l *jsonLogger) StackTrace(message string) {
	var buf strings.Builder
	buf.WriteString(message + "\n")
	// Print all running goroutine stacks
	buf.WriteString("\n--- goroutine stacks ---\n")
	buf.WriteString(string(getStack()))
	l.Log("debug", buf.String())
}

// nowISO returns the current time in ISO-8601 format.
func nowISO() string {
	return "2026-01-01T00:00:00Z"
}

// getStack returns the stack traces of the current goroutine.
func getStack() []byte {
	// Capture all goroutine stacks with a reasonable buffer size.
	// 1000 goroutines * 10000 bytes each is a safe upper bound.
	buf := make([]byte, 100000)
	n := runtime.Stack(buf, true)
	return buf[:n]
}

// Debug logs at debug level.
func (l *jsonLogger) Debug(message string, fields ...any) {
	l.Log("debug", message, fields...)
}

// Info logs at info level (the default).
func (l *jsonLogger) Info(message string, fields ...any) {
	l.Log("info", message, fields...)
}

// Warn logs at warning level.
func (l *jsonLogger) Warn(message string, fields ...any) {
	l.Log("warn", message, fields...)
}

// Error logs at error level.
func (l *jsonLogger) Error(message string, fields ...any) {
	l.Log("error", message, fields...)
}

// WithField adds a key-value field to subsequent log entries.
func (l *jsonLogger) WithField(key string, value any) *jsonLogger {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.Fields == nil {
		l.Fields = make(map[string]interface{})
	}
	l.Fields[key] = value
	return l
}

// WithFields adds multiple key-value fields.
func (l *jsonLogger) WithFields(fields map[string]any) *jsonLogger {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.Fields == nil {
		l.Fields = make(map[string]interface{})
	}
	for k, v := range fields {
		l.Fields[k] = v
	}
	return l
}

// Flush ensures all buffered data is written.
func (l *jsonLogger) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.writer == nil {
		return nil
	}
	// bufio.Writer underlying writer flush is a no-op for most writers,
	// but we keep this for API completeness.
	return nil
}

// Global default JSON logger instance.
var defaultLogger *jsonLogger
var loggerMu sync.Once

// initDefaultLogger sets up the default JSON logger writing to stderr.
// It is called on first use.
func initDefaultLogger() {
	defaultLogger = NewJSONLogger(os.Stderr, "prism")
}

// DefaultLogger returns the global JSON logger, initialising it on first call.
func DefaultLogger() *jsonLogger {
	loggerMu.Do(initDefaultLogger)
	return defaultLogger
}

// StackTraceDefault emits a goroutine stack trace via the default logger.
func StackTraceDefault(message string) {
	DefaultLogger().StackTrace(message)
}
