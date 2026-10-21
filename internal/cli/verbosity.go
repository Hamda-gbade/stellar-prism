package cli

import (
	"strings"
)

// verbosityLevel controls the granularity of diagnostic output.
// Higher values produce more verbose logging.
type verbosityLevel int

const (
	// Silent suppresses all non-fatal diagnostic output.
	Silent verbosityLevel = iota
	// ErrorLevel logs only errors and above.
	ErrorLevel
	// WarningLevel logs warnings, errors, and above.
	WarningLevel
	// InfoLevel logs info, warnings, errors, and above (default).
	InfoLevel
	// DebugLevel logs debug, info, warnings, errors, and above.
	DebugLevel
	// TraceLevel logs everything including trace-level detail.
	TraceLevel
)

// String returns the human-readable form of the verbosity level.
func (v verbosityLevel) String() string {
	switch v {
	case Silent:
		return "silent"
	case ErrorLevel:
		return "error"
	case WarningLevel:
		return "warning"
	case InfoLevel:
		return "info"
	case DebugLevel:
		return "debug"
	case TraceLevel:
		return "trace"
	default:
		return "unknown"
	}
}

// LevelFromString parses a verbosity level from a string like "silent", "error",
// "warning", "info", "debug", or "trace". Unknown values default to InfoLevel.
func LevelFromString(s string) verbosityLevel {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "silent":
		return Silent
	case "error":
		return ErrorLevel
	case "warning":
		return WarningLevel
	case "debug":
		return DebugLevel
	case "trace":
		return TraceLevel
	default:
		return InfoLevel
	}
}
