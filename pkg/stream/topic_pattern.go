package stream

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/stellar-optics/stellar-xdr-lens/pkg/lens"
	lensformat "github.com/stellar-optics/stellar-xdr-lens/pkg/lens/format"
)

// The server-side --topic filters (--topic in events.go) accept base64 ScVals
// with "*" and "**" wildcard segments, because the RPC only understands those.
// TopicPattern adds the other half of the story: matching topics by their
// *decoded* text, with glob, prefix and regular-expression patterns, applied
// client-side after a record makes it out of the stream.
//
// A pattern is a comma-separated list of segment patterns, one per topic
// position, mirroring the server's segment grammar:
//
//	*            matches exactly one topic
//	**           matches zero or more topics; must be the last segment
//	re:EXPR      the topic text matches the regular expression
//	prefix:TEXT  the topic text starts with TEXT
//	TEXT         matches TEXT exactly; "*", "?" and "[...]" glob characters
//	             make it a glob instead, with "?" matching any one character
//
// Each pattern anchored at its segment boundaries: a compiled segment matches
// the whole topic text, not a substring of it. Regular expressions are the
// exception — they are unanchored, so `re:judge` also matches `prejudge`.
//
// The text a segment matches against is derived from the same lens-decoded
// envelope `--json` prints, so a pattern tuned against `--json` output works
// against local filtering too. See topicCandidateTexts for the exact rules.

// TopicPattern is a compiled, client-side filter for the topics of one event.
// Build one with CompileTopicPattern, then query it with Matches. The zero
// value is not usable; a TopicPattern always comes from a compile function.
type TopicPattern struct {
	// raw is the pattern text the TopicPattern was compiled from.
	raw string
	// segs are the compiled segment patterns, one per topic position.
	segs []topicSeg
}

// topicSegKind distinguishes the wildcard behaviour of one compiled segment.
type topicSegKind int

const (
	// segAnyOne matches exactly one topic position.
	segAnyOne topicSegKind = iota
	// segAnySeq matches zero or more topics and must be the last segment.
	segAnySeq
	// segMatch applies its compiled regular expression to the topic text.
	segMatch
)

// topicSeg is one compiled segment of a TopicPattern.
type topicSeg struct {
	kind topicSegKind
	// re is nil for segAnyOne and segAnySeq; for segMatch it is the compiled
	// matcher, anchored for literal, glob and prefix segments.
	re *regexp.Regexp
}

// CompileTopicPattern compiles one pattern specification or returns a
// descriptive error, so an invalid pattern surfaces once at construction
// time rather than silently filtering nothing on every record.
func CompileTopicPattern(spec string) (TopicPattern, error) {
	parts := strings.Split(spec, ",")
	if spec == "" || (len(parts) == 1 && parts[0] == "") {
		return TopicPattern{}, fmt.Errorf("empty topic pattern")
	}
	if len(parts) > MaxTopicSegments+1 { // +1 allows a trailing "**"
		return TopicPattern{}, fmt.Errorf(
			"topic pattern %q has %d segments; at most %d are allowed (plus a trailing \"**\")",
			spec, len(parts), MaxTopicSegments)
	}

	segs := make([]topicSeg, 0, len(parts))
	for i, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return TopicPattern{}, fmt.Errorf("topic pattern %q has an empty segment", spec)
		}
		if part == "**" && i != len(parts)-1 {
			return TopicPattern{}, fmt.Errorf("in topic pattern %q, %q must be the last segment", spec, part)
		}

		seg, err := compileTopicSegment(part, spec)
		if err != nil {
			return TopicPattern{}, err
		}
		segs = append(segs, seg)
	}
	return TopicPattern{raw: spec, segs: segs}, nil
}

// CompileTopicPatterns compiles a list of pattern specifications. An empty
// list compiles successfully to an empty guard: callers that combine several
// patterns with AND semantics keep one code path for both cases.
func CompileTopicPatterns(specs []string) ([]TopicPattern, error) {
	if len(specs) > MaxTopicFilters {
		return nil, fmt.Errorf("at most %d topic patterns may be given, got %d", MaxTopicFilters, len(specs))
	}
	out := make([]TopicPattern, 0, len(specs))
	for _, spec := range specs {
		p, err := CompileTopicPattern(spec)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, nil
}

// String returns the pattern text the TopicPattern was compiled from.
func (p TopicPattern) String() string { return p.raw }

// Matches reports whether an event's topics match the pattern.
//
// topics holds the base64 XDR of each topic, in order; exactly the
// Record.Event.TopicXDR shape, so a caller can pass a decoded topic list that
// came from lens without further conversion. Semantics mirror the server:
// every non-wildcard segment must match one topic, and the event must have
// exactly that many topics — unless the pattern ends in "**", which covers
// any remainder.
func (p TopicPattern) Matches(topics []string) bool {
	for i, seg := range p.segs {
		switch seg.kind {
		case segAnySeq:
			// Last segment only, guaranteed at compile time.
			return true
		case segAnyOne:
			if i >= len(topics) {
				return false
			}
		default: // segMatch
			if i >= len(topics) || !segMatches(seg.re, topicCandidateTexts(topics[i])) {
				return false
			}
		}
	}
	// Every segment consumed exactly one topic, and the event has none left
	// over: an n-segment pattern matches exactly n-topic events, mirroring
	// the server's filter semantics. The zero-or-more "**" shape returned
	// above is the one exception.
	return len(p.segs) == len(topics)
}

// segMatches reports whether any candidate text of one topic matches re.
func segMatches(re *regexp.Regexp, candidates []string) bool {
	for _, text := range candidates {
		if re.MatchString(text) {
			return true
		}
	}
	return false
}

// compileTopicSegment compiles one segment pattern against its full pattern
// text for error context, or returns a zero segment and a descriptive error.
func compileTopicSegment(seg, spec string) (topicSeg, error) {
	switch {
	case seg == "*":
		return topicSeg{kind: segAnyOne}, nil
	case seg == "**":
		return topicSeg{kind: segAnySeq}, nil
	case strings.HasPrefix(seg, "re:"):
		body := seg[len("re:"):]
		if body == "" {
			return topicSeg{}, fmt.Errorf("topic segment %q has an empty regular expression", spec)
		}
		re, err := regexp.Compile(body)
		if err != nil {
			return topicSeg{}, fmt.Errorf("topic segment %q is not a valid regular expression: %w", seg, err)
		}
		return topicSeg{kind: segMatch, re: re}, nil
	case strings.HasPrefix(seg, "prefix:"):
		body := seg[len("prefix:"):]
		if body == "" {
			return topicSeg{}, fmt.Errorf("topic segment %q has an empty prefix", spec)
		}
		// A prefix is a glob with an open-ended tail.
		return topicSeg{kind: segMatch, re: compileAnchored(globToRegexpPattern(body) + ".*")}, nil
	default:
		// Plain text, or text carrying glob characters; both go through the
		// glob translation, which for wildcard-free input is an exact match.
		return topicSeg{kind: segMatch, re: compileAnchored(globToRegexpPattern(seg))}, nil
	}
}

// compileAnchored compiles an internally-constructed pattern body into an
// anchored, dot-all regular expression. The body comes only from the glob
// translation above, never from user input passed unvalidated.
func compileAnchored(body string) *regexp.Regexp {
	// The non-capturing group keeps interior alternation from swallowing the
	// anchors.
	return regexp.MustCompile("(?s)\\A(?:" + body + ")\\z")
}

// globToRegexpPattern converts a glob pattern with "*" (zero or more
// characters) and "?" (exactly one) into a regular-expression body. Anything
// else is quoted. Literal and prefix segments are just globs with no wildcards
// or with none at the end, so one translation feeds all three segment kinds.
func globToRegexpPattern(pat string) string {
	var b strings.Builder
	for _, r := range pat {
		switch r {
		case '*':
			b.WriteString(".*")
		case '?':
			b.WriteString(".")
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	return b.String()
}

// topicJSONFmt renders one decoded topic into the same compact {type, value}
// envelope `--json` emits, so the text patterns run against is the text users
// already see. Constructed once; the formatter is immutable in use.
var topicJSONFmt = lensformat.NewJSONFormatter(lensformat.WithIndent(""))

// topicCandidateTexts returns every text one base64 topic segment may be
// matched against: the string leaves of the decoded envelope (skipping the
// "type"/"Type" keys, which carry XDR enum names rather than topic data), the
// numeric leaves as literals, the full envelope JSON, and the raw base64.
//
// Two properties matter more than cleverness here:
//
//   - A symbol topic "transfer" is matched by the plain pattern "transfer",
//     because the leaf string is the symbol itself.
//   - Anything the envelope walker does not understand still has the whole
//     envelope and the raw base64 as candidates, so regex over the wire shape
//     always works; no decoded form is silently unmatchable.
func topicCandidateTexts(topic string) []string {
	value, err := lens.Decode(topic)
	if err != nil {
		// Undecodable is not unmatchable: fall back to the raw bytes so a
		// pattern over base64 still applies.
		return []string{topic}
	}
	var buf bytes.Buffer
	if err := topicJSONFmt.Format(&buf, value); err != nil {
		return []string{topic}
	}

	candidates := envelopeStringLeaves(buf.Bytes())
	candidates = append(candidates, buf.String(), topic)
	return candidates
}

// envelopeStringLeaves walks one lens-format JSON envelope and collects the
// string and number leaves under "value", skipping the enum-name keys. Parse
// failure yields no candidates: the caller still has the envelope and raw
// base64 as fallbacks, so a topic is never left with no text at all.
func envelopeStringLeaves(data []byte) []string {
	var top map[string]any
	dec := json.NewDecoder(bytes.NewReader(data))
	// Numbers must survive as digit-exact literals: uint64 ledger-high i128
	// halves lose precision as float64 and would make numeric matching lie.
	dec.UseNumber()
	if err := dec.Decode(&top); err != nil {
		return nil
	}

	var out []string
	var walk func(v any)
	walk = func(v any) {
		switch t := v.(type) {
		case map[string]any:
			for k, sub := range t {
				if k == "type" || k == "Type" {
					continue // XDR enum names are noise for topic matching
				}
				walk(sub)
			}
		case string:
			out = append(out, t)
		case json.Number:
			out = append(out, t.String())
		case bool:
			out = append(out, strconv.FormatBool(t))
		}
	}
	walk(top)
	return out
}

// PatternSink wraps a Sink and emits only the records every compiled pattern
// agrees on. Records that are not events pass through untouched, so ledger
// and transaction streams behind the same sink are unaffected by event topic
// rules.
//
// Filtering is deliberately at the sink rather than inside the tailer: the
// delivery guarantee in tail.go is about the *stream* (no lost, skipped or
// repeated records), while output shaping belongs to the consumer seam. A
// record suppressed here was streamed and then filtered, in the same way a
// --limit stops the flow after the fact.
type PatternSink struct {
	inner Sink
	// compiled patterns, all of which must match an event for it to pass.
	patterns []TopicPattern
}

// NewPatternSink returns a Sink emitting, for each event record, only the
// records matched by every pattern. Empty compiled input passes everything,
// which is what a caller with no --topic-pattern flags wants.
func NewPatternSink(inner Sink, patterns []TopicPattern) *PatternSink {
	return &PatternSink{inner: inner, patterns: patterns}
}

// Emit implements Sink.
func (s *PatternSink) Emit(r Record) error {
	if r.Kind != KindEvent || r.Event == nil {
		return s.inner.Emit(r)
	}
	for _, p := range s.patterns {
		if !p.Matches(r.Event.TopicXDR) {
			return nil
		}
	}
	return s.inner.Emit(r)
}

// Flush implements Sink.
func (s *PatternSink) Flush() error { return s.inner.Flush() }
