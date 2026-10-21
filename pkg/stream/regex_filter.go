package stream

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/stellar-optics/stellar-xdr-lens/pkg/lens"
)

// TopicRegexRule matches one decoded topic segment.
type TopicRegexRule struct {
	Index    int
	Pattern  string
	compiled *regexp.Regexp
}

// TopicRegexFilter applies all configured topic rules to an event. Rules use
// the syntax "segment-index:regular-expression" and are ANDed together.
type TopicRegexFilter struct {
	rules   []TopicRegexRule
	cacheMu sync.RWMutex
	cache   map[string]bool
}

// CompileTopicRegexFilter parses and compiles topic matching rules.
func CompileTopicRegexFilter(specs []string) (*TopicRegexFilter, error) {
	f := &TopicRegexFilter{cache: make(map[string]bool)}
	for _, spec := range specs {
		for _, ruleSpec := range strings.Split(spec, ",") {
			indexText, pattern, ok := strings.Cut(ruleSpec, ":")
			if !ok || pattern == "" {
				return nil, fmt.Errorf("topic regex %q must have the form index:pattern", ruleSpec)
			}
			index, err := strconv.Atoi(indexText)
			if err != nil || index < 0 {
				return nil, fmt.Errorf("topic regex %q has an invalid non-negative segment index", ruleSpec)
			}
			compiled, err := regexp.Compile(pattern)
			if err != nil {
				return nil, fmt.Errorf("invalid topic regex %q: %w", pattern, err)
			}
			f.rules = append(f.rules, TopicRegexRule{Index: index, Pattern: pattern, compiled: compiled})
		}
	}
	return f, nil
}

// Match implements RecordFilter.
func (f *TopicRegexFilter) Match(record Record) (bool, error) {
	if record.Kind != KindEvent || record.Event == nil {
		return false, nil
	}
	for _, rule := range f.rules {
		if rule.Index >= len(record.Event.TopicXDR) {
			return false, nil
		}
		encoded := record.Event.TopicXDR[rule.Index]
		key := strconv.Itoa(rule.Index) + ":" + rule.Pattern + ":" + encoded
		f.cacheMu.RLock()
		cached, ok := f.cache[key]
		f.cacheMu.RUnlock()
		if ok {
			if !cached {
				return false, nil
			}
			continue
		}
		decoded, err := lens.Decode(encoded)
		if err != nil {
			return false, fmt.Errorf("decoding topic[%d] for regex filter: %w", rule.Index, err)
		}
		matched := rule.compiled.MatchString(topicText(decoded.Node))
		f.cacheMu.Lock()
		if len(f.cache) >= 4096 {
			clear(f.cache)
		}
		f.cache[key] = matched
		f.cacheMu.Unlock()
		if !matched {
			return false, nil
		}
	}
	return true, nil
}

func topicText(node *lens.Node) string {
	if node == nil {
		return ""
	}
	if node.Kind == lens.KindScalar {
		if node.Value == nil {
			return ""
		}
		return fmt.Sprint(node.Value)
	}
	for _, child := range node.Children {
		if child.Name == "Sym" || child.Name == "Str" {
			if value := firstFilterScalar(child); value != nil {
				return fmt.Sprint(value)
			}
		}
	}
	// Topic ScVals are tagged unions. The first scalar nested under the active
	// arm is used for types that do not have a dedicated symbol/string field.
	for _, child := range node.Children {
		if value := topicText(child); value != "" {
			return value
		}
	}
	return ""
}
