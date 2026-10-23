package stream

import (
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestTopicRegexFilterMatchesDecodedSymbol(t *testing.T) {
	t.Parallel()
	symbol, err := xdr.NewScVal(xdr.ScValTypeScvSymbol, xdr.ScSymbol("transfer_fee"))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := xdr.MarshalBase64(symbol)
	if err != nil {
		t.Fatal(err)
	}
	filter, err := CompileTopicRegexFilter([]string{`0:^transfer(_.*)?$`})
	if err != nil {
		t.Fatal(err)
	}
	record := Record{Kind: KindEvent, Event: &Event{TopicXDR: []string{encoded}}}
	matched, err := filter.Match(record)
	if err != nil {
		t.Fatal(err)
	}
	if !matched {
		t.Fatal("topic regex did not match transfer_fee")
	}
	expression, err := CompileExpression(`topics[0].symbol == "transfer_fee"`)
	if err != nil {
		t.Fatal(err)
	}
	matched, err = expression.Match(record)
	if err != nil {
		t.Fatal(err)
	}
	if !matched {
		t.Fatal("topic symbol expression did not match")
	}
}

func TestTopicRegexFilterValidatesRules(t *testing.T) {
	t.Parallel()
	for _, spec := range []string{"x:transfer", "-1:transfer", "0:["} {
		if _, err := CompileTopicRegexFilter([]string{spec}); err == nil {
			t.Errorf("CompileTopicRegexFilter(%q) succeeded", spec)
		}
	}
}
