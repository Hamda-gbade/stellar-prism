package stream_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

type plainSource struct {
	name string
}

func (s plainSource) Name() string { return s.name }

func (s plainSource) Fetch(context.Context, string) (stream.Batch, error) {
	return stream.Batch{}, nil
}

func (s plainSource) CursorLedger(string) (uint32, bool) { return 0, false }

type rewindSource struct {
	plainSource
	ledger  uint32
	rewinds int
}

func (s *rewindSource) Rewind(ledger uint32) {
	s.ledger = ledger
	s.rewinds++
}

func TestParseGapStrategy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    stream.GapStrategy
		wantErr bool
	}{
		{name: "empty defaults to fail", input: "", want: stream.GapFail},
		{name: "fail", input: "fail", want: stream.GapFail},
		{name: "resume", input: "resume", want: stream.GapResume},
		{name: "case and whitespace", input: " RESUME ", want: stream.GapResume},
		{name: "unknown", input: "skip", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := stream.ParseGapStrategy(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseGapStrategy(%q) error = nil, want an error", tc.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseGapStrategy(%q) error = %v", tc.input, err)
			}
			if got != tc.want {
				t.Errorf("ParseGapStrategy(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestGapRecoveryFailsByDefault(t *testing.T) {
	t.Parallel()

	src := &rewindSource{plainSource: plainSource{name: "events"}}
	gap := &stream.GapError{Source: "events", From: 100, Oldest: 150}
	var events []stream.GapEvent

	recovery := stream.GapRecovery{
		OnGap: func(event stream.GapEvent) { events = append(events, event) },
	}
	recovered, err := recovery.Handle(src, gap)
	if recovered {
		t.Error("Handle() recovered a gap under the default policy")
	}
	var got *stream.GapError
	if !errors.As(err, &got) || got != gap {
		t.Fatalf("Handle() error = %v, want the original gap", err)
	}
	if src.rewinds != 0 {
		t.Errorf("source was rewound %d time(s), want 0", src.rewinds)
	}
	if len(events) != 1 || events[0].Recovered {
		t.Errorf("OnGap events = %+v, want one unrecovered notification", events)
	}
}

func TestGapRecoveryResumesAtOldestLedger(t *testing.T) {
	t.Parallel()

	src := &rewindSource{plainSource: plainSource{name: "events"}}
	gap := &stream.GapError{Source: "events", From: 100, Oldest: 150}
	var events []stream.GapEvent

	recovery := stream.GapRecovery{
		Strategy: stream.GapResume,
		OnGap:    func(event stream.GapEvent) { events = append(events, event) },
	}
	recovered, err := recovery.Handle(src, gap)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if !recovered {
		t.Fatal("Handle() did not recover the gap")
	}
	if src.ledger != 150 || src.rewinds != 1 {
		t.Errorf("source rewound to %d %d time(s), want ledger 150 once", src.ledger, src.rewinds)
	}
	if recovery.Recovered() != 1 {
		t.Errorf("Recovered() = %d, want 1", recovery.Recovered())
	}
	if len(events) != 1 || !events[0].Recovered {
		t.Errorf("OnGap events = %+v, want one recovered notification", events)
	}
}

func TestGapRecoveryRequiresRewinder(t *testing.T) {
	t.Parallel()

	src := plainSource{name: "events"}
	gap := &stream.GapError{Source: "events", From: 100, Oldest: 150}
	recovery := stream.GapRecovery{Strategy: stream.GapResume}

	recovered, err := recovery.Handle(src, gap)
	if recovered {
		t.Error("Handle() recovered without a rewinder")
	}
	if !strings.Contains(err.Error(), "Rewinder") {
		t.Errorf("Handle() error = %q, want it to explain the missing Rewinder", err)
	}
	var got *stream.GapError
	if !errors.As(err, &got) {
		t.Errorf("Handle() error = %v, want it to wrap *GapError", err)
	}
}

func TestGapRecoveryHonoursMaxRecoveries(t *testing.T) {
	t.Parallel()

	src := &rewindSource{plainSource: plainSource{name: "events"}}
	gap := &stream.GapError{Source: "events", From: 100, Oldest: 150}
	recovery := stream.GapRecovery{Strategy: stream.GapResume, MaxRecoveries: 1}

	if recovered, err := recovery.Handle(src, gap); err != nil || !recovered {
		t.Fatalf("first Handle() = (%v, %v), want (true, nil)", recovered, err)
	}
	recovered, err := recovery.Handle(src, gap)
	if recovered {
		t.Error("second Handle() recovered past the configured limit")
	}
	if !strings.Contains(err.Error(), "giving up") {
		t.Errorf("second Handle() error = %q, want a giving-up message", err)
	}
	var got *stream.GapError
	if !errors.As(err, &got) {
		t.Errorf("second Handle() error = %v, want it to wrap *GapError", err)
	}
}

func TestTailResumesAfterRetentionGap(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(1000, 30)
	srv := fakerpc.NewServer(events,
		fakerpc.WithPageLimit(5),
		fakerpc.WithScript(map[int]fakerpc.Behaviour{
			1: {OldestLedger: 1010},
		}),
	)

	src, err := stream.NewEventSource(srv, stream.EventQuery{StartLedger: 1000, PageLimit: 5})
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}

	sink := &collector{}
	var gaps []stream.GapEvent
	tl := stream.NewTailer(src, sink, stream.Options{
		Clock:       newFakeClock(),
		GapStrategy: stream.GapResume,
		OnGap:       func(event stream.GapEvent) { gaps = append(gaps, event) },
	})
	if err := tl.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	want := append(idsOf(events[:5]), idsOf(events[10:])...)
	assertStreamInvariants(t, sink.ids, want)

	stats := tl.Stats()
	if stats.Gaps != 1 || stats.Recovered != 1 {
		t.Errorf("Stats() = %+v, want one gap and one recovery", stats)
	}
	if len(gaps) != 1 || !gaps[0].Recovered || gaps[0].Gap.Oldest != 1010 {
		t.Errorf("OnGap events = %+v, want one recovery at ledger 1010", gaps)
	}
}
