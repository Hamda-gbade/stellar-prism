package stream

import "testing"

func TestSplitLedgerRange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		from    uint32
		until   uint32
		workers int
		want    []ledgerRange
	}{
		{
			name:    "one worker",
			from:    100,
			until:   109,
			workers: 1,
			want:    []ledgerRange{{100, 109}},
		},
		{
			name:    "even split",
			from:    100,
			until:   107,
			workers: 4,
			want: []ledgerRange{
				{100, 101}, {102, 103}, {104, 105}, {106, 107},
			},
		},
		{
			name:    "uneven split",
			from:    100,
			until:   106,
			workers: 3,
			want: []ledgerRange{
				{100, 102}, {103, 105}, {106, 106},
			},
		},
		{
			name:    "more workers than ledgers",
			from:    100,
			until:   101,
			workers: 10,
			want:    []ledgerRange{{100, 100}, {101, 101}},
		},
		{
			name:    "single ledger",
			from:    100,
			until:   100,
			workers: 4,
			want:    []ledgerRange{{100, 100}},
		},
		{
			name:    "empty range",
			from:    110,
			until:   100,
			workers: 4,
			want:    nil,
		},
		{
			name:    "non-positive workers mean one",
			from:    100,
			until:   104,
			workers: 0,
			want:    []ledgerRange{{100, 104}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := splitLedgerRange(tc.from, tc.until, tc.workers)
			if len(got) != len(tc.want) {
				t.Fatalf("splitLedgerRange() = %v, want %v", got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Errorf("range %d = %v, want %v", i, got[i], tc.want[i])
				}
			}
		})
	}
}
