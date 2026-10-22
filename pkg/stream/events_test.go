package stream

import (
	"reflect"
	"testing"
)

func TestSplitContractIDsRespectsServerLimit(t *testing.T) {
	ids := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"}
	groups := splitContractIDs(ids)
	want := [][]string{{"a", "b", "c", "d", "e"}, {"f", "g", "h", "i", "j"}, {"k"}}
	if !reflect.DeepEqual(groups, want) {
		t.Fatalf("groups %v, want %v", groups, want)
	}
}
