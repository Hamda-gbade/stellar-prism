package stream

import (
	"reflect"
	"testing"
)

func TestApplyInvocationTraceAnnotatesEmittingContract(t *testing.T) {
	event := &Event{ContractID: "CTOKEN"}
	trace := []InvocationFrame{{ContractID: "CROUTER", Function: "swap"}, {ContractID: "CPAIR", Function: "swap"}, {ContractID: "CTOKEN", Function: "transfer"}}
	ApplyInvocationTrace(event, trace)
	if event.CallDepth != 2 || event.ParentContractID != "CPAIR" || !reflect.DeepEqual(event.InvocationPath, []string{"CROUTER", "CPAIR", "CTOKEN"}) {
		t.Fatalf("annotated event = %+v", event)
	}
}
