package handler

import (
	"testing"

	"github.com/aifimmunology/hise-data-contracts/contracts/pubsubmessages"

	"cloud.google.com/go/pubsub/v2"
)

func TestClaimFlowAction(t *testing.T) {
	tests := []struct {
		name string
		data string
		want bool
	}{
		{"matching action", `{"action":"stop"}`, true},
		{"different action", `{"action":"start"}`, false},
		{"malformed message", `{not json`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := claimFlowAction(&pubsub.Message{Data: []byte(tt.data)}, "stop"); got != tt.want {
				t.Errorf("claimFlowAction() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDecodeMessage(t *testing.T) {
	got, err := decodeMessage[pubsubmessages.IDEFlowRequest](&pubsub.Message{Data: []byte(`{"action":"stop"}`)})
	if err != nil || got == nil || got.Action != "stop" {
		t.Fatalf("decodeMessage() = %+v, %v; want action \"stop\"", got, err)
	}

	got, err = decodeMessage[pubsubmessages.IDEFlowRequest](&pubsub.Message{Data: []byte(`{not json`)})
	if err == nil || got != nil {
		t.Fatalf("decodeMessage() = %+v, %v; want nil and an error for a malformed message", got, err)
	}
}
