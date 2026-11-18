package handler

import (
	"encoding/json"
	"fmt"

	"github.com/aifimmunology/hise-data-contracts/contracts/pubsubmessages"
	"github.com/aifimmunology/shared-utility-library/log"

	"cloud.google.com/go/pubsub/v2"
)

type baseProcessor struct{}

func (baseProcessor) Initialize() error {
	return nil
}

func (baseProcessor) IsInThrottleLimit() bool {
	return true
}

func claimMessage[T any](m *pubsub.Message, matches func(T) bool) bool {
	var message T
	if err := json.Unmarshal(m.Data, &message); err != nil {
		log.Main.Error(err)
		return false
	}
	return matches(message)
}

func claimFlowAction(m *pubsub.Message, action string) bool {
	return claimMessage(m, func(req pubsubmessages.IDEFlowRequest) bool {
		return req.Action == action
	})
}

func decodeMessage[T any](m *pubsub.Message) (*T, error) {
	var message T
	if err := json.Unmarshal(m.Data, &message); err != nil {
		log.Main.Error(fmt.Errorf("could not decode message data"), log.D{"err": err})
		m.Ack()
		return nil, err
	}
	return &message, nil
}

func decodeFlowRequest(m *pubsub.Message) (*pubsubmessages.IDEFlowRequest, error) {
	return decodeMessage[pubsubmessages.IDEFlowRequest](m)
}
