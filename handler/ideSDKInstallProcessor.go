package handler

import (
	"context"

	"github.com/aifimmunology/hise-data-contracts/clients/tracerclient"

	"cloud.google.com/go/pubsub/v2"
)

type InstallSDKProcessor struct{ baseProcessor }

func (processor *InstallSDKProcessor) Claim(m *pubsub.Message) bool {
	return claimFlowAction(m, tracerclient.ActionInstallSDK)
}

func (processor *InstallSDKProcessor) Name() string {
	return "InstallSDKProcessor"
}

func (processor *InstallSDKProcessor) Process(ctx context.Context, m *pubsub.Message) error {
	if message, err := decodeFlowRequest(m); err != nil {
		return err
	} else if _, _, err = handleArgoFromRequest(message, tracerclient.StatusInstallSDKFailure); err != nil {
		return err
	}
	return nil
}
