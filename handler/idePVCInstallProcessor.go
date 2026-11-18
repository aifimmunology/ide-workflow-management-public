package handler

import (
	"context"

	"github.com/aifimmunology/hise-data-contracts/clients/tracerclient"
	"github.com/aifimmunology/ide-workflow-management/dependencycontainer"

	"cloud.google.com/go/pubsub/v2"
)

type IDEPVCInstallProcessor struct{ baseProcessor }

func (idePVCInstallProcessor *IDEPVCInstallProcessor) Claim(m *pubsub.Message) bool {
	return claimFlowAction(m, tracerclient.ActionProvision)
}

func (idePVCInstallProcessor *IDEPVCInstallProcessor) Name() string {
	return "IDEPVCInstallProcessor"
}

func (idePVCInstallProcessor *IDEPVCInstallProcessor) Process(ctx context.Context, m *pubsub.Message) error {
	if message, err := decodeFlowRequest(m); err != nil {
		return err
	} else if _, ide, err := handleArgoFromRequest(message, tracerclient.StatusPackProvisioningFailure); err != nil {
		return err
	} else {
		setIdeStatus(ide, tracerclient.StatusPackProvisioning)
		return dependencycontainer.TracerDataServiceClient.UpdateIdeInstance(ide)
	}
}
