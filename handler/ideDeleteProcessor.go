package handler

import (
	"context"
	"strings"

	"github.com/aifimmunology/hise-data-contracts/clients/tracerclient"
	"github.com/aifimmunology/hise-data-contracts/contracts/tracer"
	"github.com/aifimmunology/ide-workflow-management/dependencycontainer"

	"cloud.google.com/go/pubsub/v2"
)

type IDEDeleteProcessor struct{ baseProcessor }

func (ideDeleteProcessor *IDEDeleteProcessor) Claim(m *pubsub.Message) bool {
	return claimFlowAction(m, tracerclient.ActionDelete)
}

func (ideDeleteProcessor *IDEDeleteProcessor) Name() string {
	return "IDEDeleteProcessor"
}

func (ideDeleteProcessor *IDEDeleteProcessor) Process(ctx context.Context, m *pubsub.Message) error {
	message, err := decodeFlowRequest(m)
	if err != nil {
		return err
	}

	_, ide, err := handleArgoFromRequest(message, tracerclient.StatusDeletionFailure)
	if err != nil {
		if !strings.Contains(err.Error(), wfNotFoundErr) {
			m.Nack()
		}
		return err
	}

	ide.ResolvedMachineInfo = tracer.ResolvedMachineInfo{}
	setIdeStatus(ide, tracerclient.StatusDeleted)
	return dependencycontainer.TracerDataServiceClient.UpdateIdeInstance(ide)
}
