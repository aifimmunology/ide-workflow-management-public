package handler

import (
	"context"

	"github.com/aifimmunology/hise-data-contracts/clients/tracerclient"
	"github.com/aifimmunology/hise-data-contracts/contracts/pubsubmessages"
	"github.com/aifimmunology/hise-data-contracts/contracts/tracer"
	"github.com/aifimmunology/ide-workflow-management/dependencycontainer"

	"cloud.google.com/go/pubsub/v2"
)

type IDEStopProcessor struct{ baseProcessor }

func (sp *IDEStopProcessor) Claim(m *pubsub.Message) bool {
	return claimFlowAction(m, tracerclient.ActionStop)
}

func (sp *IDEStopProcessor) Name() string {
	return "IDEStopProcessor"
}

func (sp *IDEStopProcessor) Process(ctx context.Context, m *pubsub.Message) error {
	if message, err := decodeFlowRequest(m); err != nil {
		return err
	} else if wf, _, err := handleArgoFromRequest(message, tracerclient.StatusStoppingFailure); err != nil {
		m.Ack()
		return err
	} else if err = sp.updateIde(message); err != nil {
		return handleETagError(err, m, wf)
	} else {
		ackMsgAndCleanUpWorkflow(m, wf)
		return nil
	}
}

func (sp *IDEStopProcessor) updateIde(message *pubsubmessages.IDEFlowRequest) error {
	ide, err := getIdeFromFlowRequest(message)
	if err != nil {
		return err
	}
	ide.ResolvedMachineInfo = tracer.ResolvedMachineInfo{}

	if snapshotSuffix, has := message.AdditionalPayload["snapshotSuffix"]; has {
		if len(ide.VolumeHistory) == 0 {
			ide.VolumeHistory = []tracer.IDEVolumeHistory{{}}
		}
		ide.VolumeHistory[len(ide.VolumeHistory)-1].VolumeSnapshotSuffix = snapshotSuffix.(string)
	}

	stoppedStatus := tracerclient.StatusStopped
	if val, has := message.AdditionalPayload["quotaReached"]; has {
		if quotaReached, is := val.(bool); is && quotaReached {
			stoppedStatus = tracerclient.StatusStoppedQuotaReached
		}
	}
	setIdeStatus(ide, stoppedStatus)

	return dependencycontainer.TracerDataServiceClient.UpdateIdeInstanceWithETag(ide)
}
