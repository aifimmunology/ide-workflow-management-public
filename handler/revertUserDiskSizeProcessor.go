package handler

import (
	"context"
	"fmt"
	"strings"

	"github.com/aifimmunology/hise-data-contracts/clients/tracerclient"
	"github.com/aifimmunology/ide-workflow-management/dependencycontainer"
	"github.com/aifimmunology/shared-utility-library/log"

	"cloud.google.com/go/pubsub/v2"
)

type RevertUserDiskSizeProcessor struct{ baseProcessor }

func (p *RevertUserDiskSizeProcessor) Claim(m *pubsub.Message) bool {
	return claimFlowAction(m, tracerclient.ActionRevertUserDiskSize)
}

func (p *RevertUserDiskSizeProcessor) Name() string {
	return "RevertUserDiskSizeProcessor"
}

func (p *RevertUserDiskSizeProcessor) Process(ctx context.Context, m *pubsub.Message) error {
	if message, err := decodeFlowRequest(m); err != nil {
		return err
	} else if wf, _, aerr := handleArgoFromRequest(message, tracerclient.StatusRevertUserDiskFailure); aerr != nil {
		if !strings.Contains(aerr.Error(), wfNotFoundErr) {
			m.Nack()
		}
		return aerr
	} else if ide, err := revertToOriginalStatus(message, wf); err != nil {
		return fmt.Errorf("could not update ide %v status: %s", message.IDEInstanceId, err.Error())
	} else if ide.Status == tracerclient.StatusRunning {
		if err = setIdeResolvedMachineInfo(ctx, ide); err != nil {
			log.Main.Error(
				fmt.Errorf("failed to set ide's resolved machine info: %s", err.Error()),
				log.D{"ideId": ide.GetId(), "ideName": ide.FriendlyName, "addedUser": ide.AuditInfo.AddedUser},
			)
		} else {
			return dependencycontainer.TracerDataServiceClient.UpdateIdeInstance(ide)
		}
	}
	return nil
}
