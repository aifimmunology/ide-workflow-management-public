package handler

import (
	"context"
	"fmt"

	"github.com/aifimmunology/hise-data-contracts/clients/tracerclient"
	"github.com/aifimmunology/hise-data-contracts/contracts"
	"github.com/aifimmunology/hise-data-contracts/contracts/pubsubmessages"
	"github.com/aifimmunology/ide-workflow-management/dependencycontainer"
	"github.com/aifimmunology/shared-utility-library/log"
	"github.com/aifimmunology/shared-utility-library/utils"
	"github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"

	"cloud.google.com/go/pubsub/v2"
)

type EditIdeMachineConfigProcessor struct{ baseProcessor }

func (p *EditIdeMachineConfigProcessor) Claim(m *pubsub.Message) bool {
	return claimFlowAction(m, tracerclient.ActionEditMachineConfig)
}

func (p *EditIdeMachineConfigProcessor) Name() string {
	return "EditIdeMachineConfigProcessor"
}

func (p *EditIdeMachineConfigProcessor) Process(ctx context.Context, m *pubsub.Message) error {
	if message, err := decodeFlowRequest(m); err != nil {
		m.Ack()
		return err
	} else if wf, _, aerr := handleArgoFromRequest(message, "edit-machine-config-failure"); aerr != nil {
		m.Ack()
		return aerr
	} else if err = p.updateIde(ctx, message, wf); err != nil {
		return handleETagError(err, m, wf)
	} else {
		m.Ack()
		return cleanupWorkflow(wf)
	}
}

func (p *EditIdeMachineConfigProcessor) updateIde(ctx context.Context, message *pubsubmessages.IDEFlowRequest, wf *v1alpha1.Workflow) error {
	ide, err := revertToOriginalStatus(message, wf)
	if err != nil {
		return err
	}

	var configStruct contracts.MachineConfig
	if config, exists := message.AdditionalPayload["machineConfig"]; !exists {
		return fmt.Errorf(
			"machineConfig not found in additional payload, cannot"+" update ide %s's metadata",
			ide.ID.String(),
		)
	} else if err = utils.AnyToTarget(config, &configStruct); err != nil {
		return fmt.Errorf(
			"failed to convert machineConfig to MachineConfig "+" struct for ide %s: %s",
			ide.ID.String(),
			err.Error(),
		)
	}

	ide.MachineConfig.Name = configStruct.Name
	ide.MachineConfig.Type = configStruct.Type
	ide.MachineConfig.CPU.RequestSize = configStruct.CPU.RequestSize
	ide.MachineConfig.CPU.LimitSize = configStruct.CPU.RequestSize
	ide.MachineConfig.GPUSelection = configStruct.GPUSelection

	if ide.Status == tracerclient.StatusRunning { // pod needs to be up to run the func below
		if err = setIdeResolvedMachineInfo(ctx, ide); err != nil {
			log.Main.Error(
				fmt.Errorf("failed to set ide's resolved machine info: %s", err.Error()),
				log.D{"ideId": ide.GetId(), "ideName": ide.FriendlyName, "addedUser": ide.AuditInfo.AddedUser},
			)
		}
	}

	return dependencycontainer.TracerDataServiceClient.UpdateIdeInstanceWithETag(ide)
}
