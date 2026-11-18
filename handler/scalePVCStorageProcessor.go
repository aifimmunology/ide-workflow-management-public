package handler

import (
	"context"

	"github.com/aifimmunology/hise-data-contracts/clients/tracerclient"
	"github.com/aifimmunology/hise-data-contracts/contracts/pubsubmessages"
	"github.com/aifimmunology/ide-workflow-management/dependencycontainer"
	"github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"

	"cloud.google.com/go/pubsub/v2"
)

type ScalePVCStorageProcessor struct{ baseProcessor }

func (sp *ScalePVCStorageProcessor) Claim(m *pubsub.Message) bool {
	return claimFlowAction(m, tracerclient.ActionScalePVCStorage)
}

func (sp *ScalePVCStorageProcessor) Name() string {
	return "ScalePVCStorageProcessor"
}

func (sp *ScalePVCStorageProcessor) Process(ctx context.Context, m *pubsub.Message) error {
	if msg, err := decodeFlowRequest(m); err != nil {
		m.Ack()
		return err
	} else if wf, _, aerr := handleArgoFromRequest(msg, tracerclient.StatusScalingPVCStorageFailure); aerr != nil {
		m.Ack()
		return aerr
	} else if err = sp.updateIde(ctx, msg, wf); err != nil {
		return handleETagError(err, m, wf)
	} else {
		m.Ack()
		return cleanupWorkflow(wf)
	}
}

func (sp *ScalePVCStorageProcessor) updateIde(ctx context.Context, msg *pubsubmessages.IDEFlowRequest, wf *v1alpha1.Workflow) error {
	if ide, perr := setPreviousIdeStatusFromFlowRequest(msg); perr != nil {
		return perr
	} else if err := setIdeResolvedMachineInfo(ctx, ide); err != nil {
		return err
	} else {
		return dependencycontainer.TracerDataServiceClient.UpdateIdeInstanceWithETag(ide)
	}
}
