package handler

import (
	"context"

	"github.com/aifimmunology/hise-data-contracts/clients/tracerclient"
	"github.com/aifimmunology/hise-data-contracts/contracts/pubsubmessages"
	"github.com/aifimmunology/ide-workflow-management/dependencycontainer"
	"github.com/aifimmunology/shared-utility-library/log"
	"github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"

	"cloud.google.com/go/pubsub/v2"
)

type JupyterTimeoutProcessor struct{ baseProcessor }

func (jp *JupyterTimeoutProcessor) Claim(m *pubsub.Message) bool {
	return claimFlowAction(m, tracerclient.ActionChangeJupyterTimeout)
}

func (jp *JupyterTimeoutProcessor) Name() string {
	return "JupyterTimeoutProcessor"
}

func (jp *JupyterTimeoutProcessor) Process(ctx context.Context, m *pubsub.Message) error {
	if message, err := decodeFlowRequest(m); err != nil {
		m.Ack()
		return err
	} else if wf, _, err := handleArgoFromRequest(message, tracerclient.StatusJupyterTimeoutFailure); err != nil {
		m.Ack()
		return err
	} else if err := jp.updateIde(ctx, message, wf); err != nil {
		return handleETagError(err, m, wf)
	} else {
		ackMsgAndCleanUpWorkflow(m, wf)
		return nil
	}
}

func (jp *JupyterTimeoutProcessor) updateIde(ctx context.Context, message *pubsubmessages.IDEFlowRequest, wf *v1alpha1.Workflow) error {
	if ide, err := revertToOriginalStatus(message, wf); err != nil {
		return err
	} else if ide.Status == tracerclient.StatusRunning {
		if err = setIdeResolvedMachineInfo(ctx, ide); err != nil {
			log.Main.Error(err)
		} else {
			return dependencycontainer.TracerDataServiceClient.UpdateIdeInstanceWithETag(ide)
		}
	}
	return nil
}
