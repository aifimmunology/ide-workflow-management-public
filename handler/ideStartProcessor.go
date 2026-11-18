package handler

import (
	"context"
	"fmt"

	"github.com/aifimmunology/hise-data-contracts/clients/tracerclient"
	"github.com/aifimmunology/hise-data-contracts/contracts"
	"github.com/aifimmunology/hise-data-contracts/contracts/pubsubmessages"
	"github.com/aifimmunology/hise-data-contracts/contracts/tracer"
	"github.com/aifimmunology/shared-utility-library/log"
	supubsub "github.com/aifimmunology/shared-utility-library/pubsub"

	"cloud.google.com/go/pubsub/v2"
	"github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
)

type IDEStartProcessor struct{ baseProcessor }

func (sp *IDEStartProcessor) Claim(m *pubsub.Message) bool {
	return claimFlowAction(m, tracerclient.ActionStart)
}

func (sp *IDEStartProcessor) Name() string {
	return "IDEStartProcessor"
}

func (sp *IDEStartProcessor) Process(ctx context.Context, m *pubsub.Message) error {
	message, err := decodeFlowRequest(m)
	if err != nil {
		log.Main.Error(fmt.Sprintf("error getting message payload: %v", err))
		return err
	} else if wf, ide, err := handleArgoFromRequest(message, tracerclient.StatusStartingFailure); err != nil {
		return handleETagError(err, m, wf)
	} else if iapArgo, err := SubmitSetIamPolicyArgo(ctx, ide, message, tracerclient.StatusStartingFailure); err != nil {
		return handleETagError(err, m, iapArgo)
	} else if err := sendMessageToIapPolicyProcessor(ctx, ide, iapArgo, message, tracerclient.ActionStart); err != nil {
		ackMsgAndCleanUpWorkflow(m, wf)
		cleanupWorkflow(iapArgo)
		return err
	} else {
		ackMsgAndCleanUpWorkflow(m, wf)
		return nil
	}
}

func sendMessageToIapPolicyProcessor(
	ctx context.Context,
	ide *tracer.IDEInstance,
	iapWf *v1alpha1.Workflow,
	message *pubsubmessages.IDEFlowRequest,
	prevAction string) error {
	additionalPayload := map[string]any{
		previousAction: prevAction,
	}

	if suffix, has := message.AdditionalPayload[pvcSuffix]; has {
		additionalPayload[pvcSuffix] = suffix.(string)
	}

	return supubsub.PublishMessage(
		ctx,
		contracts.PubsubIdeFlowRequestTopic,
		&pubsubmessages.IDEFlowRequest{
			IDEInstanceId:     ide.ID,
			IDEName:           ide.PodName,
			Action:            tracerclient.ActionSetIapPolicy,
			Workflow:          iapWf,
			AdditionalPayload: additionalPayload,
		},
	)
}
