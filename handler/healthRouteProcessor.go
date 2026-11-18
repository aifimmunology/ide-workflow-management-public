package handler

import (
	"context"
	"fmt"

	"github.com/aifimmunology/hise-data-contracts/clients/tracerclient"
	"github.com/aifimmunology/hise-data-contracts/contracts/tracer"
	"github.com/aifimmunology/ide-workflow-management/dependencycontainer"
	"github.com/aifimmunology/shared-utility-library/log"
	"github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"

	"cloud.google.com/go/pubsub/v2"
)

type HealthRouteProcessor struct{ baseProcessor }

func (p *HealthRouteProcessor) Claim(m *pubsub.Message) bool {
	return claimFlowAction(m, tracerclient.StatusProvisioningHttpRoute)
}

func (p *HealthRouteProcessor) Name() string {
	return "HealthRouteProcessor"
}

func (p *HealthRouteProcessor) Process(ctx context.Context, m *pubsub.Message) error {
	if message, err := decodeFlowRequest(m); err != nil {
		m.Ack()
		return err
	} else if err := updateActionHistoryWithEtag(message); err != nil {
		return handleETagError(err, m, nil)
	} else if wf, ide, err := handleArgoFromRequest(message, tracerclient.StatusCreationFailure); err != nil {
		return handleETagError(err, m, wf)
	} else if iapArgo, err := SubmitSetIamPolicyArgo(ctx, ide, message, tracerclient.StatusCreationFailure); err != nil {
		return handleETagError(err, m, iapArgo)
	} else if err := sendMessageToIapPolicyProcessor(ctx, ide, iapArgo, message, tracerclient.ActionCreate); err != nil {
		ackMsgAndCleanUpWorkflow(m, wf)
		cleanupWorkflow(iapArgo)
		return err
	} else {
		ackMsgAndCleanUpWorkflow(m, wf)
		return nil
	}
}

func handleHealthRouteArgoWorkflow(ide *tracer.IDEInstance, gatewayWf *v1alpha1.Workflow) error {
	if _, merr := findAndMonitorArgoWorkflowAndUpdateIde(gatewayWf, ide); merr != nil {
		log.Main.Error(fmt.Sprintf("error during find and monitor argo workflow %s: %s", gatewayWf.Name, merr.Error()))
		recentIde, err := dependencycontainer.TracerDataServiceClient.GetIdeInstance(ide.ID)
		if err != nil {
			log.Main.Error(err)
			return merr
		}

		setIdeStatus(recentIde, "provisioning-http-route-failure", merr.Error())
		if err := dependencycontainer.TracerDataServiceClient.UpdateIdeInstanceWithETag(recentIde); err != nil {
			log.Main.Error(err)
		}
		return merr
	}
	return nil
}
