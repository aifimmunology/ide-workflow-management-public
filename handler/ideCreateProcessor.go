package handler

import (
	"context"
	"fmt"

	"github.com/aifimmunology/hise-data-contracts/clients/tracerclient"
	"github.com/aifimmunology/hise-data-contracts/contracts"
	"github.com/aifimmunology/hise-data-contracts/contracts/pubsubmessages"
	"github.com/aifimmunology/hise-data-contracts/contracts/tracer"
	"github.com/aifimmunology/ide-workflow-management/dependencycontainer"
	"github.com/aifimmunology/ide-workflow-management/env"
	"github.com/aifimmunology/shared-utility-library/argo"
	"github.com/aifimmunology/shared-utility-library/log"
	supubsub "github.com/aifimmunology/shared-utility-library/pubsub"
	"github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"

	"cloud.google.com/go/pubsub/v2"
)

type IDECreateProcessor struct{ baseProcessor }

const CreateWorkflowKey string = "create-workflow"

func (p *IDECreateProcessor) Claim(m *pubsub.Message) bool {
	return claimFlowAction(m, tracerclient.ActionCreate)
}

func (p *IDECreateProcessor) Name() string {
	return "IDECreateProcessor"
}

func (p *IDECreateProcessor) Process(ctx context.Context, m *pubsub.Message) error {
	if message, err := decodeFlowRequest(m); err != nil {
		m.Ack()
		return err
	} else if err := updateActionHistoryWithEtag(message); err != nil {
		return handleETagError(err, m, nil)
	} else if wf, ide, err := handleArgoFromRequest(message, tracerclient.StatusCreationFailure); err != nil {
		return handleETagError(err, m, wf)
	} else if healthRouteWf, err := submitHealthRouteIDEArgoRequest(ide); err != nil {
		ackMsgAndCleanUpWorkflow(m, wf)
		return err
	} else if err := supubsub.PublishMessage(
		ctx,
		contracts.PubsubIdeFlowRequestTopic,
		&pubsubmessages.IDEFlowRequest{
			IDEInstanceId: ide.ID,
			IDEName:       ide.PodName,
			Action:        tracerclient.StatusProvisioningHttpRoute,
			Workflow:      healthRouteWf,
		},
	); err != nil {
		ackMsgAndCleanUpWorkflow(m, wf)
		cleanupWorkflow(healthRouteWf)
		return err
	} else {
		ackMsgAndCleanUpWorkflow(m, wf)
		return nil
	}
}

func includeDashAppValue(ide *tracer.IDEInstance) string {
	if ide.DashApp.True() || ide.DashApp.False() {
		return ide.DashApp.String()
	}
	return "true"
}

func submitHealthRouteIDEArgoRequest(ide *tracer.IDEInstance) (*v1alpha1.Workflow, error) {
	wfBuilder := InitWorkFlowBuilder("ide-health-route-workflow.yaml")
	wfBuilder.params.Add("podName", fmt.Sprintf("%s-0", ide.PodName))
	wfBuilder.params.Add("ideName", ide.PodName)
	wfBuilder.params.Add("ideId", ide.ID.String())
	wfBuilder.params.Add("ideBaseURI", env.IdeProcEndPoint())
	wfBuilder.params.Add("authSecretName", "backend-service-oauth-client-secret")
	wfBuilder.params.Add("authClientID", env.AuthClientId())
	wfBuilder.params.Add("includeDashApp", includeDashAppValue(ide))
	wfBuilder.params.Add("project", env.Project())
	wfBuilder.params.Add(argo.CleanupFlag, "false")
	wfBuilder.params.Add("podTimeout", env.PodTimeout())

	submittedFlow, err := submitArgoWorkflow(wfBuilder, env.IdeServiceAccount())
	if err != nil {
		setIdeStatus(ide, "provisioning-http-route-failure", err.Error())
		if uerr := dependencycontainer.TracerDataServiceClient.UpdateIdeInstance(ide); uerr != nil {
			log.Main.Error(
				fmt.Sprintf("error updating ide instance status for ide %s: %s", ide.ID.String(), uerr.Error()),
			)
		}
		return nil, err
	}
	return submittedFlow, nil
}
