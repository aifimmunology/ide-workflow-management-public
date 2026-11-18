package handler

import (
	"context"
	"encoding/json"

	"github.com/aifimmunology/hise-data-contracts/clients/tracerclient"
	"github.com/aifimmunology/hise-data-contracts/contracts/pubsubmessages"
	"github.com/aifimmunology/hise-data-contracts/contracts/tracer"
	"github.com/aifimmunology/ide-workflow-management/dependencycontainer"
	"github.com/aifimmunology/ide-workflow-management/env"
	"github.com/aifimmunology/shared-utility-library/argo"
	"github.com/aifimmunology/shared-utility-library/log"

	"cloud.google.com/go/pubsub/v2"
)

type HarvestArtifactsProcessor struct{ baseProcessor }

func (*HarvestArtifactsProcessor) Claim(m *pubsub.Message) bool {
	var tm pubsubmessages.TrainingImageArtifactsInitiatedMessage
	if err := json.Unmarshal(m.Data, &tm); err != nil {
		log.Main.Error(err)
		return false
	}
	return tm.Action == tracerclient.ActionHarvestArtifacts
}

func (*HarvestArtifactsProcessor) Name() string {
	return "HarvestArtifactsProcessor"
}

func (*HarvestArtifactsProcessor) Process(ctx context.Context, m *pubsub.Message) error {
	var message pubsubmessages.TrainingImageArtifactsInitiatedMessage
	if err := json.Unmarshal(m.Data, &message); err != nil {
		return err
	}

	if _, err := argo.FindAndMonitorArgoWorkflowByName(message.WorkflowName); err != nil {
		return err
	}

	ti, err := dependencycontainer.TracerDataServiceClient.GetTrainingImage(message.TrainingImageId)
	if err != nil {
		return err
	}

	ti.UpdateStatus(tracer.TrainingImageArtifactsReady, "", "ide-workflow-management")
	if err := dependencycontainer.TracerDataServiceClient.UpdateTrainingImage(ti); err != nil {
		return err
	}

	wfb := InitWorkFlowBuilder("delete-cloned-pvc.yaml")
	wfb.params.Add("pvc_guid", message.PvcGuid)
	if wkfl, err := submitArgoWorkflow(wfb, env.IdeServiceAccount()); err != nil {
		return err
	} else {
		_, err = argo.FindAndMonitorArgoWorkflow(wkfl)
		return err
	}
}
