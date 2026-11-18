package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/aifimmunology/hise-data-contracts/clients/emailclient"
	emailmodel "github.com/aifimmunology/hise-data-contracts/contracts/email"
	"github.com/aifimmunology/hise-data-contracts/contracts/workflow"
	model "github.com/aifimmunology/hise-data-contracts/contracts/workflow"
	"github.com/aifimmunology/ide-workflow-management/dependencycontainer"
	"github.com/aifimmunology/shared-utility-library/log"

	"cloud.google.com/go/pubsub/v2"
	"github.com/google/uuid"
)

const condaPackBuildWWfName = "Conda Pack Build Workflow"

var finishedStatuses = []workflow.Status{
	workflow.StatusHardFailed,
	workflow.StatusSucceeded,
	workflow.StatusSoftFailed,
}

type CondaPackCreateFailureProcessor struct{ baseProcessor }

func (p *CondaPackCreateFailureProcessor) Claim(m *pubsub.Message) bool {
	var message CondaPackWorkflow
	if err := json.Unmarshal(m.Data, &message); err != nil {
		log.Main.Error(fmt.Sprintf("error unmarshalling message data: %s", err.Error()))
		return false
	}
	return message.Status == "Ready"
}

func (p *CondaPackCreateFailureProcessor) Name() string {
	return "CondaPackCreateFailureProcessor"
}

func (p *CondaPackCreateFailureProcessor) Process(ctx context.Context, m *pubsub.Message) error {
	defer m.Ack()
	if message, err := decodeMessage[CondaPackWorkflow](m); err != nil {
		return err
	} else if workflow, err := dependencycontainer.WorkflowDataServiceClient.GetWorkflow(message.Id); err != nil {
		log.Main.Error(fmt.Sprintf("Could not get workflow with ID %s: %s", message.Id, err.Error()))
		return err
	} else {
		log.Main.Info(fmt.Sprintf("Processing Conda Pack Create Failure for workflow %s", workflow.Name))

		we := waitForWorkflow(*workflow, 120*time.Minute, 3*time.Minute)
		if we != nil {
			log.Main.Error(fmt.Sprintf("error waiting for workflow %s to complete: %s", workflow.Name, we.Error()))
			return we
		}

		// pull workflow again to get updated status
		workflow, err := dependencycontainer.WorkflowDataServiceClient.GetWorkflow(message.Id)
		if err != nil {
			log.Main.Error(fmt.Sprintf("Could not get workflow with ID %s: %s", message.Id, err.Error()))
			return err
		} else if !isFailedBuildCondaPackWf(workflow) {
			log.Main.Info(fmt.Sprintf("Workflow %s is not a failed Conda Pack build workflow.", workflow.Name))
			return nil
		}

		if serr := sendCondaPackBuildErrorEmail(workflow, message.CondaEnvName, message.PackageManager); serr != nil {
			log.Main.Error(
				fmt.Sprintf(
					"error sending email for failed Conda Pack build workflow %s: %s",
					workflow.Name,
					serr.Error(),
				),
			)
			return serr
		}

		// update status of condaPack as failed
		condaPack, cerr := dependencycontainer.TracerDataServiceClient.GetCondaPack(message.CondaId)
		if cerr != nil {
			log.Main.Error(fmt.Sprintf("error getting Conda Pack with ID %s: %s", message.CondaId, cerr.Error()))
			return cerr
		}

		condaPack.Status = model.StatusHardFailed.String()
		if cerr := dependencycontainer.TracerDataServiceClient.UpdateCondaPack(condaPack); cerr != nil {
			log.Main.Error(fmt.Sprintf("error updating Conda Pack with ID %s: %s", message.CondaId, cerr.Error()))
			return cerr
		}
	}
	return nil
}

type CondaPackWorkflow struct {
	Id             uuid.UUID       `json:"id" bson:"id"`
	Name           string          `json:"name" bson:"name"`
	Status         workflow.Status `json:"status" bson:"status"`
	CondaEnvName   string          `json:"condaEnvName" bson:"condaEnvName"`
	CondaId        uuid.UUID       `json:"condaId" bson:"condaId"`
	PackageManager string          `json:"packageManager" bson:"packageManager"`
}

func isFailedBuildCondaPackWf(workflow *model.Workflow) bool {
	statusLowercase := strings.ToLower(workflow.Status.String())
	log.Main.Debug("workflow status is:", log.D{"workflowName": workflow.Name, "status": workflow.Status.String()})
	return workflow.Name == condaPackBuildWWfName && strings.Contains(statusLowercase, "fail")
}

func sendCondaPackBuildErrorEmail(workflow *model.Workflow, condaEnvName, packageManager string) error {
	dynamicData := map[string]any{
		"conda_env_name":  condaEnvName,
		"workflow_step":   "cloud build",
		"package_manager": packageManager,
	}

	log.Main.Info(
		"Sending email for failed Conda Pack build",
		log.D{"user": workflow.Tags["user"], "data": dynamicData},
	)

	if _, err := emailclient.SendEmail(
		workflow.Tags["user"],
		emailmodel.NotificationCondaPackCreationFailure,
		nil,
		nil,
		dynamicData,
	); err != nil {
		log.Main.Error("error sending email for failed Conda Pack build", log.D{"error": err})
		return err
	}
	return nil
}

// waitForWorkflow waits until the workflow is in a terminal state or times out.
func waitForWorkflow(wf workflow.Workflow, timeout time.Duration, pollInterval time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		wf, err := dependencycontainer.WorkflowDataServiceClient.GetWorkflow(wf.Id)
		if err != nil {
			log.Main.Error("error getting workflow", log.D{"error": err, "workflow": wf.Id})
			return err
		}

		status := wf.Status
		log.Main.Info("Workflow still running", log.D{"status": status, "workflow": wf.Id})
		if slices.Contains(finishedStatuses, status) {
			log.Main.Info("Workflow completed", log.D{"status": status})
			return nil
		} else if time.Now().After(deadline) {
			return errors.New("timeout waiting for workflow to complete")
		}

		time.Sleep(pollInterval)
	}
}
