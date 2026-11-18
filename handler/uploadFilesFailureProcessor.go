package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aifimmunology/hise-data-contracts/clients/emailclient"
	emailmodel "github.com/aifimmunology/hise-data-contracts/contracts/email"
	"github.com/aifimmunology/hise-data-contracts/contracts/tracer"
	model "github.com/aifimmunology/hise-data-contracts/contracts/workflow"
	"github.com/aifimmunology/ide-workflow-management/dependencycontainer"
	"github.com/aifimmunology/ide-workflow-management/env"
	"github.com/aifimmunology/shared-utility-library/log"

	"cloud.google.com/go/pubsub/v2"
	"github.com/google/uuid"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

const (
	ideCommitWfName   string = "IDE Commit"
	uploadFilesWfName string = "Upload Files Workflow"
)

type UploadFilesFailureProcessor struct{ baseProcessor }

func (p *UploadFilesFailureProcessor) Claim(m *pubsub.Message) bool {
	var message model.WorkflowCompleteMsg
	if err := json.Unmarshal(m.Data, &message); err != nil {
		log.Main.Error(fmt.Sprintf("error unmarshalling message data: %s", err.Error()))
		return false
	}
	return message.Action == model.WorkflowCompleteMsgAction
}

func (p *UploadFilesFailureProcessor) Name() string {
	return "UploadFilesFailureProcessor"
}

func (p *UploadFilesFailureProcessor) Process(ctx context.Context, m *pubsub.Message) error {
	defer m.Ack()
	message, err := decodeMessage[model.WorkflowCompleteMsg](m)
	if err != nil {
		return err
	}

	workflow, err := dependencycontainer.WorkflowDataServiceClient.GetWorkflow(message.WorkflowId)
	if err != nil {
		log.Main.Error(fmt.Sprintf("error getting workflow %s: %s ", message.WorkflowId, err.Error()))
		return err
	}

	var templateName string
	if isFailedUploadFilesWf(workflow) {
		templateName = emailmodel.NotificationUploadFilesFailure
	} else if isFailedIDECommitWf(workflow) {
		templateName = emailmodel.NotificationIDECommitFailure
	} else {
		log.Main.Info(
			"Upload file failure processor ignoring unrecognized workflow failure",
			log.D{"name": workflow.Name, "status": workflow.Status},
		)
		return nil
	}

	taskLogs, err := getTaskLogs(workflow)
	if err != nil {
		log.Main.Error(fmt.Sprintf("error getting task logs for workflow %s: %s ", workflow.Id.String(), err.Error()))
		return err
	}

	formattedLogs := formatTaskLogs(taskLogs)
	if serr := sendUploadFilesErrorEmail(workflow, templateName, formattedLogs); serr != nil {
		log.Main.Error(fmt.Sprintf("error sending email: %s ", serr.Error()))
		return serr
	}
	return nil
}

func formatTaskLogs(taskLogs []*model.TaskLogs) string {
	var logsStr strings.Builder
	for _, taskLog := range taskLogs {
		for _, logs := range taskLog.Logs {
			logsStr.WriteString(logs)
		}
	}
	return truncateLog(logsStr.String())
}

func getIdeFromWorkflow(workflow *model.Workflow) (*tracer.IDEInstance, error) {
	if ideId, saved := workflow.Tags["ideInstanceId"]; !saved {
		return nil, fmt.Errorf("IDE instance id not found in workflow %s tags", workflow.Id.String())
	} else if ideGuid, err := uuid.Parse(ideId); err != nil {
		return nil, fmt.Errorf("error parsing ide instance id %s: %s", ideId, err.Error())
	} else if ide, err := dependencycontainer.TracerDataServiceClient.GetIdeInstance(ideGuid); err != nil {
		return nil, fmt.Errorf("error getting ide %s: %s ", ideId, err.Error())
	} else {
		return ide, nil
	}
}

func getTaskLogs(workflow *model.Workflow) ([]*model.TaskLogs, error) {
	taskLogs := []*model.TaskLogs{}
	for _, id := range workflow.TaskIds {
		taskLog, err := dependencycontainer.WorkflowDataServiceClient.GetWorkflowTaskLogs(id)
		if err != nil {
			log.Main.Error(
				fmt.Sprintf("error getting workflow %s task %s: %s ", workflow.GetId(), id.String(), err.Error()),
			)
			continue
		}
		taskLogs = append(taskLogs, taskLog)
	}
	return taskLogs, nil
}

func isFailedIDECommitWf(workflow *model.Workflow) bool {
	statusLowercase := strings.ToLower(workflow.Status.String())
	return workflow.Name == ideCommitWfName && strings.Contains(statusLowercase, "fail")
}

func isFailedUploadFilesWf(workflow *model.Workflow) bool {
	statusLowercase := strings.ToLower(workflow.Status.String())
	return workflow.Name == uploadFilesWfName && strings.Contains(statusLowercase, "fail")
}

func parseUName(email string) string {
	if strings.Contains(email, ".") {
		return cases.Title(language.English).String(strings.SplitN(email, ".", 2)[0])
	}
	return cases.Title(language.English).String(strings.SplitN(email, "@", 2)[0])
}

func sendUploadFilesErrorEmail(workflow *model.Workflow, template, logs string) error {
	ide, err := getIdeFromWorkflow(workflow)
	if err != nil {
		return err
	}

	accountVal := ide.AccountGUID.String()
	if acctName, err := getAccountName(ide.AccountGUID); err != nil {
		log.Main.Error(err.Error())
	} else {
		accountVal = acctName
	}

	if _, err := emailclient.SendEmailCustomRequest(
		&emailmodel.EmailRequest{
			ToAddress:        ide.AuditInfo.AddedUser,
			CCAddress:        env.SupportEmail(),
			NotificationType: template,
			DynamicTemplateData: map[string]any{
				"wf_id":    workflow.Id.String(),
				"logs":     logs,
				"env":      env.Project(),
				"acct_id":  accountVal,
				"ide_name": ide.PodName,
				"username": parseUName(ide.AuditInfo.AddedUser),
			},
		},
	); err != nil {
		log.Main.Error(fmt.Sprintf("Failed to send email notification: %s", err.Error()))
		return err
	}
	return nil
}

func truncateLog(log string) string {
	maxLength := 4000
	if len(log) > maxLength {
		return log[:maxLength]
	}
	return log
}
